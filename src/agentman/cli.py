"""Typer commands. The client resolves config and owns the Python types.

``send`` and ``pipe`` are two verbs over one function. The only difference is
where the message text comes from. Both accept ``--agent``, ``--model`` and
``--json``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

import typer
from typer.core import TyperCommand, TyperGroup, _click

from agentman import __version__
from agentman.client import Client, DaemonError, DaemonNotRunning, TurnStreamError
from agentman.config import Config, ConfigError, load_config, provenance
from agentman.discovery import Directories, global_directory, resolve_directories, resolve_workspace
from agentman.model import ModelResolutionError, resolve_parallelism
from agentman.protocol import PROTOCOL_VERSION, TurnRequest, TurnResponse
from agentman.render import render_thread
from agentman.schema import OutputValidationError, SchemaError, schema_out, validate_output
from structured_agents import SpecError, discover_specs, load_spec

EXIT_OK = 0
EXIT_FINDING = 1
EXIT_CONFIG = 2
EXIT_USAGE = 3


class _UsageExitCommand(TyperCommand):
    """Make Click/Typer argument failures use agentman's exit contract."""

    def parse_args(self, ctx: _click.Context, args: list[str]) -> list[str]:
        try:
            return super().parse_args(ctx, args)
        except _click.exceptions.UsageError as error:
            error.exit_code = EXIT_USAGE
            raise


class _UsageExitGroup(TyperGroup):
    """Apply the usage exit code to both subcommand and group parsing."""

    def parse_args(self, ctx: _click.Context, args: list[str]) -> list[str]:
        try:
            return super().parse_args(ctx, args)
        except _click.exceptions.UsageError as error:
            error.exit_code = EXIT_USAGE
            raise

    def get_command(self, ctx: _click.Context, cmd_name: str) -> _click.Command | None:
        command = super().get_command(ctx, cmd_name)
        if command is not None and not isinstance(command, _UsageExitCommand):
            command.__class__ = _UsageExitCommand
        return command


app = typer.Typer(cls=_UsageExitGroup, add_completion=False, help="Talk to typed agents; every call is recorded.")


@app.command()
def version() -> None:
    """Print the package and socket-protocol versions."""
    typer.echo(f"agentman {__version__} (protocol {PROTOCOL_VERSION})")


@dataclass
class Context:
    """Resolved, repo-facing state for one CLI invocation."""

    config: Config
    directories: Directories
    workspace: str


@dataclass(frozen=True)
class _TurnSetup:
    """Repo-facing request data shared by one turn or a fan-out."""

    context: Context
    agent_name: str
    model: str
    spec: Any
    output_ref: Any
    output_schema: dict[str, Any] | None
    output_type: str


@dataclass(frozen=True)
class _PipeArguments:
    preamble: str | None
    jobs: int | None
    jobs_seen: bool


class PipeUsageError(ValueError):
    """The optional fan-out arguments are malformed."""


def _context(*, agent: str | None = None, model: str | None = None) -> Context:
    workspace = resolve_workspace()
    directories = resolve_directories(workspace)
    config = load_config(
        cli={"agent": agent, "model": model},
        repo_dir=directories.repo,
        global_dir=directories.global_,
    )
    return Context(config=config, directories=directories, workspace=str(workspace))


def _client(context: Context) -> Client:
    socket_path = context.config.socket.value
    if not socket_path:
        raise typer.BadParameter("no socket configured; run `agentman config` to see why")
    return Client(socket_path)


def _fail(message: str, code: int = EXIT_CONFIG) -> NoReturn:
    typer.echo(f"error: {message}", err=True)
    raise typer.Exit(code)


# -- the daemon ----------------------------------------------------------------


@app.command()
def serve(
    systemd_socket: bool = typer.Option(False, "--systemd-socket", help="Use the listening fd passed by systemd."),
) -> None:
    """Run the daemon in the foreground (development only)."""
    from agentman.daemon import serve as run_server

    try:
        context = _context()
        run_server(context.config, systemd_socket=systemd_socket)
    except ConfigError as error:
        _fail(str(error))
    except Exception as error:  # uvicorn and DBOS raise several types
        _fail(str(error))


@app.command()
def migrate() -> None:
    """Apply the packaged database migrations before daemon startup."""
    from agentman.daemon import migrate_database

    try:
        global_dir = global_directory()
        config = load_config(global_dir=global_dir if global_dir.is_dir() else None)
        migrate_database(config)
    except ConfigError as error:
        _fail(str(error))
    except Exception as error:
        _fail(str(error))


# -- threads -------------------------------------------------------------------


@app.command()
def new(name: str | None = typer.Argument(None)) -> None:
    """Start a thread, make it current, and switch to it."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            thread = await client.new_thread(workspace=context.workspace, name=name)
        _write_current(context, thread.id)
        typer.echo(f"{thread.short_id}  {thread.name or '(unnamed)'}")

    _call(run)


@app.command()
def use(name: str) -> None:
    """Switch to the most recently updated thread with this name."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            thread = await client.resolve_thread(workspace=context.workspace, name=name)
        _write_current(context, thread.id)
        typer.echo(f"{thread.short_id}  {thread.name}")

    _call(run)


@app.command("ls")
def list_threads() -> None:
    """List the workspace's threads."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            threads = await client.list_threads(workspace=context.workspace)
        colliding = _colliding_names(threads)
        for thread in threads:
            marker = ""
            if thread.name in colliding:
                marker = f"  #{thread.short_id}"
            typer.echo(f"{thread.short_id}  {thread.name or '(unnamed)'}{marker}  ({thread.runs} runs)")

    _call(run)


@app.command()
def show(thread: str | None = typer.Argument(None)) -> None:
    """Render a thread as Markdown to stdout."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            thread_id = await _resolve_thread_id(client, context, thread)
            view = await client.get_thread(thread_id)
        typer.echo(render_thread(view.messages), nl=False)

    _call(run)


@app.command()
def status() -> None:
    """Thread and run state, plus unresolved tool effects."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            thread_id = _read_current(context)
            view = await client.status(workspace=context.workspace, thread_id=thread_id)
        _print_status_view(view)

    _call(run)


@app.command()
def resume(
    run_id: str = typer.Argument(...),
    yes: bool = typer.Option(False, "--yes", help="resume without confirmation"),
) -> None:
    """Inspect a run, then resume its DBOS workflow."""

    async def run() -> None:
        context = _context()
        async with _client(context) as client:
            try:
                target = await client.get_run(run_id)
            except DaemonError as error:
                if error.status_code == 404:
                    raise DaemonError(f"cannot resume {run_id}: not an agentman run", status_code=409) from error
                raise
            if target.workspace != context.workspace:
                _fail(f"cannot resume {run_id}: belongs to a different workspace")
            view = await client.status(workspace=context.workspace, thread_id=target.thread_id)
            _print_status_view(view)
            if not yes and not typer.confirm(f"Resume run {run_id}?"):
                return
            await client.resume(workspace=context.workspace, run_id=run_id)
        typer.echo(f"resumed {run_id}")

    _call(run)


@app.command()
def config() -> None:
    """Print the resolved config and where each value came from."""
    try:
        context = _context()
    except ConfigError as error:
        _fail(str(error))
    for key, value, source in provenance(context.config):
        if key == "router_url" and value is None:
            # The daemon resolves the router URL from inferference's
            # `placement.yaml`. Show the resolved value so an operator does not
            # read `(unset)` while the daemon uses a real URL.
            typer.echo(f"{key} = {_resolved_router_url()}    # {source} (resolved from inferference)")
            continue
        shown = value if value is not None else "(unset)"
        typer.echo(f"{key} = {shown}    # {source}")


def _resolved_router_url() -> str:
    """Return the router URL inferference resolves, or the reason it cannot."""
    from inferference import ConfigError, resolve_router_url

    try:
        return resolve_router_url()
    except ConfigError as error:
        return f"(unresolved: {error})"


# -- turns ---------------------------------------------------------------------


@app.command()
def send(
    message: str = typer.Argument(...),
    agent: str | None = typer.Option(None, "--agent"),
    model: str | None = typer.Option(None, "--model"),
    max_transport_retries: int | None = typer.Option(
        None,
        "--max-transport-retries",
        min=0,
        help="Set the local provider's transport retry limit for this run.",
    ),
    json_output: bool = typer.Option(False, "--json", help="print the validated output and suppress the stream"),
) -> None:
    """One turn."""
    _call(
        lambda: _one_turn(
            message,
            agent=agent,
            model=model,
            json_output=json_output,
            max_transport_retries=max_transport_retries,
        )
    )


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def pipe(
    ctx: typer.Context,
    preamble: str | None = typer.Argument(None),
    agent: str | None = typer.Option(None, "--agent"),
    model: str | None = typer.Option(None, "--model"),
    max_transport_retries: int | None = typer.Option(
        None,
        "--max-transport-retries",
        min=0,
        help="Set the local provider's transport retry limit for each run.",
    ),
    json_output: bool = typer.Option(False, "--json", help="print the validated output and suppress the stream"),
    each: bool = typer.Option(False, "--each", help="run one independent turn per stdin line"),
) -> None:
    """Read stdin as one turn, or fan out one turn per line with ``--each``."""
    # Typer/Click 8.5 does not expose an optional-value option through
    # Typer's public Option wrapper. Keep the actual command surface and parse
    # only the two deliberately optional-value spellings here.
    raw_args = ([preamble] if preamble is not None else []) + list(ctx.args)
    try:
        parsed = _parse_pipe_arguments(raw_args)
    except PipeUsageError as error:
        _fail(str(error), EXIT_USAGE)
    if parsed.jobs_seen and not each:
        _fail("-j/--jobs requires --each", EXIT_USAGE)
    text = sys.stdin.read()
    if not each:
        message = f"{parsed.preamble}\n\n{text}" if parsed.preamble else text
        _call(
            lambda: _one_turn(
                message,
                agent=agent,
                model=model,
                json_output=json_output,
                max_transport_retries=max_transport_retries,
            )
        )
        return
    records = _split_pipe_records(text)
    _call(
        lambda: _fan_out(
            records,
            preamble=parsed.preamble,
            jobs=parsed.jobs,
            agent=agent,
            model=model,
            json_output=json_output,
            max_transport_retries=max_transport_retries,
        )
    )


async def _one_turn(
    message: str,
    *,
    agent: str | None,
    model: str | None,
    json_output: bool,
    max_transport_retries: int | None = None,
) -> None:
    context = _context(agent=agent, model=model)
    setup = _prepare_turn(context)

    async with _client(context) as client:
        thread_id = await _ensure_current_thread(client, setup.context)
        request = _turn_request(
            setup,
            thread_id=thread_id,
            prompt=message,
            max_transport_retries=max_transport_retries,
        )
        final: TurnResponse | None = None
        async for envelope in client.stream_turn(request):
            if envelope.kind == "turn_error":
                error_message = envelope.event.get("error")
                _fail(
                    error_message if isinstance(error_message, str) else "the daemon reported a failed turn",
                    EXIT_FINDING,
                )
            if envelope.final:
                if envelope.kind != "turn_result":
                    _fail(f"the daemon sent unknown final frame {envelope.kind!r}", EXIT_FINDING)
                final = TurnResponse.model_validate(envelope.event)
            elif not json_output:
                _print_delta(envelope)

    if final is None:
        _fail("the daemon ended the stream without a result", EXIT_FINDING)
    try:
        output = _validated_output(final, setup)
    except OutputValidationError as error:
        _fail(f"output failed validation: {error}", EXIT_FINDING)
    except SchemaError as error:
        _fail(str(error))
    if json_output:
        typer.echo(json.dumps(output))
    else:
        typer.echo()


async def _fan_out(
    records: list[str],
    *,
    preamble: str | None,
    jobs: int | None,
    agent: str | None,
    model: str | None,
    json_output: bool,
    max_transport_retries: int | None = None,
) -> None:
    """Submit one ordinary turn request per physical stdin line."""
    if not records:
        return
    context = _context(agent=agent, model=model)
    setup = _prepare_turn(context)
    concurrency = await resolve_parallelism(
        setup.model,
        jobs=jobs,
        router_url=context.config.router_url.value,
    )
    prompts = [_compose_pipe_message(preamble, record) for record in records]

    async with _client(context) as client:
        thread_id = await _ensure_current_thread(client, context)
        requests = [
            _turn_request(
                setup,
                thread_id=thread_id,
                prompt=prompt,
                max_transport_retries=max_transport_retries,
            )
            for prompt in prompts
        ]
        outcomes = await client.stream_turns(requests, concurrency=concurrency)

    outputs: dict[int, Any] = {}
    failures: list[tuple[int, int, str]] = []
    for outcome in outcomes:
        if outcome.error is not None:
            failures.append((outcome.index, _fanout_error_code(outcome.error), str(outcome.error)))
            continue
        if outcome.response is None:
            failures.append((outcome.index, EXIT_FINDING, "the daemon ended the stream without a result"))
            continue
        try:
            outputs[outcome.index] = _validated_output(outcome.response, setup)
        except OutputValidationError as error:
            failures.append((outcome.index, EXIT_FINDING, f"output failed validation: {error}"))
        except SchemaError as error:
            failures.append((outcome.index, EXIT_CONFIG, str(error)))

    for index in range(len(records)):
        if index not in outputs:
            continue
        output = outputs[index]
        if json_output:
            typer.echo(json.dumps(output))
        else:
            typer.echo(f"[{index + 1}]")
            typer.echo(output if isinstance(output, str) else json.dumps(output))
    for index, _, message in sorted(failures):
        typer.echo(f"input {index + 1}: {message}", err=True)
    if failures:
        code = EXIT_CONFIG if any(code == EXIT_CONFIG for _, code, _ in failures) else EXIT_FINDING
        raise typer.Exit(code)


def _prepare_turn(context: Context) -> _TurnSetup:
    agent_name = context.config.agent.value
    if not agent_name:
        _fail("no agent configured; set `agent:` in .agentman/agentman.yaml")
    model_string = context.config.model.value
    if not model_string:
        _fail("no model configured; set `model:` in .agentman/agentman.yaml or pass --model")
    spec = _load_agent(context, agent_name)
    output_ref = spec.output
    output_schema = None
    output_type = "str"
    if output_ref is not None:
        try:
            output_schema = schema_out(output_ref, workspace=Path(context.workspace))
        except SchemaError as error:
            _fail(str(error))
        output_type = output_ref.target
    return _TurnSetup(
        context=context,
        agent_name=agent_name,
        model=model_string,
        spec=spec,
        output_ref=output_ref,
        output_schema=output_schema,
        output_type=output_type,
    )


async def _ensure_current_thread(client: Client, context: Context) -> str:
    thread_id = _read_current(context)
    if thread_id is None:
        thread = await client.new_thread(workspace=context.workspace, name=None)
        _write_current(context, thread.id)
        thread_id = thread.id
    return thread_id


def _turn_request(
    setup: _TurnSetup,
    *,
    thread_id: str,
    prompt: str,
    max_transport_retries: int | None = None,
) -> TurnRequest:
    return TurnRequest(
        workspace=setup.context.workspace,
        thread_id=thread_id,
        agent_spec=setup.spec.agent.model_dump(mode="json", by_alias=True, exclude_defaults=False),
        tools=setup.spec.tools.value if setup.spec.tools is not None else None,
        agent_name=setup.spec.name,
        model=setup.model,
        prompt=prompt,
        output_schema=setup.output_schema,
        output_type=setup.output_type,
        max_transport_retries=max_transport_retries,
    )


def _validated_output(final: TurnResponse, setup: _TurnSetup) -> Any:
    output = final.output
    if setup.output_ref is not None and output is not None:
        canonical = validate_output(setup.output_ref, json.dumps(output), workspace=Path(setup.context.workspace))
        return json.loads(canonical)
    return output


def _compose_pipe_message(preamble: str | None, record: str) -> str:
    return f"{preamble}\n\n{record}" if preamble else record


def _split_pipe_records(text: str) -> list[str]:
    """Split physical stdin lines without making a trailing newline a record."""
    return text.splitlines()


def _parse_pipe_arguments(args: list[str]) -> _PipeArguments:
    preamble: str | None = None
    jobs: int | None = None
    jobs_seen = False
    positionals: list[str] = []
    index = 0
    while index < len(args):
        token = args[index]
        if token in ("-j", "--jobs"):
            if jobs_seen:
                raise PipeUsageError("-j/--jobs may be supplied only once")
            jobs_seen = True
            if index + 1 < len(args) and not _looks_like_option(args[index + 1]):
                jobs = _parse_job_count(args[index + 1])
                index += 2
            else:
                index += 1
            continue
        if token.startswith("--jobs="):
            if jobs_seen:
                raise PipeUsageError("-j/--jobs may be supplied only once")
            jobs_seen = True
            jobs = _parse_job_count(token.split("=", 1)[1])
            index += 1
            continue
        if token.startswith("-j") and token != "-j":
            if jobs_seen:
                raise PipeUsageError("-j/--jobs may be supplied only once")
            jobs_seen = True
            jobs = _parse_job_count(token[2:])
            index += 1
            continue
        if token.startswith("-"):
            raise PipeUsageError(f"unknown pipe option {token!r}")
        positionals.append(token)
        index += 1
    if len(positionals) > 1:
        raise PipeUsageError("pipe accepts at most one preamble")
    preamble = positionals[0] if positionals else None
    return _PipeArguments(preamble=preamble, jobs=jobs, jobs_seen=jobs_seen)


def _looks_like_option(token: str) -> bool:
    return token.startswith("-") and not token.lstrip("-").isdigit()


def _parse_job_count(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise PipeUsageError("job count must be a positive integer") from error
    if value <= 0:
        raise PipeUsageError("job count must be a positive integer")
    return value


def _fanout_error_code(error: Exception) -> int:
    if isinstance(error, TurnStreamError):
        return EXIT_FINDING
    if isinstance(error, DaemonNotRunning | ModelResolutionError):
        return EXIT_CONFIG
    if isinstance(error, DaemonError):
        return EXIT_CONFIG if error.status_code is not None else EXIT_FINDING
    return EXIT_CONFIG


def _print_delta(envelope: Any) -> None:
    event = envelope.event
    kind = event.get("event_kind")
    if kind == "part_delta":
        delta = event.get("delta", {})
        if delta.get("part_delta_kind") == "text":
            typer.echo(delta.get("content_delta") or "", nl=False)
    elif kind == "part_start":
        part = event.get("part", {})
        if part.get("part_kind") == "text" and part.get("content"):
            typer.echo(part["content"], nl=False)


def _print_status_view(view: Any) -> None:
    if view.thread is None:
        typer.echo("no current thread")
        return
    typer.echo(f"thread {view.thread.short_id}  {view.thread.name or '(unnamed)'}")
    if view.latest_run is None:
        typer.echo("no runs")
        return
    run = view.latest_run
    typer.echo(f"run {run.id}  {run.status}  {run.agent}  {run.model}")
    metadata = run.run_metadata
    requested = (
        str(metadata.requested_max_transport_retries)
        if metadata.requested_max_transport_retries is not None
        else "provider default"
    )
    effective = (
        str(metadata.effective_max_transport_retries)
        if metadata.effective_max_transport_retries is not None
        else "unknown"
    )
    physical = (
        str(metadata.physical_model_request_count) if metadata.physical_model_request_count is not None else "unknown"
    )
    fallback = "enabled" if metadata.provider_fallback else "disabled"
    typer.echo(f"transport retries requested {requested}  effective {effective}  physical requests {physical}")
    typer.echo(f"provider fallback {fallback}")
    typer.echo(f"events {view.events}  snapshots {view.snapshots}")
    typer.echo(f"messages {view.message_rows} rows  {view.message_bytes} bytes")
    for effect in view.unresolved_effects:
        typer.echo(f"unresolved: {effect.run_id} {effect.tool_name} {effect.tool_call_id} ({effect.note})")


# -- helpers -------------------------------------------------------------------


def _call(coro_factory: Any) -> None:
    try:
        asyncio.run(coro_factory())
    except ConfigError as error:
        _fail(str(error))
    except DaemonNotRunning as error:
        _fail(str(error))
    except DaemonError as error:
        _fail(str(error), EXIT_CONFIG if error.status_code is not None else EXIT_FINDING)
    except ModelResolutionError as error:
        _fail(str(error))
    except SpecError as error:
        _fail(str(error))
    except SchemaError as error:
        _fail(str(error))


def _load_agent(context: Context, name: str):
    specs = discover_specs(context.directories.precedence())
    path = specs.get(name)
    if path is None:
        _fail(f"no agent named {name!r}; known agents: {', '.join(sorted(specs)) or '(none)'}")
    return load_spec(path)


async def _resolve_thread_id(client: Client, context: Context, name: str | None) -> str:
    if name is not None:
        thread = await client.resolve_thread(workspace=context.workspace, name=name)
        return thread.id
    current = _read_current(context)
    if current is None:
        _fail("no current thread; run `agentman new` first")
    return current


def _colliding_names(threads: list[Any]) -> set[str]:
    seen: dict[str, int] = {}
    for thread in threads:
        seen[thread.name] = seen.get(thread.name, 0) + 1
    return {name for name, count in seen.items() if count > 1}


def _state_dir() -> Path:
    base = os.environ.get("XDG_STATE_HOME")
    root = Path(base) if base else Path.home() / ".local" / "state"
    return root / "agentman"


def _current_path() -> Path:
    return _state_dir() / "current.json"


def _read_current(context: Context) -> str | None:
    path = _current_path()
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    value = data.get(context.workspace)
    return value if isinstance(value, str) else None


def _write_current(context: Context, thread_id: str) -> None:
    path = _current_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {}
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {}
    data[context.workspace] = thread_id
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
