"""The CLI surface that does not need a daemon."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import agentman.cli as cli
from agentman.cli import app
from agentman.client import DaemonError, TurnOutcome
from agentman.config import CONFIG_FILENAME, Config, ConfigError, Value
from agentman.discovery import Directories
from agentman.model import ModelResolutionError
from agentman.protocol import PROTOCOL_VERSION, TurnResponse
from agentman.schema import OutputValidationError


def test_config_prints_every_value_and_its_source(tmp_path: Path, monkeypatch) -> None:
    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    (config_home / "agentman" / CONFIG_FILENAME).write_text(
        "database_url: postgresql://db\nmodel: inferference:gemma\n"
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(app, ["config"])

    assert result.exit_code == 0, result.output
    assert "database_url = postgresql://db" in result.output
    assert "model = inferference:gemma" in result.output
    assert "agent = default" in result.output
    assert str(config_home / "agentman" / CONFIG_FILENAME) in result.output


def test_version_reports_package_and_protocol_versions() -> None:
    from agentman import __version__

    result = CliRunner().invoke(app, ["version"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == f"agentman {__version__} (protocol {PROTOCOL_VERSION})"


@pytest.mark.parametrize("unknown", [True, False])
def test_resume_preflight_uses_non_sensitive_refusal_text(
    monkeypatch: pytest.MonkeyPatch,
    unknown: bool,
) -> None:
    context, _ = _fanout_test_setup()

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def get_run(self, run_id: str):
            if unknown:
                raise DaemonError("raw HTTP 404 detail", status_code=404)
            return SimpleNamespace(workspace="/private/repository/path", thread_id="thread-1")

        async def status(self, **_: object):
            raise AssertionError("a refused preflight must not inspect or resume the run")

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())
    result = CliRunner().invoke(app, ["resume", "run-unknown", "--yes"])

    assert result.exit_code == cli.EXIT_CONFIG, result.output
    assert "cannot resume run-unknown" in result.output
    expected = "not an agentman run" if unknown else "different workspace"
    assert expected in result.output
    assert "/private/repository/path" not in result.output
    assert "raw HTTP" not in result.output


def test_config_prints_the_router_url_inferference_resolves(tmp_path: Path, monkeypatch) -> None:
    """F10: an unset router_url must not read as `(unset)`."""
    import inferference

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(inferference, "resolve_router_url", lambda: "http://127.0.0.1:9999/v1")

    result = CliRunner().invoke(app, ["config"])

    assert result.exit_code == 0, result.output
    assert "router_url = http://127.0.0.1:9999/v1" in result.output
    assert "resolved from inferference" in result.output


def test_config_reports_an_unresolved_router_url(tmp_path: Path, monkeypatch) -> None:
    import inferference

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(tmp_path)

    def explode() -> str:
        raise inferference.ConfigError("no placement at /nowhere/placement.yaml")

    monkeypatch.setattr(inferference, "resolve_router_url", explode)

    result = CliRunner().invoke(app, ["config"])

    assert result.exit_code == 0, result.output
    assert "router_url = (unresolved: no placement at /nowhere/placement.yaml)" in result.output


def test_config_explicit_router_url_does_not_consult_inferference(tmp_path: Path, monkeypatch) -> None:
    import inferference

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    (config_home / "agentman" / CONFIG_FILENAME).write_text("router_url: http://explicit.test/v1\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(tmp_path)

    def explode() -> str:
        raise AssertionError("explicit router_url must win")

    monkeypatch.setattr(inferference, "resolve_router_url", explode)

    result = CliRunner().invoke(app, ["config"])

    assert result.exit_code == 0, result.output
    assert "router_url = http://explicit.test/v1" in result.output


def test_migrate_uses_global_database_configuration(tmp_path: Path, monkeypatch) -> None:
    config_home = tmp_path / "config"
    global_dir = config_home / "agentman"
    global_dir.mkdir(parents=True)
    (global_dir / CONFIG_FILENAME).write_text("database_url: postgresql://db\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))

    seen: list[str | None] = []

    def migrate(config: Config) -> None:
        seen.append(config.database_url.value)

    monkeypatch.setattr("agentman.daemon.migrate_database", migrate)
    result = CliRunner().invoke(app, ["migrate"])

    assert result.exit_code == cli.EXIT_OK, result.output
    assert seen == ["postgresql://db"]


@pytest.mark.parametrize("args", [["send", "--not-a-real-option", "hello"], ["send"]])
def test_cli_usage_errors_use_exit_code_three(args: list[str]) -> None:
    result = CliRunner().invoke(app, args)

    assert result.exit_code == cli.EXIT_USAGE, result.output


def test_cli_success_uses_exit_code_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    async def succeed(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(cli, "_one_turn", succeed)

    result = CliRunner().invoke(app, ["send", "hello"])

    assert result.exit_code == cli.EXIT_OK, result.output


def test_cli_configuration_failure_uses_exit_code_two(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail(*args: object, **kwargs: object) -> None:
        raise ConfigError("invalid configuration")

    monkeypatch.setattr(cli, "_one_turn", fail)

    result = CliRunner().invoke(app, ["send", "hello"])

    assert result.exit_code == cli.EXIT_CONFIG, result.output
    assert "invalid configuration" in result.output


def test_cli_missing_daemon_is_configuration_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fail(*args: object, **kwargs: object) -> None:
        from agentman.client import DaemonNotRunning

        raise DaemonNotRunning("cannot connect; start `agentman serve`")

    monkeypatch.setattr(cli, "_one_turn", fail)

    result = CliRunner().invoke(app, ["send", "hello"])

    assert result.exit_code == cli.EXIT_CONFIG, result.output
    assert "start `agentman serve`" in result.output


def test_send_passes_transport_retry_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int | None] = []

    async def fake_one_turn(
        message: str,
        *,
        agent: object,
        model: object,
        json_output: object,
        max_transport_retries: int | None,
    ) -> None:
        calls.append(max_transport_retries)

    monkeypatch.setattr(cli, "_one_turn", fake_one_turn)
    result = CliRunner().invoke(app, ["send", "hello", "--max-transport-retries", "0"])

    assert result.exit_code == 0, result.output
    assert calls == [0]


def test_send_rejects_negative_transport_retry_limit() -> None:
    result = CliRunner().invoke(app, ["send", "hello", "--max-transport-retries", "-1"])

    assert result.exit_code == 3


def test_pipe_each_parses_explicit_and_no_value_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    async def fake_fan_out(records: list[str], **kwargs: object) -> None:
        calls.append({"records": records, **kwargs})

    monkeypatch.setattr(cli, "_fan_out", fake_fan_out)

    explicit = CliRunner().invoke(
        app,
        ["pipe", "--each", "-j", "2", "--max-transport-retries", "0"],
        input="one\n\n",
    )
    automatic = CliRunner().invoke(app, ["pipe", "--each", "-j"], input="one\n")

    assert explicit.exit_code == 0, explicit.output
    assert automatic.exit_code == 0, automatic.output
    assert calls[0]["jobs"] == 2
    assert calls[0]["max_transport_retries"] == 0
    assert calls[0]["records"] == ["one", ""]
    assert calls[1]["jobs"] is None
    assert calls[1]["records"] == ["one"]


@pytest.mark.parametrize("raw", ["0", "-1", "not-an-integer"])
def test_pipe_each_rejects_invalid_jobs(raw: str) -> None:
    result = CliRunner().invoke(app, ["pipe", "--each", "-j", raw], input="line\n")

    assert result.exit_code == 3
    assert "job count must be a positive integer" in result.output


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", []),
        ("one\n", ["one"]),
        ("\n", [""]),
        ("one\n\ntwo", ["one", "", "two"]),
    ],
)
def test_pipe_each_splits_physical_records(text: str, expected: list[str]) -> None:
    assert cli._split_pipe_records(text) == expected


def test_pipe_without_each_keeps_the_single_turn_path(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object, object, object, int | None]] = []

    async def fake_one_turn(
        message: str,
        *,
        agent: object,
        model: object,
        json_output: object,
        max_transport_retries: int | None,
    ) -> None:
        calls.append((message, agent, model, json_output, max_transport_retries))

    monkeypatch.setattr(cli, "_one_turn", fake_one_turn)
    result = CliRunner().invoke(
        app,
        ["pipe", "summarize", "--json", "--max-transport-retries", "0"],
        input="payload\n",
    )

    assert result.exit_code == 0, result.output
    assert calls == [("summarize\n\npayload\n", None, None, True, 0)]


def test_pipe_preamble_is_applied_to_each_record() -> None:
    assert cli._compose_pipe_message("review", "first") == "review\n\nfirst"
    assert cli._compose_pipe_message("review", "") == "review\n\n"


def _fanout_test_setup(*, typed: bool = False) -> tuple[cli.Context, cli._TurnSetup]:
    context = cli.Context(
        config=Config(
            agent=Value("worker", "test"),
            model=Value("inferference:alias", "test"),
            database_url=Value(None, "test"),
            socket=Value("/tmp/agentman.sock", "test"),
            router_url=Value(None, "test"),
        ),
        directories=Directories(workspace=Path("/repo"), repo=None, global_=None),
        workspace="/repo",
    )
    spec = SimpleNamespace(
        agent=SimpleNamespace(model_dump=lambda **_: {"instructions": "test"}),
        tools=None,
        name="worker",
    )
    setup = cli._TurnSetup(
        context=context,
        agent_name="worker",
        model="inferference:alias",
        spec=spec,
        output_ref=SimpleNamespace(target="models:Answer") if typed else None,
        output_schema={} if typed else None,
        output_type="models:Answer" if typed else "str",
    )
    return context, setup


def test_turn_request_carries_explicit_transport_retry_limit() -> None:
    _, setup = _fanout_test_setup()

    request = cli._turn_request(
        setup,
        thread_id="thread-1",
        prompt="hello",
        max_transport_retries=0,
    )

    assert request.max_transport_retries == 0


def test_pipe_each_no_value_jobs_uses_default_through_the_cli_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, setup = _fanout_test_setup()
    parallelism_calls: list[tuple[str, int | None, str | None]] = []
    ensure_calls: list[str] = []

    async def fake_parallelism(model: str, *, jobs: int | None, router_url: str | None) -> int:
        parallelism_calls.append((model, jobs, router_url))
        return 2

    async def fake_ensure(client: object, current: cli.Context) -> str:
        ensure_calls.append(current.workspace)
        return "thread-1"

    class FakeClient:
        def __init__(self) -> None:
            self.requests = []

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def stream_turns(self, requests, *, concurrency: int) -> list[TurnOutcome]:
            self.requests.extend(requests)
            return [
                TurnOutcome(
                    index=index,
                    response=TurnResponse(run_id=f"run-{index}", thread_id=request.thread_id, output=f"output-{index}"),
                )
                for index, request in enumerate(requests)
            ]

    client = FakeClient()
    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_prepare_turn", lambda _: setup)
    monkeypatch.setattr(cli, "_ensure_current_thread", fake_ensure)
    monkeypatch.setattr(cli, "_client", lambda _: client)
    monkeypatch.setattr(cli, "resolve_parallelism", fake_parallelism)

    result = CliRunner().invoke(app, ["pipe", "--each", "review", "-j"], input="alpha\n\nbeta\n")

    assert result.exit_code == 0, result.output
    assert parallelism_calls == [("inferference:alias", None, None)]
    assert ensure_calls == ["/repo"]
    assert [request.prompt for request in client.requests] == [
        "review\n\nalpha",
        "review\n\n",
        "review\n\nbeta",
    ]
    assert [request.thread_id for request in client.requests] == ["thread-1"] * 3
    assert result.output.splitlines() == ["[1]", "output-0", "[2]", "output-1", "[3]", "output-2"]


def test_pipe_each_model_resolution_failure_is_config_error_before_any_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context, setup = _fanout_test_setup()
    ensure_calls: list[str] = []
    client_calls: list[str] = []

    async def fail_parallelism(*args: object, **kwargs: object) -> int:
        raise ModelResolutionError("capability lookup failed")

    async def fail_ensure(client: object, current: cli.Context) -> str:
        ensure_calls.append(current.workspace)
        raise AssertionError("thread resolution must not start after default resolution fails")

    def fail_client(_: cli.Context) -> object:
        client_calls.append("client")
        raise AssertionError("the daemon client must not open after default resolution fails")

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_prepare_turn", lambda _: setup)
    monkeypatch.setattr(cli, "_ensure_current_thread", fail_ensure)
    monkeypatch.setattr(cli, "_client", fail_client)
    monkeypatch.setattr(cli, "resolve_parallelism", fail_parallelism)

    result = CliRunner().invoke(app, ["pipe", "--each", "-j"], input="alpha\nbeta\n")

    assert result.exit_code == cli.EXIT_CONFIG, result.output
    assert "capability lookup failed" in result.output
    assert ensure_calls == []
    assert client_calls == []
    assert not (tmp_path / "state" / "agentman" / "current.json").exists()


def test_pipe_each_keeps_successes_and_reports_indexed_validation_and_stream_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context, setup = _fanout_test_setup(typed=True)

    class FakeClient:
        def __init__(self) -> None:
            self.requests = []

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def stream_turns(self, requests, *, concurrency: int) -> list[TurnOutcome]:
            self.requests.extend(requests)
            return [
                TurnOutcome(
                    index=0,
                    response=TurnResponse(run_id="run-0", thread_id="thread-1", output={"value": "zero"}),
                ),
                TurnOutcome(index=1, response=TurnResponse(run_id="run-1", thread_id="thread-1", output={"bad": True})),
                TurnOutcome(index=2, error=DaemonError("daemon failed")),
                TurnOutcome(
                    index=3,
                    response=TurnResponse(run_id="run-3", thread_id="thread-1", output={"value": "three"}),
                ),
            ]

    def validate_one_line(reference: object, payload: str, *, workspace: Path) -> str:
        value = json.loads(payload)
        if "value" not in value:
            raise OutputValidationError("missing value")
        return payload

    client = FakeClient()
    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_prepare_turn", lambda _: setup)
    monkeypatch.setattr(cli, "_ensure_current_thread", lambda *_: _async_value("thread-1"))
    monkeypatch.setattr(cli, "_client", lambda _: client)
    monkeypatch.setattr(cli, "validate_output", validate_one_line)

    result = CliRunner().invoke(app, ["pipe", "--each", "-j", "4", "--json"], input="one\ntwo\nthree\nfour\n")

    assert result.exit_code == cli.EXIT_FINDING, result.output
    assert [json.loads(line) for line in result.output.splitlines() if line.startswith("{")] == [
        {"value": "zero"},
        {"value": "three"},
    ]
    assert "input 2: output failed validation: missing value" in result.output
    assert "input 3: daemon failed" in result.output
    assert len(client.requests) == 4


async def _async_value(value: str) -> str:
    return value


def test_status_prints_the_usage_and_end_reason_of_a_cancelled_run(capsys) -> None:
    from datetime import UTC, datetime

    from agentman.protocol import RunError, RunInfo, StatusView, ThreadInfo

    now = datetime.now(UTC)
    view = StatusView(
        thread=ThreadInfo(
            id="t" * 32, workspace="/repo", name="topic", created_at=now, updated_at=now, short_id="tttttttt"
        ),
        latest_run=RunInfo(
            id="run-1",
            thread_id="t" * 32,
            workspace="/repo",
            agent="topic",
            model="openai:slow",
            status="cancelled",
            prompt="p",
            usage={"requests": 1, "input_tokens": 120, "output_tokens": 30},
            error=RunError(reason="cancel_requested"),
            created_at=now,
            updated_at=now,
        ),
    )

    cli._print_status_view(view)

    output = capsys.readouterr().out
    assert "run run-1  cancelled  topic  openai:slow" in output
    assert "usage requests 1  input tokens 120  output tokens 30" in output
    assert "ended cancel_requested\n" in output


def test_cancel_accepts_a_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()
    calls: list[tuple[str, str]] = []

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return ["cancel"]

        async def cancel_run(self, run_id: str, *, workspace: str) -> SimpleNamespace:
            calls.append((run_id, workspace))
            return SimpleNamespace(run_id=run_id, status="cancelled", cancelled=True)

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "run-1"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "cancelled run-1"
    assert calls == [("run-1", "/repo")]


def test_cancel_resolves_a_thread_and_only_cancels_running_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()
    calls: list[str] = []

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return ["cancel"]

        async def resolve_thread(self, *, workspace: str, name: str) -> SimpleNamespace:
            assert (workspace, name) == ("/repo", "review")
            return SimpleNamespace(id="thread-1")

        async def get_thread(self, thread_id: str) -> SimpleNamespace:
            assert thread_id == "thread-1"
            return SimpleNamespace(
                runs=[
                    SimpleNamespace(id="run-running", status="running"),
                    SimpleNamespace(id="run-done", status="completed"),
                ]
            )

        async def cancel_run(self, run_id: str, *, workspace: str) -> SimpleNamespace:
            assert workspace == "/repo"
            calls.append(run_id)
            return SimpleNamespace(run_id=run_id, status="cancelled", cancelled=True)

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "--thread", "review"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "cancelled run-running"
    assert calls == ["run-running"]


def test_cancel_selects_running_runs_across_the_workspace(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()
    calls: list[str] = []

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return ["cancel"]

        async def list_threads(self, *, workspace: str) -> list[SimpleNamespace]:
            assert workspace == "/repo"
            return [SimpleNamespace(id="thread-1"), SimpleNamespace(id="thread-2")]

        async def get_thread(self, thread_id: str) -> SimpleNamespace:
            runs = {
                "thread-1": [SimpleNamespace(id="run-1", status="running")],
                "thread-2": [SimpleNamespace(id="run-2", status="failed")],
            }
            return SimpleNamespace(runs=runs[thread_id])

        async def cancel_run(self, run_id: str, *, workspace: str) -> SimpleNamespace:
            assert workspace == "/repo"
            calls.append(run_id)
            return SimpleNamespace(run_id=run_id, status="cancelled", cancelled=True)

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "--all-running"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "cancelled run-1"
    assert calls == ["run-1"]


def test_cancel_reports_when_no_run_is_running(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return ["cancel"]

        async def list_threads(self, *, workspace: str) -> list[SimpleNamespace]:
            return [SimpleNamespace(id="thread-1")]

        async def get_thread(self, thread_id: str) -> SimpleNamespace:
            return SimpleNamespace(runs=[SimpleNamespace(id="run-1", status="completed")])

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "--all-running"])

    assert result.exit_code == 0, result.output
    assert result.output.strip() == "no running run"


def test_cancel_reports_an_already_ended_named_run_as_a_finding(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return ["cancel"]

        async def cancel_run(self, run_id: str, *, workspace: str) -> SimpleNamespace:
            return SimpleNamespace(run_id=run_id, status="completed", cancelled=False)

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "run-1"])

    assert result.exit_code == cli.EXIT_FINDING, result.output
    assert result.output.strip() == "not cancelled run-1: already completed"


def test_cancel_refuses_an_old_daemon_without_the_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    context, _ = _fanout_test_setup()

    class FakeClient:
        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def capabilities(self) -> list[str]:
            return []

    monkeypatch.setattr(cli, "_context", lambda **_: context)
    monkeypatch.setattr(cli, "_client", lambda _: FakeClient())

    result = CliRunner().invoke(app, ["cancel", "run-1"])

    assert result.exit_code == cli.EXIT_CONFIG, result.output
    assert "daemon does not support cancel" in result.output


@pytest.mark.parametrize("arguments", [[], ["run-1", "--all-running"]])
def test_cancel_requires_exactly_one_selector(arguments: list[str]) -> None:
    result = CliRunner().invoke(app, ["cancel", *arguments])

    assert result.exit_code == cli.EXIT_USAGE, result.output
    assert "exactly one" in result.output
