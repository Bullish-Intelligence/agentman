# Getting started

This guide describes the current repository development path. It assumes a
Linux environment with `devenv` and a local Postgres cluster. The machine-wide
systemd installation is configured outside this repository; its acceptance
procedure is in the [development guide](development.md).

## Prerequisites

- a checkout of this repository;
- `devenv`;
- a Python version accepted by `pyproject.toml` (`>=3.13`);
- a running inferference router if using `inferference:<name>` models; and
- a writable Unix runtime directory for the daemon socket.

The project devenv defines a private Postgres service on `127.0.0.1:5433` with
an `agentman` database. Do not use the unrelated system Postgres cluster on
port 5432 for the default development instructions.

## Enter the environment

Run project commands through the devenv shell:

```sh
devenv shell
```

Start the project services in another terminal or as a background devenv
process:

```sh
devenv up
```

The shell provides the pinned Python environment and the repository’s manager
toolchain. Bare `python`, `pytest`, `uv`, or `ruff` commands are not the
repository workflow.

## Configure the daemon

The daemon needs a global database URL. Create
`$XDG_CONFIG_HOME/agentman/agentman.yaml`, or
`~/.config/agentman/agentman.yaml` when `XDG_CONFIG_HOME` is unset:

```yaml
database_url: postgresql+asyncpg://<role>@127.0.0.1:5433/agentman
socket: /run/user/<uid>/agentman.sock
# Optional when inferference cannot resolve its own placement:
# router_url: http://127.0.0.1:<port>
```

`database_url`, `socket`, and `router_url` are global-only keys. They must not
be placed in a repository’s tracked `.agentman/agentman.yaml`. For one-off
migrations or test environments, `AGENTMAN_DATABASE_URL` may provide the
database URL when no higher-precedence config value exists.

Check the resolved values and provenance:

```sh
agentman config
```

The socket defaults to `$XDG_RUNTIME_DIR/agentman.sock`, with
`/run/user/<uid>/agentman.sock` as the fallback when that directory exists.

## Apply migrations

Database tables are created by the explicit migration command, not by the
daemon while serving requests:

```sh
agentman migrate
```

The migration creates the `agentman` and `step_persistence` schemas. DBOS owns
its own tables in the same Postgres database. See
[Persistence and recovery](persistence-and-recovery.md) for the ownership
model.

## Start the development daemon

Run the daemon in the foreground:

```sh
agentman serve
```

The command binds the configured Unix socket. It does not daemonize itself.
When the socket is absent, client commands report that `agentman serve` must
be running. There is no spawn-if-absent client fallback in the current
development path.

The code also accepts a systemd-passed descriptor:

```sh
agentman serve --systemd-socket
```

Use this only when a socket unit has supplied exactly one listening descriptor;
it is not a replacement for the foreground development command.

## Define a repository agent

Create a `.agentman/` directory at the resolved repository root. A minimal
configuration is:

```yaml
# .agentman/agentman.yaml
agent: reviewer
model: inferference:qwen-coder
```

Create a matching `.agentman/reviewer.yaml`:

```yaml
name: reviewer
instructions: Review the supplied change carefully and report concrete findings.
output: models:Review
```

The `output:` value is an agentman extension. The module path is resolved
relative to the directory containing the spec, in the repository’s own Python
interpreter. For the example, `.agentman/models.py` could contain:

```python
from pydantic import BaseModel


class Review(BaseModel):
    summary: str
    findings: list[str]
```

The default output type is `str`; omitting `output:` does not create an
untyped execution path. To opt into the current workspace-scoped harness
tools, add:

```yaml
tools: coder
```

`coder` includes file tools and an unrestricted shell. Treat enabling it as a
privileged choice: commands execute with the daemon user’s permissions in the
workspace named by the request.

## Run a first turn

Create and select a thread:

```sh
agentman new review
```

Send a message with streaming output:

```sh
agentman send "Review the current working tree."
```

Use `--json` to suppress the live text stream and print the validated output:

```sh
agentman send --json "Return a structured review of the current working tree."
```

The client automatically creates an unnamed current thread if a turn is sent
before `agentman new`. Explicit thread creation is easier to inspect and share
in scripts.

Pipe one message from standard input:

```sh
git diff | agentman pipe "Review this change"
```

Set transport retries for one run:

```sh
agentman send --max-transport-retries 0 "Return one proposal."
```

The setting applies to `inferference:` models. Zero permits one initial
request and no transport retry. It does not disable Pydantic AI output or tool
retries. Set `retries: 0` in the agent specification to control those retries.

Fan out one independent run per input line:

```sh
printf '%s\n' one two three | agentman pipe --each "Classify this item"
```

Use `-j N` or `--jobs N` to set the client-side in-flight bound. With an
inferference model, `-j` without a value discovers the model’s advertised
`n_parallel` value. The bound only limits requests from this invocation; it
does not serialize the daemon or DBOS runtime.

## Inspect the record

```sh
agentman ls
agentman status
agentman show
```

`show` renders the latest thread message history as Markdown on demand. The
rendered Markdown is not stored and is not a second message format.

`status` reports the latest run, retry limits, physical request count, event
and snapshot counts, message storage size, and unresolved tool effects across
the thread. An unresolved effect is
shown as `unknown_after_crash` when the journal says a tool started but does
not contain enough evidence to prove its outcome.

## Resume behavior

Inspect before resuming:

```sh
agentman status
agentman resume <run-id>
```

The CLI prints the status view before asking for confirmation. `--yes` skips
only that confirmation:

```sh
agentman resume --yes <run-id>
```

Resume is refused for terminal runs, missing workspaces, unknown run ids, or
unresolved effects whose current filesystem state cannot be reconciled with
the recorded evidence. agentman delegates workflow resumption to DBOS and
does not replay tools itself. Both the CLI preflight and the daemon verify that
the request workspace matches the workspace stored on the run.

## Stop a turn

Cancel one run by id:

```sh
agentman cancel <run-id>
```

Use `--thread NAME` to cancel running turns in one named thread. Use
`--all-running` to cancel every running turn in the current workspace. A
client disconnect also cancels its running turn.
