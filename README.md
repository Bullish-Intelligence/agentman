# agentman

A CLI for talking to typed agents. Every model call and tool call is recorded,
always.

The project has a durable, workspace-scoped agent runtime with:

- typed agent specifications and client-side output validation;
- a Unix-socket client/daemon split with SSE streaming;
- Postgres-backed threads, runs, model events, snapshots, messages, and tool
  effects;
- workspace-scoped `coder` tools with file-image evidence for recovery;
- status inspection and policy-gated DBOS workflow resumption; and
- concurrent stdin fan-out through `pipe --each`.

The detailed documentation set is the source for the product and operational
picture:

- [Documentation index](docs/README.md) — reading paths and document map.
- [Overview](docs/overview.md) — what exists, what it means, and the current
  status boundaries.
- [Getting started](docs/getting-started.md) — development setup and first
  typed turn.
- [Architecture](docs/architecture.md) — request flow, ownership, durability,
  streaming, and concurrency.
- [Persistence and recovery](docs/persistence-and-recovery.md) — schemas,
  snapshots, effects, crash behavior, and `resume`.
- [CLI and configuration reference](docs/reference.md) — commands, config,
  agent specs, exit codes, and the socket API.
- [Development guide](docs/development.md) — repository layout, invariants,
  verification, and contribution workflow.
- [Roadmap and gaps](docs/roadmap.md) — what the system should have next and
  what is not yet operationally complete.
- [ADR 0001](docs/adr/0001-system-wide-ai-activity-record.md) — proposed
  activity record across Pi, Claude Code, and Codex, with Atuin owning commands.

Run everything inside `devenv shell`. See `AGENTS.md` for the working agreement.

## Getting started

```bash
devenv shell          # enter the pinned environment
uv sync --all-extras  # app dependencies and the dev tools
devenv up -d          # start the project Postgres service
```

See [Getting started](docs/getting-started.md) for daemon configuration and a
first typed turn.

## Verify

Run the checks from the repository root:

```bash
devenv shell -- ruff check src tests
devenv shell -- ruff format --check src tests
devenv shell -- ty check src
devenv shell -- pytest
```

The `devenv.nix` tasks expose the same checks. The default pytest run is
hermetic. Integration tests need `AGENTMAN_RUN_INTEGRATION=1` and an explicit
`AGENTMAN_TEST_DATABASE_URL`. Keep `devenv.lock` and `uv.lock` in Git.

## Version control

Use Git. Keep a change focused. Keep `devenv.lock` and `uv.lock` in the same
change as the inputs they pin. Run the checks before you push. Open a pull
request for review.

## Layout

```text
src/agentman/             # package source
src/structured_agents/    # provider-neutral agent specification
tests/                    # pytest tests
docs/                     # product, architecture, and operations documentation
devenv.nix                # shell, services, and check tasks
devenv.yaml               # inputs
pyproject.toml            # project metadata and tool configuration
```

## Author

Bullish Design <242310653+Bullish-Intelligence@users.noreply.github.com>
