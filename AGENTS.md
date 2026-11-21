# AGENTS.md — project instructions

> `CLAUDE.md` is a symlink to this file, so every agent tool reads the same text.

## What this repository is

agentman is a Python CLI and daemon for typed agents. Every model call and tool
call is recorded. The package is under `src/agentman/`; the provider-neutral
agent specification is under `src/structured_agents/`; the tests are under
`tests/`; the documentation is under `docs/`.

## Working here

Run project commands inside the `devenv` shell. The shell pins Python and
provides `uv`, the local Postgres service, and the check tools.

```bash
devenv shell
uv sync --all-extras
uv run ruff check src tests packages
uv run ruff format --check src tests packages
uv run ty check src packages/agentman-client/src
uv run pytest
uv run python tests/support/lint_imports.py
```

Start the project Postgres service with `devenv up -d`. The default pytest run
is hermetic. Integration tests need `AGENTMAN_RUN_INTEGRATION=1` and an explicit
`AGENTMAN_TEST_DATABASE_URL`.

Keep `devenv.lock` and `uv.lock` in Git. Put a lock change in the same change as
the input that caused it. Do not commit local environment overrides, generated
files, credentials, keys, or database dumps.

## Layout

- `src/agentman/` — the `agentman` distribution: CLI, daemon, model resolution,
  runtime, recovery, persistence, and status. It has no `__init__.py`.
- `packages/agentman-client/` — the `agentman-client` distribution, a uv
  workspace member with no server dependencies: `agentman/__init__.py`, client,
  protocol, config, discovery, and the provider-neutral spec loader
  (`structured_agents`). Its tests are in `packages/agentman-client/tests/`.
- `migrations/` — the frozen Alembic schema history.
- `CHANGELOG.md` — release notes for both distributions.
- `tests/` — unit, integration, crash-window, live-rig, and CLI tests.
- `docs/` — the user-facing product, architecture, operations, and reference
  documents.
- `devenv.nix`, `devenv.yaml` — the development shell, services, and checks.
- `pyproject.toml` — project metadata, dependencies, and tool configuration.

## Rules

- Keep the public command behavior and exit codes stable. Exit code `0` is ok,
  `1` means act on findings, and `2` means the command could not run.
- An additive protocol change gets no version bump; a breaking change bumps `PROTOCOL_VERSION`. Clients ignore unknown response fields, the daemon rejects unknown request fields, and a new optional request feature gets a health capability. See `docs/reference.md`.
- The daemon is the only Postgres client. CLI reads and writes go through the
  Unix socket.
- The canonical run id is the DBOS workflow id, the pydantic-ai run id, the
  harness run id, and `agentman.run.id`. Do not add a translation table.
- The journal is authoritative for model messages, snapshots, events, and tool
  effects. Markdown rendering and SSE are views.
- Recovery applies a small policy to harness facts. It never silently replays an
  unknown side effect.
- A request workspace is part of thread identity, client state, tool roots, and
  the agent cache shape. Key every filesystem capability by that workspace.
- Resolve repository output types in the repository interpreter. The daemon
  receives JSON Schema, not repository Python objects.
- Do not serialize turns behind a daemon-wide lock, queue, or exclusive
  connection. The `pipe --each` bound is local to one client invocation.

- Keep one version for both distributions. Change both `pyproject.toml` files
  and `__version__` together. `docs/development.md` lists the steps.
- The client modules and `structured_agents` must not import a server module or
  a server package. An import-linter contract and a test enforce this.

## Documentation

Update the matching document in the same change as the behavior:

- commands and configuration: `docs/reference.md` and
  `docs/getting-started.md`;
- runtime and boundaries: `docs/architecture.md`;
- persistence and recovery: `docs/persistence-and-recovery.md`;
- status, deployment, and remaining work: `docs/overview.md` and
  `docs/roadmap.md`;
- contributor workflow: `docs/development.md` and this file.

The implementation and tests are the final evidence. Do not describe a design
note as a shipped feature.

## Version control

Use Git. Keep a change focused, run the checks before you push, and open a pull
request for review. Do not add attribution lines to commits or pull requests.
