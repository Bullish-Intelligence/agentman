# Development guide

## Repository workflow

This is a Python repository with a devenv shell. Use the shell for all
commands. Keep a change focused, run the checks, and review the result before
you push.

Initial setup:

```sh
devenv shell
uv sync --all-extras
```

The repository's `AGENTS.md` is the canonical working agreement. It also
defines the exit-code convention used by the application.

`agentman version` reports the installed package version and socket protocol
version. `GET /v1/health` reports the same pair so operators can compare the
client and daemon before a request.

## Two distributions

The repository is a uv workspace. It builds two distributions:

| Distribution | Folder | Content |
| --- | --- | --- |
| `agentman-client` | `packages/agentman-client/` | `agentman.client`, `agentman.protocol`, `agentman.config`, `agentman.discovery`, `agentman/__init__.py`, and `structured_agents` |
| `agentman` | repository root | the daemon, runtime, persistence, CLI, and migrations |

`agentman-client` depends on `pydantic`, `pydantic-ai-slim` (no extras),
`httpx`, and `pyyaml`. It installs none of DBOS, SQLAlchemy, asyncpg, psycopg,
Alembic, uvicorn, `pydantic-ai-harness`, or `inferference`.

`agentman` depends on `agentman-client` at the same version. Both wheels
install into one `agentman/` folder. Only the client wheel ships
`agentman/__init__.py`. That file extends the package path, so
`agentman.daemon` resolves from the root wheel, and from the root source tree
in an editable install. Do not add `src/agentman/__init__.py` to the root
distribution.

A client-only program, such as a web UI, depends on the client alone:

```toml
[project]
dependencies = ["agentman-client"]

[tool.uv.sources]
agentman-client = { git = "https://github.com/Bullish-Intelligence/agentman.git", subdirectory = "packages/agentman-client", tag = "vX.Y.Z" }
```

A downstream uv2nix build of `agentman` resolves the `agentman-client`
workspace member from the same `uv.lock`. Build both members from one source
checkout. Do not look for `agentman-client` in an index.

### Version bump

Both distributions use one version. Change all three places in one commit:

1. `version` in `pyproject.toml`;
2. `version` in `packages/agentman-client/pyproject.toml` and the
   `agentman-client==X.Y.Z` pin in `pyproject.toml`;
3. `__version__` in `packages/agentman-client/src/agentman/__init__.py`.

Then run `uv lock` and commit `uv.lock` in the same change. Add the release to
`CHANGELOG.md`. `tests/test_versions.py` fails when the versions differ.

### Packaging test

`tests/test_packaging.py` builds both wheels and installs them into clean
virtual environments. It is slow and may need the network, so it is off by
default:

```sh
AGENTMAN_RUN_PACKAGING=1 devenv shell -- uv run pytest tests/test_packaging.py
```

Set `AGENTMAN_PACKAGING_OFFLINE=1` to install from the uv cache only. The test
checks three things:

- a venv with only the client wheel holds none of the server packages;
- in that venv, `Client`, `TurnRequest`, and `load_spec` import, and the tests
  in `packages/agentman-client/tests/` pass;
- a venv with both wheels imports `agentman.daemon` and runs `agentman version`.

### Import contracts

import-linter checks both source trees. The contract "the client distribution
has no server imports" blocks `agentman.client`, `agentman.protocol`,
`agentman.config`, `agentman.discovery`, and `structured_agents` from
importing a server module or a server package.

grimp reads the package location before it imports anything. In a development
checkout, `agentman` has two source folders, so run the checker through the
helper. It imports `agentman` first:

```sh
devenv shell -- uv run python tests/support/lint_imports.py
```

`tests/test_import_contracts.py` runs the helper. It also proves that the client
contract fails when a client module imports `agentman.daemon`,
`agentman.runtime`, `agentman.persistence`, `sqlalchemy`, or `dbos`.

## Repository map

```text
packages/agentman-client/          the agentman-client distribution
  pyproject.toml
  src/agentman/
    __init__.py          version and package path extension
    client.py            Unix-socket client and SSE handling
    protocol.py          versioned socket request/response models
    config.py            flat configuration and provenance
    discovery.py         workspace and config directory resolution
  src/structured_agents/
    spec.py              provider-neutral spec loader and extensions
  tests/                 tests that need only the client distribution
src/agentman/            the agentman distribution (no __init__.py)
  cli.py                 public command surface and exit codes
  daemon.py              ASGI daemon, routes, DBOS lifecycle, socket serving
  model.py               inferference and pydantic-ai model resolution
  agent.py               AgentSpec → pydantic-ai Agent construction
  runtime.py             DBOS workflow and history loading
  schema.py              repository-interpreter schema/validation bridge
  recovery.py            resume authorization policy
  effects.py             file-effect pre/post hash evidence
  status.py              product/journal status composition
  render.py              on-demand Markdown view
  persistence/
    tables.py            SQLAlchemy metadata
    store.py             agentman thread/run CRUD
    step_store.py        harness StepStore Postgres adapter
    inspect.py            journal inspection queries
migrations/
  versions/0001_initial.py  frozen product/journal schema
tests/
  test_*.py              unit, integration, crash, live-rig, and CLI tests
docs/
  *.md                   user-facing documentation
```

## Architectural contracts

Changes should preserve these contracts unless the design and docs change with
them:

### One canonical run id

The DBOS workflow id, pydantic-ai `run_id`, harness `run_id`, and
`agentman.run.id` are the same value. Do not add a translation table.

### The daemon is the only Postgres client

CLI reads and writes go through the socket. Keep SQLAlchemy, asyncpg, product
store, journal, and DBOS wiring on the daemon side. Import-linter contains an
explicit client boundary contract.

### The client owns repository Python types

The daemon receives JSON Schema, not a Python class. Resolve and validate
output types with the repository interpreter; do not import repository modules
into the daemon.

### No interactive serialization

Do not introduce a DBOS queue, process-wide lock, exclusive connection, or
daemon-wide semaphore around `execute()`. Fan-out’s semaphore is deliberately
local to one client invocation.

### Record before convenience

The journal is authoritative for model messages, snapshots, events, and tool
effects. Markdown rendering and SSE are views. A feature that introduces a
second message or event representation needs a written justification.

### Recovery facts, not replay permission

Harness inspection supplies facts. agentman’s recovery module applies a small
policy. Unknown side effects are surfaced and refused; they are not silently
rerun.

### Workspace is a security and correctness input

Workspace participates in thread identity, client state, tool roots, and agent
cache shape. Any capability that can touch the filesystem must be keyed and
scoped by the request workspace.

## Verification

The default suite is hermetic. Integration tests that need Postgres are gated
by `AGENTMAN_RUN_INTEGRATION=1`; the live router suite additionally uses
`AGENTMAN_RUN_LIVE_RIG=1`.

Run the checks from the repository root:

```sh
devenv shell -- ruff check src tests packages
devenv shell -- ruff format --check src tests packages
devenv shell -- ty check src packages/agentman-client/src
devenv shell -- pytest
```

`pytest` collects `tests/` and `packages/agentman-client/tests/`.

The `devenv.nix` tasks expose the same checks, and import-linter enforces the
package boundaries.

For the project Postgres cluster:

```sh
devenv up -d
devenv shell -- env AGENTMAN_RUN_INTEGRATION=1 \
  AGENTMAN_TEST_DATABASE_URL=postgresql+asyncpg://<role>@127.0.0.1:<port>/<fresh-test-database> \
  uv run pytest
```

Integration mode requires an explicit `AGENTMAN_TEST_DATABASE_URL`; the
fixture applies `alembic upgrade head` before the tests. Use a fresh or known
test database. Do not rely on a familiar port or database name as proof of
ownership.

Useful focused suites include:

| Suite | Evidence |
| --- | --- |
| `test_cli.py`, `test_cli_integration.py` | command behavior and exit codes |
| `test_agent.py` | the model step is the only preemptible DBOS step |
| `test_cancellation.py`, `support/slow_upstream.py` | model stream cancel closes the upstream connection; the slow upstream serves the stream |
| `test_cancel_integration.py` | cancel, disconnect, and recovery against Postgres, including a shell command stopped through the socket |
| `test_tool_cancel.py` | a cancel stops real shell commands through the harness, `cancel_requested`, the final frame, and the startup repair |
| `test_runtime.py` | workflow lifecycle, caching, canonical ids |
| `test_daemon_integration.py` | real daemon transport and concurrency overlap |
| `test_step_store_integration.py`, `test_store_integration.py` | Postgres product/journal behavior |
| `test_crash_windows.py`, `test_recovery_integration.py` | crash-window and resume policy |
| `test_live_rig.py` | live router capabilities, schema request, and model path |
| `test_systemd.py` | passed-fd serving behavior |
| `test_versions.py` | both distributions and `__version__` state one version |
| `test_import_contracts.py` | the import-linter contracts hold; the client contract catches a server import |
| `test_packaging.py` | gated; clean-venv install of the client wheel and of both wheels |

The default test run does not prove Postgres recovery or a live model path.
Always name the gate when reporting evidence.
The live-rig tests default to the `gemma` model. Set `AGENTMAN_LIVE_MODEL` to
a resident model name when the configured router uses another model; the tests
read that model's advertised parallel slot count.

## Fresh-machine acceptance

This procedure applies to the intended machine installation, not to the
hermetic project shell. It must be run against a machine where the current
agentman package is installed, the systemd socket and service units are active,
and machine Postgres has an `agentman` database and role. That installation is
configured outside this repository.

Record the package version, source revision, toolchain revision, and the exact
check commands before starting. Then verify:

Run `agentman version` to record the package and protocol versions. Record the
installed toolchain manifest and the active system generation as well.

1. On reboot, no agentman daemon process is running before the first request.
2. The socket unit exposes `%t/agentman.sock` with mode `0600`.
3. The first `agentman ls` activates the service and reaches the daemon.
4. `agentman migrate` completes against machine Postgres without a
   repository-tracked credential.
5. A fresh clone can define a minimal repository agent and typed output model
   without installing agentman into the clone's dependency tree.
6. A typed `agentman send --json` succeeds from that clone, and
   `agentman status`, `agentman show`, and `agentman resume` inspect the
   same durable record. A terminal run cannot resume and returns HTTP 409.
7. A repository with `tools: coder` is rejected or accepted only according
   to the intended socket permission boundary; never broaden the socket mode
   for convenience.
8. The daemon is using the machine database and does not bind its own Unix
   socket when systemd has supplied the listening descriptor.

Capture command output and service/database logs as acceptance evidence. A
passing project test run is not a substitute for this check. Record the results
in the release evidence record. If a reboot or privileged system change cannot
run, record the exact blocker and leave the affected acceptance steps pending.

## Release procedure

Create a release from a verified change on the main branch:

```sh
git switch -c release-x-y-z
# bump the version in both pyproject.toml files and in __version__ (see "Version bump")
git commit -m "chore: bump version to X.Y.Z"
# run the checks above
git switch main
git merge --ff-only release-x-y-z
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin main --tags
```

Before the release tag is created, the evidence record must name the
hermetic check, a fresh or migrated Postgres integration check, live-router
typed validation when applicable, crash-window recovery, fan-out ordering, and
the fresh-machine and systemd acceptance result. The release note must state
which checks ran and which remain unavailable; “tests pass” is not sufficient.

## Database changes

The current Alembic revision is intentionally frozen as an explicit schema
description. When changing SQLAlchemy metadata:

1. decide whether the data-model change is actually required;
2. update the migration explicitly rather than importing live metadata into an
   old revision;
3. add migration and integration tests;
4. verify metadata/migration drift checks; and
5. document compatibility and recovery implications.

DBOS’s private schema is not an agentman migration target. Keep application
tables, journal tables, and DBOS tables in their ownership domains.

## Dependency changes

The important compatibility pins are intentional:

- `pydantic-ai-harness` is capped below the next minor because the run-id
  adapter depends on an internal `for_run` behavior;
- DBOS is pinned to the stable 3.0 line; and
- inferference is pinned to the release that provides the agentman capability
  contract.
- `[tool.uv.extra-build-dependencies]` adds hatchling for inferference. The
  uv2nix builds of nix-meta and vendomat need it for the inferference git
  source. Keep it, even though `uv sync` works without it.

Read the relevant upstream behavior and record the reason before widening a
range. A dependency bump that changes workflow or step storage requires the
recovery acceptance suite, not just import success.

## Documentation maintenance

When changing a user-visible command, config key, protocol field, storage
invariant, or operational assumption, update the matching document in the same
change. In particular:

- command/config changes → `docs/reference.md` and `docs/getting-started.md`;
- runtime or boundary changes → `docs/architecture.md`;
- persistence/recovery changes → `docs/persistence-and-recovery.md`;
- status or deployment changes → `docs/overview.md` and `docs/roadmap.md`;
- repository workflow changes → this document and `AGENTS.md` if the project
  law changed.

The docs should link to source concepts and tests by stable path, but should
avoid pretending a design note, a unit test, and a deployed service are the
same kind of evidence.
