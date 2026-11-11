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

## Repository map

```text
src/agentman/
  cli.py                 public command surface and exit codes
  client.py              Unix-socket client and SSE handling
  config.py              flat configuration and provenance
  discovery.py           workspace and config directory resolution
  daemon.py              ASGI daemon, routes, DBOS lifecycle, socket serving
  model.py               inferference and pydantic-ai model resolution
  agent.py               AgentSpec → pydantic-ai Agent construction
  runtime.py             DBOS workflow and history loading
  protocol.py            versioned socket request/response models
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
src/structured_agents/
  spec.py                provider-neutral spec loader and extensions
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
devenv shell -- ruff check src tests
devenv shell -- ruff format --check src tests
devenv shell -- ty check src
devenv shell -- pytest
```

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
| `test_cancel_integration.py` | cancel, disconnect, and recovery against Postgres |
| `test_runtime.py` | workflow lifecycle, caching, canonical ids |
| `test_daemon_integration.py` | real daemon transport and concurrency overlap |
| `test_step_store_integration.py`, `test_store_integration.py` | Postgres product/journal behavior |
| `test_crash_windows.py`, `test_recovery_integration.py` | crash-window and resume policy |
| `test_live_rig.py` | live router capabilities, schema request, and model path |
| `test_systemd.py` | passed-fd serving behavior |

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
# bump the version in pyproject.toml
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
