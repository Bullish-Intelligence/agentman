# Changelog

agentman ships two distributions at one version: `agentman` (the daemon and the
CLI) and `agentman-client` (the client, the protocol models, and the spec
loader).

## Unreleased

### Runs

- Record the facts of each outgoing chat-completion request in
  `RunMetadata.chat_requests`: the model, the output-length fields as sent, the
  body size, the body SHA-256, the URL host and path, and the time. A record
  never holds the prompt or a credential. The field is optional, so older
  clients and older daemons still work. The protocol version stays `4`.

## 0.1.0 — 2026-11-21

This release breaks the socket protocol. Protocol 4 does not work with
protocol-3 clients. Upgrade `agentman` and `agentman-client` together.

### Packaging

- Add the `agentman-client` distribution. It holds `agentman.client`,
  `agentman.protocol`, `agentman.config`, `agentman.discovery`, and
  `structured_agents`. It installs none of DBOS, SQLAlchemy, asyncpg, psycopg,
  Alembic, uvicorn, `pydantic-ai-harness`, or `inferference`.
- The `agentman` distribution now depends on `agentman-client` at the same
  version. Both install into one `agentman` package. No import path, command,
  or option changes.
- The repository is a uv workspace. A downstream uv2nix build of `agentman`
  also resolves the `agentman-client` workspace member from `uv.lock`.
- A client-only program can depend on `agentman-client` from git with
  `subdirectory = "packages/agentman-client"`.
- Add an import-linter contract: the client modules must not import server
  modules or server packages.
- Add a gated packaging test (`AGENTMAN_RUN_PACKAGING=1`) and a test that both
  distributions state one version.

### Protocol and runs

- Move the socket protocol to version 4, with a compatibility rule.
- Make the event ordinal one sequence for each run.
- Emit `run_started`, and let a client choose the run id.
- Let a caller attach metadata to a run.
- Stop the shell commands of a run when it is cancelled.
