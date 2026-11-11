# Roadmap and gaps

This document answers the second half of the project question: what should
exist around the current implementation for agentman to be a complete,
dependable product? It separates unfinished work from intentionally absent
work.

## Current state at a glance

```text
Core record             complete in code and tests
Tools and recovery      complete in code and tests
DBOS 3.0 migration      complete in code and recovery evidence
Fan-out                 complete in code and tests
Run cancellation       complete in code and tests
Deployment/operations   installed and verified; reboot, second-UID, clone, and sustained-router checks are documented
Product expansion       proposed; concept review pending
```

## Run cancellation

The daemon cancels runs by id. The CLI also selects running runs by thread or
workspace. A client disconnect cancels its turn. The model step stops within
about one second. An active tool step finishes so its side-effect record stays
complete.

The next cancellation work is:

- update karakeep-archive to cancel after a client timeout when health reports
  the `cancel` capability;
- allow `max_transport_retries` for `openai:` models;
- count usage from interrupted model requests when the server exposes it;
- subscribe before workflow startup so the first stream events are not lost;
- change Strata only if the live probe proves that it keeps generating after
  the client closes its stream.

## P0 — complete the operating model

### 1. Prove the machine install is reproducible

The machine configuration installs agentman beside the shared toolchain and
declares its socket-activated service and machine Postgres role. The package is
pinned, built, and portable. A consumer without an agentman dependency completed
a typed request through the installed client. The remaining acceptance limits
are:

- install agentman beside the existing machine toolchain;
- update it through the same machine synchronization mechanism as the other
  managers;
- verify a fresh clone can use the client without adding agentman to the clone’s
  dependency tree; and
- document how the installed client discovers the repository interpreter for
  typed outputs.

### 2. Verify systemd socket/service units

The daemon code supports `--systemd-socket` and accepts exactly one passed
descriptor. The machine configuration provides:

```ini
# agentman.socket
ListenStream=%t/agentman.sock
SocketMode=0600
Accept=no

# agentman.service
ExecStart=agentman serve --systemd-socket
Restart=on-failure
```

The socket should be enabled while the service is socket-activated. The unit
must not expose a broader permission mode: enabling `coder` gives callers with
socket access arbitrary command execution as the daemon user.

A controlled check should start the service from an installed `agentman ls`
request and confirm that the request returns HTTP 200. The service migration
step must also exit 0.

Remaining acceptance evidence:

- no daemon process after reboot before the first request;
- the daemon does not bind the path when systemd has passed fd 3; and
- a socket mode of `0600`.

### 3. Verify machine Postgres

The machine configuration declares a dedicated `agentman` database and role,
peer authentication, and a global daemon database URL without repository
credentials. A migration must succeed against that database. Normal installed
use should work without `devenv up`.

### 4. Close the deployment acceptance loop

The installed path and a consumer typed turn must pass. The consumer should
have no agentman dependency. A reboot and a direct second-UID probe remain
unrun. Sustained inference availability is unproven. See the release evidence
record for exact commands and logs.

## Proposed future addition

[ADR 0001](adr/0001-system-wide-ai-activity-record.md) was rejected after its
concept review.
[ADR 0002](adr/0002-external-coding-agent-jobs.md) proposes a bounded way to
start and track jobs that agentman launches through external coding agents.
Its user flow and failure rules need requirements review before implementation.
The remaining operating-model checks stay separate.

## P1 — improve maintainability and operator confidence

### 1. Keep the status surface useful

The status view already shows message row/byte counts. The daemon now reports
its package and socket-protocol versions through `agentman version` and
`/v1/health`. The next useful operator-facing measurements are cache size and
database connectivity/latency. Add measurements only when they answer an
operational question; do not turn status into a second observability system.

### 2. Add explicit compatibility checks for records

DBOS 3.0 is the current stable line. Future DBOS upgrades should be gated by:

- the four crash-window tests;
- process-isolated recovery of an existing unfinished workflow;
- a migration note describing workflow input/output storage compatibility; and
- an explicit policy for old processes and new processes sharing a database.

The current design does not assume arbitrary old/new DBOS versions can safely
run concurrently.

### 3. Strengthen release evidence

Before a release, the project should have a repeatable evidence bundle covering:

- hermetic verification;
- integration verification against a fresh/migrated Postgres database;
- live router schema enforcement and typed validation;
- crash-window recovery;
- fan-out concurrency and ordered output; and
- the actual machine/systemd path.

The release note should state which gates ran, not just say “tests pass.”

### 4. Decide whether a second consumer warrants extracting `structured_agents`

`structured_agents` is currently an internal provider-neutral boundary. Keep it
small while agentman is its only consumer. A second real consumer is the
trigger to define a public API, release it separately, and document
compatibility; do not widen it speculatively.

## P2 — revisit only when real use demands it

These are valid questions, but not current implementation requirements:

- message retention or pruning after measured storage pressure;
- cache eviction/observability for a long-lived multi-repository daemon;
- richer effect evidence for operations beyond file writes;
- a container/CI spawn-if-absent client fallback when systemd is unavailable;
- router metrics that prove actual server-side batching rather than request
  overlap; and
- richer thread navigation or a durable current-thread abstraction.

Each needs a concrete trigger. The design’s stopping rule is to add nothing
that real use has not demanded.

## Deliberately not on the roadmap

Unless the product boundary changes, do not add:

- a second database or SQLite daemon fallback;
- a generic exactly-once wrapper around arbitrary tools;
- automatic replay of unknown shell/filesystem effects;
- a separate stored Markdown or event format;
- generalized model alias management beyond inferference capability aliases;
- a batch product noun for `pipe --each`; or
- a daemon-wide queue that would serialize interactive model calls.

These omissions protect the core record and concurrency invariants.

## Documentation gaps now closed

Before this documentation set, the repository had only a short README and
large internal design notes. That left several practical questions unanswered:

- how to start the database, migrate, and serve the daemon;
- where config and specs live and which scope owns each key;
- how typed output crosses the client/daemon boundary;
- what `status` and `resume` actually guarantee;
- what the database stores and who owns each schema;
- how `pipe --each` differs from batching; and
- which production/systemd pieces are code-supported versus active.

The documents in this directory now answer those questions. Future feature
work should update them as part of the same change, with status language that
matches the available evidence.
