# Persistence and recovery

## Ownership model

There is one Postgres database and three ownership domains:

```text
Postgres database
├── DBOS-owned tables
│   └── workflow state, step history, workflow inputs/outputs
├── step_persistence.*
│   └── harness semantic journal and tool-effect ledger
└── agentman.*
    └── product-facing threads and runs
```

agentman shares the database and canonical identifiers, not persistence
abstractions. It never reads or writes DBOS private tables, never folds journal
rows into the product `run` table, and never attempts a cross-domain
transaction. Alembic owns `agentman.*` and `step_persistence.*`; DBOS owns its
schema.

Run:

```sh
agentman migrate
```

before serving requests. The request path does not create tables.

## Product index

### `agentman.thread`

Stores the user-facing conversation identity:

- generated id;
- resolved workspace;
- optional display name; and
- creation/update timestamps.

Thread names are labels and are not unique. `agentman use <name>` selects the
most recently updated matching thread in the current workspace. If names
collide, `ls` shows a short id so the records remain distinguishable.

### `agentman.run`

Stores one turn’s product metadata:

- canonical run id;
- thread and workspace;
- agent and model labels;
- prompt;
- `running`, `completed`, or `failed` status;
- output JSON and output type;
- fallback `messages_json`; and
- usage and timestamps; and
- non-secret run metadata with requested and effective transport retry limits,
  fallback state, logical turn count, and physical model request count.

The product row is written through DBOS steps so recovery does not attempt a
second plain insert for the same workflow id. A failure to record `failed` is
logged without replacing the original turn exception.

## Semantic journal

`step_persistence.*` mirrors the pydantic-ai harness `StepStore` contract:

- `runs` records journal runs and conversation ids;
- `events` records semantic step events with idempotency protection;
- `messages` stores content-addressed `ModelMessage` documents;
- `snapshots` stores ordered message-hash frontiers;
- `tool_effects` stores tool lifecycle and optional evidence; and
- `snapshot_idempotency_keys` keeps snapshot replay protection after pruning.

The Postgres adapter intentionally follows the upstream store semantics:

- registering a duplicate run is an error, not an upsert;
- duplicate idempotent events are suppressed;
- snapshot identity is a storage sequence, not a step index;
- tool effects upsert by `(run_id, tool_call_id)`; and
- each store method owns one short transaction.

### Content-addressed messages

A snapshot contains the ordered message history, but Postgres stores each
distinct message once per run by a SHA-256 content hash. The snapshot stores
the ordered hash array. This makes snapshots linear in the number of distinct
messages within a run instead of duplicating the entire history in every
snapshot row.

Deduplication is per run, not per thread. Each turn stores the history it saw,
so a long thread can produce quadratic message-row growth across its runs.
The status view exposes message-row count and byte size; no pruning policy is
enabled by default because the trace is the product and context limits are
expected to matter first.

## History loading

For a new turn, the runtime:

1. finds prior harness runs for the thread;
2. prefers the most recent usable harness snapshot;
3. excludes the current run id during replay;
4. falls back to the product run’s `messages_json` if no snapshot is usable;
5. passes the resulting `ModelMessage` list to pydantic-ai.

The fallback exists for resilience and is tested. Markdown rendering is a
view over this stored message representation, never an alternate history
format.

## Tool effects and evidence

`tools: coder` enables harness `Coder` rooted at the request workspace. For
the file operations agentman understands—`write_file` and `edit_file`—the
`FileEffectEvidence` capability records:

- the resolved target path;
- a pre-image SHA-256 hash; and
- a post-image SHA-256 hash when the tool returns.

The capability is ordered after `StepPersistence`, so the started effect row
exists before evidence is attached. Shell effects receive no generic evidence:
shell can mutate arbitrary state and cannot be reconciled safely by a generic
hash rule.

This is evidence, not an exactly-once guarantee. A filesystem write can still
land before the completion record is persisted. The evidence lets the recovery
policy distinguish some cases without pretending it can solve arbitrary side
effects.

## Crash windows

The important states are:

| Crash point | Durable fact | Resume consequence |
| --- | --- | --- |
| Before the effect starts | No started effect | DBOS may resume ordinary unrecorded work |
| After `started`, before mutation | Effect started; current file matches pre-image | A known file effect may be reconciled as not landed |
| After mutation, before completion record | Effect started; current file matches post-image | A known file effect may be reconciled as landed; arbitrary or inconclusive effects remain refused |
| After completion record | Terminal effect record exists | DBOS replay uses the durable step result |

When a started effect cannot be reconciled, status preserves the harness state
and reports the product note `unknown_after_crash`. agentman does not invent a
fourth harness lifecycle state and does not silently run the tool again.

## `status`

`agentman status` composes:

- the current workspace thread;
- latest run metadata;
- requested and effective transport retry limits and physical request count;
- event and snapshot counts for the latest run;
- message-row and byte statistics across the thread; and
- unresolved effects across every run in the thread.

This matters because a later successful turn must not hide an unresolved effect
from an earlier interrupted turn.

For the local OpenAI-compatible model path, the physical request count includes
each chat-completion HTTP attempt, including SDK retries. The count is null for
provider paths that do not expose this transport counter. Existing runs have
empty metadata and do not claim a retry count.

## `resume`

The recovery path has a deliberately small policy boundary:

1. Verify that the run id is an agentman run.
2. Verify that the request workspace matches the workspace stored on the run.
3. Refuse terminal runs.
4. Refuse a missing workspace.
5. Ask harness recovery inspection for unresolved effects.
6. Reconcile known file evidence against the current pre/post image.
7. Refuse absent, malformed, out-of-workspace, or inconclusive evidence.
8. Hand the canonical run id to `DBOS.resume_workflow_async`.

DBOS owns workflow resumption. Harness owns journal inspection. agentman owns
the policy that decides whether the recorded facts authorize resumption. The
workspace comparison is enforced in the daemon as well as preflighted by the
CLI, so a client cannot bypass workspace scoping by calling the socket API
directly.

## Operational cautions

- Keep the database available before starting the daemon.
- Treat the Unix socket as a security boundary. When `coder` is enabled, a
  caller that can access the socket can execute commands as the daemon user.
- Keep the daemon and DBOS version compatible with existing workflow records.
  The current dependency is DBOS 3.0.x; do not run incompatible old/new
  processes against the same application records without a migration plan.
- Back up Postgres as a unit. The product index, semantic journal, and DBOS
  state jointly explain a durable run.
- Alembic owns `agentman.*` and `step_persistence.*`; DBOS owns `dbos.*`.
  Do not manage DBOS tables with Agentman's Alembic migrations. A restore
  rehearsal must include all three schemas and confirm the product revision
  before starting the daemon.
