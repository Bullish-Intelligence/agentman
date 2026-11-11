# Architecture

## Design rule

Before adding an agentman abstraction, check whether pydantic-ai, pydantic-ai
harness, or DBOS already owns that problem. The current code gives agentman
three product responsibilities:

1. resolve the workspace and thread;
2. identify the agent and model for a turn; and
3. store and expose the record.

The surrounding libraries retain ownership of agent semantics, model provider
integration, durable execution, and the semantic step journal.

## System shape

```text
repository shell
  └─ agentman CLI
       ├─ resolves workspace, config, spec, Python output type
       ├─ sends JSON + JSON Schema over HTTP on a Unix socket
       └─ validates returned JSON in the repository interpreter

Unix socket
  └─ agentman daemon (ASGI/uvicorn)
       ├─ owns the only Postgres connection path
       ├─ resolves model capabilities and router endpoint
       ├─ starts one DBOS workflow per turn
       ├─ streams AgentStreamEvent values as SSE
       └─ serves thread/run/status/resume queries

DBOS workflow
  └─ pydantic-ai Agent
       ├─ StepPersistence → step_persistence.* journal
       ├─ optional Coder → workspace tools
       ├─ DBOSDurability → DBOS-owned workflow state
       └─ model call → inferference or another pydantic-ai provider

Postgres
  ├─ DBOS-owned tables
  ├─ step_persistence.*
  └─ agentman.*
```

## Client and daemon boundary

The client resolves repository-local concerns because one system daemon must be
usable from many repositories and virtual environments. A `TurnRequest`
contains:

- the resolved workspace path;
- the thread id;
- the serialized pydantic-ai agent spec;
- the optional `coder` tool declaration;
- the optional provider transport retry limit;
- the agent and model labels;
- the prompt; and
- the output JSON Schema and output type label.

The daemon does not read its own current working directory, the caller’s
repository config, or repository Python modules. It receives data over the
socket and can therefore remain independent of the repository’s dependency
tree.

The daemon is the only Postgres client. Even `ls`, `show`, and `status` are
socket requests. This keeps the database URL and SQLAlchemy/asyncpg boundary
out of client-side repository environments.

## Typed output flow

1. An agent spec may declare `output: module:Attribute`.
2. The CLI resolves that reference against the spec directory using the
   repository interpreter.
3. That interpreter emits JSON Schema through `_schema_runner.py`.
4. The client sends the schema to the daemon.
5. The daemon builds a pydantic-ai `StructuredDict`; it does not import the
   Python model.
6. For `inferference:<name>`, the daemon checks router capabilities, resolves
   aliases to a resident model, and requires `SCHEMA` only when the request is
   typed.
7. The daemon returns JSON in the final SSE envelope.
8. The client runs pydantic validation in the repository interpreter and prints
   the canonical JSON or reports a finding.

For local inferference models, the model profile is merged over the provider
default to advertise native JSON Schema output and the router’s context window.
The schema sent to the OpenAI-compatible endpoint is flattened by the daemon’s
`StructuredDict` path, so acceptance and final validation are the meaningful
checks—not byte-for-byte equality with the source schema.

## Model resolution

The model boundary has two related paths. The daemon's turn path:

- checks inferference capabilities for `inferference:<name>` requests, resolves
  aliases through the shared capability resolver, and obtains context metadata
  for the canonical resident model;
- requires the router's `SCHEMA` constraint only when the client sends a typed
  output schema; default string turns do not require the constraint;
- resolves the router lazily if it was not configured globally, so a
  repository-selected inferference model still works;
- builds the model against the configured or resolved router endpoint; and
- hands any other model string to pydantic-ai unchanged when no transport
  retry limit was requested.

The local OpenAI-compatible route supports a per-run transport retry limit.
The daemon rejects an explicit limit for other provider strings before it
starts a workflow. Omission keeps the pinned OpenAI SDK default of two retries.
The SDK allows at most `N + 1` physical attempts for each logical model
request. Pydantic AI output and tool retries remain separate. Agentman does not
switch providers or models after an error.

The client-side `model.py` resolver supplies fan-out's default concurrency
from inferference's `ModelInfo.n_parallel`. Explicit `-j N` does not need a
capabilities lookup. Unknown aliases or missing resident models fail before a
workflow starts. agentman does not construct or own GPU schedulers;
inferference owns the long-lived router process.

## Workflow and concurrency

The runtime exposes one workflow boundary:

```python
@DBOS.workflow()
async def execute(turn: Turn) -> TurnResult: ...
```

The workflow id is read as the run id and passed unchanged to:

```text
agentman.run.id
  = DBOS workflow id
  = pydantic-ai run_id
  = StepPersistence run_id
```

The thread id is the pydantic-ai conversation id. This prevents a mapping
table or fuzzy reconciliation between subsystems.

agentman intentionally does not register a DBOS queue on the interactive path,
put a lock around `execute`, hold one exclusive connection for a turn, or add a
shared runtime semaphore. Postgres transactions are short and per store
method. Concurrent turns remain in flight so the model router can batch them
when the router supports it.

`pipe --each` adds only a per-invocation client semaphore. It limits how many
ordinary `/v1/turns` requests that one CLI invocation opens; it does not become
a daemon-wide queue.

## Agent construction and caching

The daemon builds pydantic-ai agents from the serialized spec. The capability
list contains:

1. a run-id-aware `StepPersistence` adapter;
2. optional file-effect evidence and harness `Coder` rooted at the request
   workspace; and
3. `DBOSDurability` with the daemon’s stream handler.

Agents are cached by the inputs that shape them, including the spec, model
inputs, transport retry limit, output schema, workspace, tool declaration,
store, and stream handler.
The cache is process-local. A cached agent is run-agnostic; its run id comes
from the individual pydantic-ai invocation.

Each local model client owns its retry limit. A context-local counter records
physical chat completion requests for the active run. Concurrent runs do not
share the counter or retry setting.

## Streaming

The model’s `AgentStreamEvent` values are converted to `StreamEnvelope` SSE
frames. The daemon keeps a `run_id → subscriber queues` registry only for live
connections. Events are dropped when nobody is listening; the durable journal
is authoritative.

DBOS may replay a durable step, so the daemon stamps request and event ordinals.
The client deduplicates on `(run_id, request_ordinal, event_ordinal)`.
Streaming is therefore a presentation path with replay tolerance, not the
source of truth for history. The current request path starts the workflow before
registering the subscriber, so an initial event can be missed. On success the
daemon emits a final `turn_result` frame after draining live events. On
workflow failure it emits a final `turn_error` frame; the CLI reports a finding
and operators inspect the durable run record. When a client disconnects, its
subscriber and local wait tasks are cleaned up. The daemon also cancels the
DBOS workflow and records `client_disconnected` on the run.

## Cancellation

The daemon cancels a workflow with `DBOS.cancel_workflow_async`. Only the model
step is preemptible. DBOS then cancels the model task and closes its upstream
stream. Tool steps stay non-preemptible so their side-effect records remain
complete.

When the cancelled workflow unwinds, the runtime records usage and metadata
directly through the product store. It cannot use another DBOS step after
cancellation. The daemon records the terminal status and reason, then sends a
final `turn_error` frame when the client remains connected.

## Module responsibilities

| Module | Responsibility |
| --- | --- |
| `cli.py` | Typer commands, config/spec resolution, output validation, rendering and exit codes |
| `config.py` | Flat global/repository config merge and provenance |
| `discovery.py` | Workspace and `.agentman` directory resolution |
| `structured_agents/spec.py` | Provider-neutral spec discovery plus `output:` and `tools:` parsing |
| `schema.py`, `_schema_runner.py` | Repository-interpreter schema generation and validation |
| `model.py` | Model and router capability resolution |
| `agent.py` | pydantic-ai agent and capability construction |
| `runtime.py` | DBOS workflow, history loading, run lifecycle, and cache |
| `daemon.py` | ASGI transport, lifecycle, SSE registry, Postgres wiring and routes |
| `client.py` | Unix-socket HTTP client, SSE parsing, fan-out collection and deduplication |
| `render.py` | On-demand Markdown from stored `ModelMessage` JSON |
| `status.py` | Product status composed with journal facts |
| `recovery.py` | Resume authorization; never a replay engine |
| `persistence/store.py` | Product-facing thread/run CRUD |
| `persistence/step_store.py` | Harness `StepStore` contract over Postgres |
| `persistence/inspect.py` | Product-facing queries over the journal |

`structured_agents` must not import `agentman`, DBOS, SQLAlchemy, or HTTP
transport code. Import-linter contracts enforce that direction.
