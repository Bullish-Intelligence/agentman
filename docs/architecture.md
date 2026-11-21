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
The request ordinal is `ctx.run_step`. The event ordinal is one sequence for the
whole run, so the client deduplicates on `(run_id, event_ordinal)`.

Inside a DBOS step, the event ordinal is `(step_id << 20) | index`. `step_id` is
`DBOS.step_id`. It counts up in call order and stays the same when DBOS recovers
the run. `index` is the position of the event in that handler call. DBOS gives
each tool event its own step, and one step for each model request stream. A
replayed step therefore yields the same ordinals, and the client drops the
copies. DBOS step ids start at 1, so these ordinals are 1 or more. A handler call
that emits 2**20 events or more raises an error. Outside a DBOS step, as in
`run_turn` in tests, the subscriber registry counts events for the run, from 1,
and forgets the counter when the last subscriber leaves. Ordinal 0 belongs to
the `run_started` frame. The final frame keeps request and event ordinal `-1`.

The daemon chooses the run id before it starts the workflow. It takes the id from
`TurnRequest.run_id`, or it makes a UUID. It subscribes to the id, then sends the
response start and a first `run_started` frame, then starts the workflow with
`SetWorkflowID`. The subscriber therefore exists before the workflow can publish,
and the client knows the run id before any model event. The daemon answers `409`
when a live stream or an existing run or workflow already uses the id, because
DBOS itself returns the existing workflow for a duplicate id.

The daemon also keeps a map of live runs, from the reservation of the id to the
end of the stream. The workflow writes the run row in its first step, so a
cancel can arrive before the row exists. The cancel then finds the run in the
map, waits for the workflow start, and cancels the DBOS workflow. If the row
is still absent, the daemon writes it with status `cancelled` and reason
`cancel_requested`.

The daemon also keeps a map of the shell commands of each run
(`run_id -> {tool_call_id: pid}`, in `processes.py`). The event handler fills it.
It adds a command at `shell.command_started`. It drops the command at
`shell.command_finished` only when `exit_code` is set. The harness also emits
`command_finished` when the tool stops waiting for a command that still runs, as
after a timeout or in background mode. The `exit_code` is then `None`, and the
map keeps the command. The daemon drops the entry of a run when its stream ends.
A lock guards the map, because the handler can run on a workflow thread.

Streaming is therefore a presentation path with replay tolerance, not the
source of truth for history. On success the
daemon emits a final `turn_result` frame after draining live events. On
workflow failure it emits a final `turn_error` frame; the CLI reports a finding
and operators inspect the durable run record. When a client disconnects, its
subscriber and local wait tasks are cleaned up. The daemon also cancels the
DBOS workflow and records `client_disconnected` on the run.

## Cancellation

The daemon cancels a workflow with `DBOS.cancel_workflow_async`. Only the model
request and model stream steps are preemptible. DBOS then cancels the model task
and closes its upstream stream. The event, tool, and capability steps are not
preemptible. The code sets `preemptible` only in the `model` step configuration.

A `Coder` tool does not run as a DBOS step. pydantic-ai wraps only MCP and
dynamic toolsets, so a `Coder` function tool runs inline in the workflow
coroutine. DBOS cannot stop it. The daemon stops the shell process itself. The
harness records the start and the end of each tool call as DBOS steps
(`record_tool_effect`, `finish_tool_effect`). DBOS refuses to start a step in a
cancelled workflow.

The shell tool starts each command in its own session. The `pid` of the
`shell.command_started` event is the session leader, so it is also the process
group id. `os.killpg(pid, ...)` reaches every child of the command. The harness
has no public stop call.

A cancel (`cancel_run`) runs these steps in order:

1. Write the status `cancel_requested` on the run row, with the reason. The
   status is not final. If DBOS then refuses the cancel, the daemon moves the
   row back to `running`.
2. Ask DBOS to cancel the workflow. DBOS then refuses the next step.
3. Send SIGTERM to each process group that the run started and did not finish.
   Poll until each group is gone. After the grace time (2 seconds,
   `TOOL_STOP_GRACE_SECONDS`), send SIGKILL to each group that remains, and
   poll again for up to 2 seconds.
4. Mark the effect record of each stopped command `failed`, with the summary
   `stopped by cancel`. The harness cannot do it, because DBOS refuses the
   step.
5. Write `cancelled`.

The POST waits for these steps before it answers. While the daemon works,
`GET /v1/runs/<id>` returns `cancel_requested`. A client disconnect runs the
same steps, with the reason `client_disconnected`.

A group counts as gone when it has no member, or only zombie members. A zombie
has exited and stays until its parent reads its status.

The daemon signals only a process group that this run started and that has no
exit code. It checks that the group still exists before each signal. A command
that already finished is never signaled.

If a group survives SIGKILL, the run stays `cancel_requested`, the daemon logs
the error, and the POST answers `cancelled: false` with that status. A second
cancel tries again.

After the kill, the shell tool returns a normal result with `exit_code: null`.
The workflow cannot start another step, so it ends cancelled. The workflow
unwind does not write `cancelled` over `cancel_requested`. It adds only the
usage and the metadata. The daemon writes `cancelled` after the groups are gone.

The final turn frame goes out after the stop. It is a `turn_error` frame with
the key `live_tool_processes`: the process ids that still live. An empty list
tells the client that no tool process runs. A cancelled workflow that DBOS
cancelled without a daemon cancel has no stop. The list then names the tracked
groups that still exist.

A daemon restart loses the process map. A cancel after a restart cannot stop a
shell command that the old daemon started. At start, the daemon settles each
`cancel_requested` row (see `docs/persistence-and-recovery.md`).

When the cancelled workflow unwinds, the runtime records usage and metadata
directly through the product store. It cannot use another DBOS step after
cancellation.

## Package boundary

The code ships as two distributions in one uv workspace. `agentman-client`
holds `client.py`, `protocol.py`, `config.py`, `discovery.py`,
`agentman/__init__.py`, and `structured_agents`. It has no server
dependencies. `agentman` holds every other module and depends on
`agentman-client` at the same version. Both share the `agentman` package
folder. The import-linter contract "the client distribution has no server
imports" keeps the boundary. See `docs/development.md`.

## Module responsibilities

The "Distribution" column names the wheel that ships each module.

| Module | Distribution | Responsibility |
| --- | --- | --- |
| `__init__.py`, `protocol.py` | `agentman-client` | Package version and path extension; versioned socket request and response models |
| `cli.py` | `agentman` | Typer commands, config/spec resolution, output validation, rendering and exit codes |
| `config.py` | `agentman-client` | Flat global/repository config merge and provenance |
| `discovery.py` | `agentman-client` | Workspace and `.agentman` directory resolution |
| `structured_agents/spec.py` | `agentman-client` | Provider-neutral spec discovery plus `output:` and `tools:` parsing |
| `schema.py`, `_schema_runner.py` | `agentman` | Repository-interpreter schema generation and validation |
| `model.py` | `agentman` | Model and router capability resolution |
| `agent.py` | `agentman` | pydantic-ai agent and capability construction |
| `runtime.py` | `agentman` | DBOS workflow, history loading, run lifecycle, and cache |
| `daemon.py` | `agentman` | ASGI transport, lifecycle, SSE registry, Postgres wiring and routes |
| `processes.py` | `agentman` | Map of the shell commands of each run, and the SIGTERM and SIGKILL stop of their process groups |
| `client.py` | `agentman-client` | Unix-socket HTTP client, SSE parsing, fan-out collection and deduplication |
| `render.py` | `agentman` | On-demand Markdown from stored `ModelMessage` JSON |
| `status.py` | `agentman` | Product status composed with journal facts |
| `recovery.py` | `agentman` | Resume authorization; never a replay engine |
| `persistence/store.py` | `agentman` | Product-facing thread/run CRUD |
| `persistence/step_store.py` | `agentman` | Harness `StepStore` contract over Postgres |
| `persistence/inspect.py` | `agentman` | Product-facing queries over the journal |

`structured_agents` must not import `agentman`, DBOS, SQLAlchemy, or HTTP
transport code. The client modules must not import a server module or a server
package. Import-linter contracts enforce both directions.
