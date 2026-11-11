# CLI and configuration reference

## Command summary

All commands run against the workspace resolved from the current directory.
Inside a Git repository, the workspace is the resolved repository root; outside
one, it is the resolved current directory.

| Command | Purpose |
| --- | --- |
| `agentman serve` | Run the daemon in the foreground |
| `agentman serve --systemd-socket` | Run from one systemd-passed listening descriptor |
| `agentman migrate` | Apply packaged Alembic migrations |
| `agentman version` | Print package and socket-protocol versions |
| `agentman new [NAME]` | Create a thread, make it current, and switch to it |
| `agentman use NAME` | Select the most recently updated named thread |
| `agentman ls` | List threads in the current workspace |
| `agentman show [THREAD]` | Render a thread as Markdown; omitted means current |
| `agentman status` | Show current thread/run state and unresolved effects |
| `agentman resume RUN_ID [--yes]` | Inspect, confirm, and resume a DBOS workflow |
| `agentman cancel [RUN_ID] [--thread NAME] [--all-running]` | Cancel one or more running turns |
| `agentman config` | Print resolved values and their sources |
| `agentman send MESSAGE` | Run one turn |
| `agentman pipe [PREAMBLE]` | Read one turn from stdin |
| `agentman pipe --each [PREAMBLE]` | Run one independent turn per stdin line |

`send` and `pipe` accept `--agent NAME`, `--model MODEL`,
`--max-transport-retries N`, and `--json`.
`--json` suppresses live text deltas and prints the validated final result.
`--max-transport-retries` sets the transport retry limit for one run. Zero
allows one initial request and no transport retry. The option applies only to
`inferference:` models. Omit it to keep the provider default.

## Exact command behavior

| Command | Arguments and options | Behavior |
| --- | --- | --- |
| `serve` | `[--systemd-socket]` | Starts the daemon in the foreground. The systemd form requires exactly one passed listening descriptor. |
| `migrate` | none | Applies packaged migrations; the daemon does not migrate during request handling. |
| `version` | none | Prints the installed package version and socket-protocol version. |
| `new` | `[NAME]` | Creates a thread, writes its id to the local current-thread file, and prints its short id and name. |
| `use` | `NAME` | Selects the most recently updated thread with that name and writes its id locally. |
| `ls` | none | Lists all threads in the resolved workspace. |
| `show` | `[THREAD]` | With no argument, renders the current thread. With an argument, resolves it as a thread name, not an arbitrary id. |
| `status` | none | Reads the local current-thread pointer and prints status. With no pointer it prints `no current thread` and exits successfully. |
| `resume` | `RUN_ID [--yes]` | Fetches the run, verifies its workspace, prints status, optionally confirms, and delegates resumption to DBOS. |
| `cancel` | `[RUN_ID] [--thread NAME] [--all-running]` | Requires exactly one selector. A run id selects one run. `--thread` selects running runs in the named thread. `--all-running` selects running runs in every workspace thread. A selector can pick several runs. The command tries every run, even after an error, and prints each error on stderr. The exit code is `2` if any run failed with an error, else `1` if any run was already final, else `0`. |
| `config` | none | Prints resolved values and provenance, including inferference's resolved router URL when applicable. |
| `send` | `MESSAGE [--agent NAME] [--model MODEL] [--max-transport-retries N] [--json]` | Runs one turn. If no current thread exists, it creates an unnamed one. |
| `pipe` | `[PREAMBLE] [--agent NAME] [--model MODEL] [--max-transport-retries N] [--json] [--each]` | Reads all stdin. Without `--each`, stdin is one turn; with it, each physical line is an independent ordinary run. |

For the pinned OpenAI software development kit (SDK), the omitted local-provider
default is two retries. The SDK allows at most `N + 1` physical attempts for
one logical model request. The SDK retries only eligible failures. Agent-level
output and tool retries remain separate. Each new logical request gets its own
transport retry limit. Agentman does not fall back to another provider or
model.

An explicit retry limit with another provider returns an error before the
daemon starts a run. Run records expose the requested and effective limits,
the fallback state, the logical turn count, and the physical request count.
The physical count is null when the selected provider does not expose an
instrumented transport path.

`pipe` accepts the optional-value forms `-j`, `-jN`, `--jobs`,
`--jobs=N`, and `--jobs N`. The option may appear once and requires
`--each`. A positive value sets the client-side in-flight bound; no value
discovers inferference's `n_parallel` value. A non-inferference model must use
an explicit value.

For `pipe --each`, a trailing newline does not create an extra input, blank
physical lines are inputs, results are emitted in input order, and a failed
input is reported by its one-based input index. Text output is prefixed with
`[N]`; JSON output emits one validated JSON value per successful input.

## CLI error table

| Situation | Result |
| --- | --- |
| Unknown command, unknown option, multiple preambles, repeated `-j`, or a non-positive job count | Error on stderr, exit code `3` |
| `-j`/ `--jobs` without `--each` | Error on stderr, exit code `3` |
| `cancel` has no selector or more than one selector | Error on stderr, exit code `3` |
| Missing socket, invalid config/spec, schema-import failure, unavailable database/router, or daemon HTTP error | Error on stderr, exit code `2` |
| The connection to the daemon fails during a call (for example a daemon restart during a turn stream) | Error on stderr, exit code `2` |
| The daemon sends a response this client cannot read (a protocol skew) | Error on stderr that says to upgrade the client and daemon together, exit code `2` |
| Daemon health has no `cancel` capability | Error on stderr, exit code `2` |
| `pipe --each` cannot discover parallelism for a non-inferference model without an explicit job count | Error on stderr, exit code `2` |
| An explicit transport retry limit uses a non-inferference model | Error before the run starts, exit code `2` |
| Final output fails client-side validation, the daemon reports `turn_error`, or the stream ends without a result | Error on stderr, exit code `1` |
| Fan-out has one or more failed inputs | Successful outputs remain ordered and visible; failures name their input indexes; exit code is `1`, unless a configuration failure makes it `2` |
| `status` has no current thread | Prints `no current thread`, exit code `0` |
| `cancel --thread` or `cancel --all-running` finds no running run | Prints `no running run`, exit code `0` |
| `cancel RUN_ID` finds a terminal run | Prints `not cancelled <run-id>: already <status>`, exit code `1` |
| `cancel --thread` or `cancel --all-running` cannot cancel one run because of an error | Prints the error on stderr, tries the remaining runs, exit code `2` |
| `cancel --thread` or `cancel --all-running` finds a terminal run and no error | Prints `not cancelled <run-id>: already <status>` for it, exit code `1` |

## `pipe --each`

Examples:

```sh
printf '%s\n' alpha beta | agentman pipe --each
printf '%s\n' alpha beta | agentman pipe --each -j 2
printf '%s\n' alpha beta | agentman pipe --each -j
printf '%s\n' alpha beta | agentman pipe --each --json "Classify this"
```

Each physical stdin line is one prompt. A trailing newline does not create an
extra record. A preamble is prepended to each line. Blank lines are records
when they are present as physical lines.

- `-j N` / `--jobs N` sets a positive client-side concurrency bound.
- `-j` / `--jobs` without a value discovers `n_parallel` for an inferference
  model.
- A non-inferference model requires an explicit job count when the bound cannot
  be discovered.
- Results print in input order, with input indexes for text output.
- `--json` emits one JSON result per successful input in input order.
- A failed input is reported with its input index; successful submitted work is
  not hidden by a later failure.

Fan-out creates ordinary runs in one thread. It does not create a durable batch
object and does not change daemon scheduling.

## Configuration files

The configuration filename is `agentman.yaml` in two possible directories:

```text
$XDG_CONFIG_HOME/agentman/agentman.yaml   global
~/.config/agentman/agentman.yaml          global fallback
<workspace>/.agentman/agentman.yaml       repository
```

The schema is flat. Precedence is:

```text
CLI flag → repository config → global config → built-in default
```

| Key | Allowed scope | Meaning | Default |
| --- | --- | --- | --- |
| `agent` | global and repository | Default agent spec name | `default` |
| `model` | global and repository | Default pydantic-ai model string | unset |
| `database_url` | global only | Daemon and migration database URL | unset, except `AGENTMAN_DATABASE_URL` |
| `socket` | global only | Unix socket path | `$XDG_RUNTIME_DIR/agentman.sock` or `/run/user/<uid>/agentman.sock` |
| `router_url` | global only | Optional inferference endpoint override | inferference placement resolution |

The repository file may contain only `agent` and `model`. Putting a global-only
key in it is a configuration error. `agentman config` prints source provenance;
when `router_url` is unset it also displays the URL inferference resolves.

## Agent specifications

Agent files are `.yaml` or `.yml` files in `.agentman/` or the global agentman
directory. The filename stem is the fallback agent name. Repository specs
shadow global specs with the same name.

The underlying mapping is a pydantic-ai `AgentSpec`. agentman removes these
two extensions before passing the mapping to pydantic-ai:

```yaml
name: reviewer
instructions: Review carefully.
output: models:Review
tools: coder
```

`output:` accepts either the literal `str` or a `module:Attribute` reference.
The module is resolved relative to the spec directory in the repository’s
interpreter. `tools:` currently accepts only `coder`; omitting it means no
tools.

Every agent has an output type. If `output:` is absent, the type is `str`.

## Thread and current-state files

The current thread pointer is stored locally at:

```text
$XDG_STATE_HOME/agentman/current.json
~/.local/state/agentman/current.json   fallback
```

The file maps workspace paths to thread ids. It is a convenience pointer, not
the durable thread identity; the authoritative thread and run records are in
Postgres.

## Exit codes

The CLI contract is stable and should be treated as an API:

| Code | Meaning | Examples |
| ---: | --- | --- |
| `0` | Success | Turn completed and output validated |
| `1` | Finding | Output failed client validation; stream ended without a result |
| `2` | Infrastructure/configuration | Socket absent, database/router unavailable, unreadable config |
| `3` | Usage | Unknown command, malformed flag, invalid fan-out job count |

Callers should not blindly retry code `1`: the infrastructure may have worked
and the model result may simply not satisfy the requested type.

## Unix-socket HTTP API

The client uses HTTP over the configured Unix socket. The protocol version is
`3`. `agentman version` prints both the package version and protocol version;
`GET /v1/health` reports the same values. The daemon currently exposes:

Protocol version 3 changes the request, response, and run record shapes.
Protocol 2 clients and daemons are incompatible because the models reject extra
fields. Upgrade the client and daemon together.

| Method and path | Request | Response |
| --- | --- | --- |
| `GET /v1/health` | none | status, package version, protocol version, and capabilities |
| `POST /v1/turns` | `TurnRequest` JSON | SSE `StreamEnvelope` frames and one final `turn_result` or `turn_error` frame |
| `POST /v1/threads` | workspace and optional name | `ThreadInfo` |
| `GET /v1/threads?workspace=...` | query parameter | `ThreadList` |
| `POST /v1/threads/resolve` | workspace and name | `ThreadInfo` |
| `GET /v1/threads/<id>` | path id | `ThreadView` |
| `GET /v1/runs/<id>` | path id | `RunInfo` |
| `POST /v1/status` | workspace plus thread id or name | `StatusView` |
| `POST /v1/resume` | workspace and run id | `ResumeResponse` |
| `POST /v1/runs/<id>/cancel` | workspace | `CancelResponse` |

The cancel route answers `404` for an unknown run. It answers `409` for a run
in another workspace, and for a run that DBOS could not cancel.

`RunInfo` carries the run status (`running`, `completed`, `failed`, or
`cancelled`), the usage known so far, and `error`. `error` is null unless the
run ended without a result. Then it names the `reason` (`exception`,
`cancel_requested`, `client_disconnected`, or `workflow_cancelled`) and the
exception `type` and `message` when one exists. These two fields are additive
within protocol 3. An older agentman client rejects them, so upgrade the client
and daemon together.

Turn history is not sent as a `messages` field. The daemon loads it by thread
id from the journal. `TurnRequest.max_transport_retries` is optional and must
be non-negative when present. SSE frames carry the protocol version, run id,
request and event ordinals, event kind, event payload, and a final-result flag. Clients must
deduplicate replayed frames by the stamped ordinal tuple.

### SSE delivery and failure semantics

SSE is a live presentation stream, not the durable record. The wire format is:

```text
event: <envelope.kind>
data: <JSON StreamEnvelope>

```

The daemon keeps subscriber queues only for live connections. The current
implementation starts the DBOS workflow before registering the request's
subscriber, so events emitted in that short window can be dropped. It does not
replay missed events from Postgres into a later subscriber. The journal and
stored snapshots are authoritative.

DBOS replay can cause an event to be observed more than once. The request and
event ordinal tuple makes those duplicates removable by the client. This is
different from dropped events: deduplication removes repeated delivery, but it
cannot reconstruct a frame that was emitted before subscription or after the
subscriber disappeared.

On success, the daemon drains live event frames and then emits one final
`turn_result` envelope. If the workflow raises, it emits a final `turn_error`
frame with a generic message. The CLI reports that as a finding (exit code
`1`); callers should inspect `status` and the durable run record rather than
treating a failed turn as proof that no work occurred. A cancellation also ends
with `turn_error`. Its event contains `error: "turn cancelled"`, the cancel
reason, and the run id. A connected client receives that final frame. The
daemon records the cancellation reason on the run and closes the model stream.
A disconnected client receives no more frames. The daemon cancels its workflow
and removes the subscriber and local wait tasks. A client that closes the
stream, for any reason, therefore cancels the turn. The reason is
`client_disconnected`.

The client sets no read timeout on a turn stream. A turn can send no data for a
long time, for example during a long tool call, and a read timeout would close
the stream and cancel the turn. Other calls keep the 300 second timeout. The
CLI exits with code `2` when the connection to the daemon fails during a
stream.

## Important errors

- A missing socket means the foreground daemon is not running in development.
- A repository config containing `database_url`, `socket`, or `router_url` is
  invalid by design.
- A missing `output` type module or invalid result is a client-side finding or
  configuration error, not a daemon-side import problem.
- Inferference aliases are normalized by the shared capability resolver.
  Unknown models return HTTP `422`. Router/capability failures return HTTP
  `503`. `SCHEMA` is required only when the request includes an output schema;
  the default `str` output does not require it.
- Resume refusals are HTTP `409` responses and CLI exit code `2`. They cover
  unknown runs, terminal runs, missing workspaces, unreconciled effects, and
  requests whose workspace does not match the stored run. Both CLI and daemon
  workspace mismatch messages avoid disclosing the stored path.
