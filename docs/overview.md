# Overview

## What agentman is

agentman is a CLI for talking to typed agents while retaining a durable record
of the work. It is designed for exploratory and operational agent use where a
conversation is not disposable: model requests, streamed events, outputs,
snapshots, and tool effects should remain inspectable after the process that
created them exits.

The record is the product. DBOS supplies durable workflow execution and
recovery, but agentman does not treat durability as a generic excuse to replay
side effects. Tool effects are recorded as evidence and surfaced for
inspection; resume is refused when the evidence is not sufficient.

## Recording guarantee

For an accepted turn, agentman gives the turn one canonical run identity and
records the durable lifecycle across the product run store and the
`step_persistence` journal. Model and tool lifecycle events observed by
`StepPersistence`, snapshots, messages, output, and supported file-effect
evidence remain inspectable through the daemon after the client exits. The
journal, not the live SSE stream, is the source of truth.

“Recorded” deliberately does not mean:

- every physical model or tool execution happens exactly once;
- an SSE client receives every event or a dropped event can be reconstructed
  from the stream;
- an unrestricted shell command receives file pre/post evidence;
- an unknown shell or filesystem effect is automatically replayed after a crash;
- a client-side Python output type is imported by the daemon; or
- a final result exists when the workflow fails before it can produce one.

Recovery supplies facts and refusal policy. It does not turn arbitrary external
side effects into transactionally safe or generally replayable operations.

## The three nouns

| Noun | Meaning | Identity and scope |
| --- | --- | --- |
| **Agent** | A YAML pydantic-ai `AgentSpec`, plus agentman’s `output:` and `tools:` extensions | A named spec discovered from global or repository configuration |
| **Thread** | A named, resumable conversation | A generated id scoped to a resolved workspace |
| **Run** | One turn: workspace + thread + agent + model + prompt → output | The DBOS workflow id; also the pydantic-ai and journal run id |

There is intentionally no separate batch, capability, grant, receipt, or
workflow-mapping product noun. Fan-out is a client convenience that creates
ordinary runs. Capabilities are pydantic-ai/harness implementation details.

## Current implementation status

| Area | Status in this checkout | Important boundary |
| --- | --- | --- |
| Single-turn CLI (`send`, `pipe`) | Implemented and tested | Both use the same daemon turn path |
| Threads and workspace identity | Implemented and persisted | Names are labels; ids are identities |
| Typed output | Implemented and tested | Schema leaves the repository; final validation returns to its interpreter |
| Local `inferference:<model>` resolution | Implemented | The daemon resolves router aliases; typed output requires `SCHEMA`, while default string output does not |
| Other pydantic-ai model strings | Supported by delegation | Provider behavior and structured-output support remain provider-owned |
| Unix-socket daemon and SSE | Implemented | Development uses foreground `agentman serve` |
| Postgres product index and journal | Implemented | Migrations are explicit; request handling never creates tables |
| DBOS durable workflow | Implemented on DBOS 3.0 | The workflow id is the canonical run id |
| `coder` tools | Implemented as an opt-in spec extension | Shell access is unrestricted; the local socket is the access boundary |
| File-effect evidence | Implemented for `write_file` and `edit_file` | It records pre/post hashes; it is not generic exactly-once execution |
| `status` and unresolved effects | Implemented | Earlier runs in a thread are included in unresolved-effect inspection |
| `resume` | Implemented and policy-gated | It never silently replays an unresolved side effect |
| `pipe --each -j N` | Implemented | The bound is per CLI invocation; the daemon remains unscheduled and concurrent |
| Systemd fd handoff | Implemented in daemon code and tested | The daemon accepts one systemd listening descriptor |
| Machine-wide install and Postgres | Configured and built; current release not active | Installation and activation are outside this repository |

The distinction between implementation and operation matters. For example,
`serve --systemd-socket` knows how to consume one systemd listening descriptor.
The user unit and machine database are configured outside this repository.
Keep the installed package and client protocol versions aligned.

## What a complete turn does

At a high level:

1. The CLI resolves the workspace, config, agent spec, output type, model
   string, and current thread.
2. The repository interpreter generates the output JSON Schema when the agent
   is typed.
3. The client sends a self-contained request over the Unix socket. It sends
   JSON Schema and agent data, not repository Python imports.
4. The daemon resolves the model, loads thread history, starts one DBOS
   workflow, and streams pydantic-ai events back as SSE.
5. DBOS and harness persistence record the workflow and semantic journal while
   agentman records the product-facing run row.
6. The daemon returns the structured JSON result.
7. The client validates that result in the repository interpreter and prints
   either the stream/result or canonical JSON.

The history source is the harness snapshot from the thread’s most recent
settled run, with the product run’s stored `messages_json` as a fallback when a
snapshot is unavailable.

## What agentman deliberately does not claim

- A recorded tool call is not automatically exactly once.
- A crashed arbitrary shell command is not safe to replay.
- SSE is not the authoritative record; it is a live presentation of events.
- Wall-clock overlap proves agentman adds no serialization; it does not by
  itself prove that the model server decoded requests in one batch.
- A model constrained to JSON Schema can still produce semantically wrong
  values; client-side pydantic validation remains mandatory.
- Moving or renaming a repository preserves the record but changes the
  workspace identity used by convenience lookups.

## Deliberately absent capabilities

The current design does not include Markdown view files, file watching, a
Neovim plugin, a second database, SQLite as a daemon fallback, a shared model
alias registry, repo-id hashing, system boundaries, Nix agent composition,
automatic replay of unknown effects, or a generic exactly-once tool wrapper.
These are not hidden bugs; they are scope decisions. Their review triggers are
listed in the [roadmap](roadmap.md).
