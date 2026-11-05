# ADR 0001: Record AI activity across local agents

Date: 2026-10-03  
Status: Rejected after concept review on 2026-10-20

## Review outcome

The concept review rejects this candidate design. The user needs live command search and an exact action audit. Start the audit with the four local AI tools and shell commands. Assess raw and redacted content before a retention decision. Atuin already supplies command search. The proposed ingestion path cannot prove a full audit when hosts omit events. Test source evidence before proposing another store. Agentman remains focused on its typed-agent runtime. This outcome does not change deployment acceptance.

The text below preserves the rejected proposal for context. It does not authorize implementation.

## Context

agentman runs typed agents and records their turns in Postgres. Its daemon owns
the DBOS workflow, pydantic-ai agent, semantic journal, and recovery policy.
Pi, Claude Code, and Codex run their own agent loops. Their model and tool
activity does not pass through agentman's current `/v1/turns` endpoint.

The user wants one place to inspect AI work across these tools. Atuin already
records shell commands on this machine. Its agent hooks cover Pi, Claude Code,
and Codex. Atuin records command text, working directory, time, exit code, and
duration. Its agent hooks cover Bash calls, not prompts, model calls, file
tools, web tools, or final answers.

The operating model covers contract checks, portable dependencies, machine
installation, systemd socket activation, machine Postgres, and release evidence.
This proposal does not claim that any external-agent ingestion exists today.

## Candidate design

The following design is a proposal. The concept review may change or reject
it. No external-agent ingestion work is approved by this record.

agentman will provide a durable, system-wide activity record. Its current
typed-agent runtime will be one producer of that record. External agents will
remain responsible for their own execution and native session state.

The activity record will use the existing Postgres database and daemon-owned
write path. It will add a versioned ingestion contract for external sessions,
turns, and events. Each accepted event will retain its source, source event ID,
source session and turn IDs when available, workspace, event time, ingest time,
event kind, and original payload. A small common event vocabulary will support
cross-tool queries. Source-specific fields will remain available without
forcing unlike events into one shape. Ingestion will deduplicate retries by
source identity and event ID. An adapter must assign a stable ID when its host
does not provide one. Unified reads will use the existing product and journal
stores for native runs and the new ingest store for external events. They will
not copy native journal rows into a second event store.

Imported events will not be inserted into DBOS-owned tables or the harness
`step_persistence` journal as if agentman had executed them. A native run keeps
its existing canonical DBOS workflow ID and recovery contract. An external
turn carries the source's identity and an explicit `observed` provenance.
Agentman will not offer DBOS resume for external turns.

Atuin will remain the owner of command history. Agentman will not install a
second Bash recorder or copy every Atuin command into its database. A combined
view will query Atuin through a supported read-only interface and show its
command records beside agent activity. The first version may correlate by
source, workspace, and time. It must label that link as inferred. An exact
per-turn link requires an adapter to retain both the Atuin history ID and the
agent tool-call or turn ID. That link is separate follow-up work.

Agent adapters will use each host's supported event surface:

- Pi: a global extension for messages, provider activity, and tool events.
- Claude Code: lifecycle hooks and OpenTelemetry export.
- Codex: supported local hooks and OpenTelemetry export.

Hooks and telemetry may report the same action. Adapters will preserve source
IDs and deduplicate where the host provides stable correlation. Each view will
state its coverage and gaps. It will not claim that all activity was captured
when a host omitted, redacted, or failed to deliver an event.

The ingest design must handle a stopped daemon without discarding events the
adapters accepted. A local spool or equivalent retry path will hold unsent
events. The daemon will acknowledge only durable writes. Before collecting prompt bodies,
tool inputs, or responses, the implementation must define content controls,
redaction, access, and retention. The local socket permission boundary remains
`0600`.

## Consequences

- Users can inspect native and external AI activity through one agentman
  interface while keeping each tool's execution model.
- Atuin keeps its command search and history. Agentman adds the conversation
  and model context that Atuin does not collect.
- External records have observation guarantees, not agentman's native DBOS
  execution and recovery guarantees.
- Source adapters must track host API changes. The record keeps original
  payloads so a common view does not discard available evidence.
- OpenTelemetry is an input format, not the canonical product model. Its
  exporters can omit or redact content, and its transport does not promise
  end-to-end delivery across collectors.

## Possible delivery order and acceptance

1. Complete the machine operating model. Record its acceptance evidence before
   starting external-agent ingestion.
2. Define and test the external event contract, durable acknowledgment,
   idempotency, provenance, and coverage labels.
3. Add one Pi session adapter and show a session with its Atuin commands.
   Label time-based command matches as inferred.
4. Add Claude Code and Codex adapters. Test each host's actual emitted events,
   redaction settings, duplicate delivery, daemon outage, and partial sessions.
5. Add exact Atuin command links only when the host and Atuin provide stable
   IDs through supported interfaces.

The work does not require agentman to become an Anthropic or OpenAI API proxy.
A gateway remains a separate decision if enforced model routing becomes a
requirement. This proposal does not authorize automatic replay of external
tools, a second database, or a claim of complete capture from unsupported
hosts or hosted tool paths.

## References

- [Current agentman architecture](../architecture.md)
- [Atuin agent hooks](https://docs.atuin.sh/main/guide/agent-hooks/)
- [Atuin read-only history interface](https://docs.atuin.sh/main/ai/mcp/)
- [Pi extensions](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/extensions.md)
- [Claude Code hooks](https://code.claude.com/docs/en/hooks)
- [Claude Code monitoring](https://code.claude.com/docs/en/monitoring-usage)
- [Codex hooks](https://learn.chatgpt.com/docs/hooks)
- [Codex telemetry](https://learn.chatgpt.com/docs/config-file/config-advanced)
- [OpenTelemetry Protocol delivery limits](https://opentelemetry.io/docs/specs/otlp/)
