# ADR 0002: Launch and track external coding-agent jobs

Date: 2026-10-20  
Status: Proposed for requirements review

## Context

Agentman runs typed pydantic-ai agents in DBOS workflows. Its harness records
the work those agents perform. The optional `Coder` toolset provides an
unrestricted shell. That shell can start Claude Code, Codex, Pi, or Zellij.
Today, agentman records the launch command. It has no job identity, status,
result, or safe recovery path for work that continues in the child process.
[Architecture](../architecture.md),
[persistence and recovery](../persistence-and-recovery.md).

The user wants to start a coding-agent request from agentman, let it continue
after the current terminal disconnects, and inspect its status and result.
A Claude Code session inside detached Zellij is one possible presentation.
The first required flow is a detached task with a status and final result.

Claude Code supports programmatic requests, structured output, and session
resume. Codex supports non-interactive execution, structured events, and
session resume. Pi supports a long-running JSON Lines remote procedure call
(RPC) process and named session selection. These interfaces offer a path to
control each host without interpreting terminal text.
[Claude Code programmatic use](https://code.claude.com/docs/en/headless),
[Codex non-interactive mode](https://learn.chatgpt.com/docs/non-interactive-mode),
[Pi RPC](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/rpc.md).

[ADR 0001](0001-system-wide-ai-activity-record.md) rejected a general
cross-tool ingestion service. The evidence report
found no complete cross-tool audit or exact Atuin-to-host tool-call link.
This proposal concerns only jobs that agentman starts. It does not reopen the
general audit design. Deployment checks remain separate.

## Confirmed requirements

The user confirmed these requirements on 2026-10-20:

1. The first experience is a detached task with status and result.
2. Both a user command and an agentman agent tool may start a job.
3. The first version supports Claude Code, Codex, and Pi.
4. A job that edits files works in an isolated workspace.
5. Unattended tool requests use a configured allowlist. A request outside
   that allowlist must stop or wait for approval.
6. The first version starts new host sessions. Resuming an existing host
   session is later work.
7. The user reviews and integrates changes from each isolated workspace.
8. Agentman retains the full structured event stream for each job it starts.
9. A request outside the allowlist waits for approval, then stops after a
   timeout.
10. An attachable Zellij session is optional later work.
11. Agentman stores a redacted copy of every received structured event. It
    links to the host's native raw record instead of copying raw content.
12. The caller sets a spending limit for each job. The limit is advisory.
    Agentman may start a job when the host cannot enforce the limit.
13. The default approval-wait timeout is 10 minutes. The caller can set a
    finite, positive timeout for a job.
14. Agentman retains job records and redacted events without automatic expiry.
    The host controls retention of its native raw record. The link can expire.
15. The job owner may read and export redacted events. A project administrator
    needs an explicit, recorded grant. An agent tool may read only its own
    authorized job. Only the owner may see the native raw link.

All three hosts must satisfy the first-version contract. Implementations and
probes may proceed one host at a time. A Claude-only release would not meet
the confirmed host scope.

## Proposed product boundary

Agentman would accept a user command or an agent tool call to start an
external coding-agent job. It would return a stable agentman job ID without
waiting for the host task to finish. The caller could inspect status and read
the final result. A job would record the parent agentman run ID when an
agent tool requested the launch.

The external host would own its agent loop, model calls, tool permissions,
native session, and transcript. Agentman would own the launch request, job
identity, process supervision, and status view. The host session ID would
remain distinct from the agentman job ID and any DBOS run ID.

The first version would use one job contract with adapters for Claude Code,
Codex, and Pi. Each adapter must report the same required job states, final
result, and structured events. Each job starts a new native host session.
An attachable Zellij terminal can follow in a later version.

The job record would keep control metadata: host, workspace, request time,
launch state, native session ID when known, process handle, exit status,
result location, and a parent run ID when present. It would also persist the
host-emitted structured event stream and link to the native transcript.
The stream records what the host emitted and agentman received. It cannot
prove that the host emitted every action. Missing sequence, dropped output,
redaction, and truncation must appear as coverage gaps. Agentman would keep
each received event with stable source IDs, order, status, and a redacted
payload. It would label changed or removed fields. The native host keeps the
raw transcript. A "full stream" means all received event records, not exact
raw payload bytes or proof of all host actions. Agentman keeps these records
without automatic expiry. External events must not enter the harness journal
as agentman-executed tool effects.

### Candidate user flow

1. A user command or agent tool selects a host and request for a new session.
   It supplies a per-job spending limit and may override the project's
   approval-wait timeout. Agentman creates an isolated editing workspace.
2. Agentman creates a job ID and records launch intent before starting the
   host process.
3. The adapter starts the host through a supported interface. It records the
   native session ID and a way to locate the process or terminal.
4. Agentman returns the job ID after it can find the launched job again.
5. The caller asks for status, structured events, result, or cancellation by
   job ID.
6. Agentman reports the host result and exit status when both are available.
7. The user reviews and integrates any workspace changes. Agentman does not
   merge or push them as part of job completion.

This flow is a proposal. It does not assert that the current daemon can keep
a detached child alive across a service restart. A probe must establish that
property before an accepted job receives a durability claim.

## Identity and failure rules

| Condition | Proposed treatment |
| --- | --- |
| Host accepted a request | Keep the host session ID and its native record reference. |
| Process exited | Record its exit status. Check the host result before calling the job complete. |
| Daemon restarted | Reconcile the known process or terminal and native session before changing job status. |
| Launch outcome is uncertain | Mark the job uncertain. Do not start the request again without proof that the first launch failed. |
| Host asks for approval | Apply the configured allowlist. Wait for a request outside it, then stop after 10 minutes or the job override. Record both states. |
| Spending limit is reached | Request host cancellation when reported spend reaches the limit. Record the request, observed exit, and final spend when available. Show unknown spend or possible overrun when the host cannot report or stop promptly. |
| Caller cancels a job | Ask the host to stop, then record the observed process and host states. |
| Host transcript is missing | Show the record gap. Do not infer that no tool action occurred. |

An agentman DBOS workflow may record an agent tool's request to start a job.
The user command uses the same job contract. A DBOS run ID does not make the
external host's actions replayable. The harness `StepPersistence` journal
must not represent those actions as agentman tool effects. An external job
needs its own status and recovery rules.

## Options considered

| Option | Benefit | Limit |
| --- | --- | --- |
| Use `Coder` shell alone | Works for manual launch now. | No managed job identity, status, cancellation, or restart reconciliation. |
| Add host adapters, a job record, and structured events | Supports a named handoff, status, result, and emitted-event review. | Requires process, stream, storage, and version checks for all three hosts. |
| Make Zellij the job record | Provides a terminal that a user can attach to. | A terminal session alone does not prove host completion or preserve host IDs. |
| Build the ADR 0001 ingestion service | Could collect wider reported activity. | Does not solve launch ownership or prove complete capture. |

## Privacy and operating cost

A launch request may contain private source or secrets. A structured event
stream may repeat them. The first version would persist a redacted copy of
the events agentman receives. It would link to the host's native raw record.
Redaction must happen before agentman persists, indexes, or logs event bodies.
Agentman keeps the redacted copy without automatic expiry. Native raw records
may disappear under host retention rules. Perpetual retention needs storage
planning, backup handling, and an explicit deletion path before
implementation. Redaction can remove audit and mining detail, and unknown
secret patterns can survive it. A missed secret would remain until deletion.
The synthetic comparison
showed that a live deletion left raw test secrets in a backup.

### Access rule

The job owner is the user who starts the command or authorizes the parent
agentman run. Give that owner read and export access to the job's redacted
events. The owner may give a project administrator an explicit, recorded grant.
An agent tool may read status, result, and redacted events only for jobs that its
authorized parent run started. Do not give an agent tool general export access.
Show the native raw link only to the job owner. Opening it must still pass the
host's native file or session permissions. Agentman must not proxy raw content
or elevate access to it. Record who reads or exports redacted events and who
requests the raw link. This rule follows least-privilege guidance for log data
in the [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html).
The user confirmed this access rule on 2026-10-20.

Each adapter must track host interface versions, authentication, permission
behavior, process exit, session location, and workspace access. The daemon
user must have the required host credentials and file permissions. An
editing job needs an isolated workspace and an integration path for its
changes. The three-host scope adds three compatibility and permission gates.
Perpetual records make storage growth and backup recovery ongoing costs.
The stream adds backpressure, export, and schema-change costs. An
isolated workspace remains available until the user reviews its changes.
These costs belong to the job feature.

## First evidence gate

Settle the remaining requirements below. Then test each host in an isolated
workspace with a synthetic request. A controlled model call would require a
separate cost cap and explicit authorization.

For each host, an end-to-end test must show one accepted job ID, one native
session ID, a detached run, status during work, a final result, and the full
received structured event stream with redacted payloads. It must test daemon
restart, duplicate launch prevention, cancellation, a failed event write, and
a tool request outside the allowlist. The adapter must prove that it waits
and stops after 10 minutes or the job override. Test a caller-set spending
limit for each host. Record whether the host reports spend, accepts a stop
request, and confirms its final spend. Mark unsupported or untested parts.
An unenforceable spending limit does not block launch. Compare its stream with
the host's native record. Mark every omission, redaction, and truncation.
The first version needs this evidence for Claude Code, Codex, and Pi.

## Open requirements

1. Which spending unit does the caller enter? How does agentman report spend
   when a host reports only tokens or no cost?
2. What should the adapter do if a host cannot pause an approval request?
3. Which fields must the redactor remove before persistence?
4. How do explicit deletion, backup handling, and completed workspace cleanup
   work with perpetual job records and redacted events?

This ADR remains proposed until the user settles these requirements and a
bounded host probe supports the selected flow. It authorizes no process
launcher, host adapter, or transcript ingestion.
