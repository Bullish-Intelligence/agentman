# agentman documentation

agentman is a durable CLI and daemon for running typed agents. The central
product promise is simple:

> Every model call and tool call is recorded, always.

This documentation set has two jobs. It explains how the current code works,
and it makes the remaining product and operations gaps visible. It should not
quietly turn a design note into a claim that the feature is installed,
deployed, or proven in production.

## Read this first

Start with [the overview](overview.md). It defines the three user-facing
nouns—agent, thread, and run—and gives the implementation status matrix.

Then choose a path:

- **I want to use it:** [Getting started](getting-started.md), then the
  [CLI reference](reference.md).
- **I need to understand the system:** [Architecture](architecture.md), then
  [Persistence and recovery](persistence-and-recovery.md).
- **I need to work on the repository:** [Development guide](development.md),
  then the architecture and persistence docs for the invariant being changed.
- **I want to know what is missing:** [Roadmap and gaps](roadmap.md).
- **I want the proposed cross-tool design:**
  [ADR 0001: Record AI activity across local agents](adr/0001-system-wide-ai-activity-record.md).

## What each document owns

| Document | Purpose | Primary audience |
| --- | --- | --- |
| [Overview](overview.md) | Product definition, scope, and current status | Everyone |
| [Getting started](getting-started.md) | Local development installation and first run | Operators and users |
| [Architecture](architecture.md) | Components, boundaries, request flow, and concurrency | Contributors and reviewers |
| [Persistence and recovery](persistence-and-recovery.md) | Database model, journaling, crash windows, and resume policy | Contributors and operators |
| [Reference](reference.md) | Commands, configuration, agent specs, exit codes, and HTTP-over-UDS endpoints | Users and integrators |
| [Development](development.md) | Repository workflow, tests, contracts, and release preparation | Contributors |
| [Roadmap and gaps](roadmap.md) | Implemented, pending, deferred, and deliberately absent work | Maintainers and planners |
| [ADR 0001](adr/0001-system-wide-ai-activity-record.md) | Proposed activity record and Atuin boundary | Maintainers and integrators |

## Status vocabulary

The docs use these terms deliberately:

- **Implemented** means the behavior exists in this checkout and is covered by
  the relevant unit or integration tests.
- **Code-supported** means the repository contains the implementation, but the
  surrounding machine or deployment setup is not part of this repository or is
  not active in the current development environment.
- **Verified** means there is a named test or verification command for the
  behavior. It does not mean a production deployment has exercised it.
- **Pending** means it is required for the intended operating model but has not
  been completed here.
- **Deferred** means the design intentionally leaves it out until real use
  creates a reason to add it.

## Source of truth

The implementation is under `src/agentman/` and `src/structured_agents/`.
Tests under `tests/` are executable behavior evidence. The documents above
explain the design and the current operational picture.

When a document and the current code disagree, document the difference in
[Roadmap and gaps](roadmap.md), then update the appropriate source of truth.
