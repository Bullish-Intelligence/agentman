"""The request and response types the socket carries.

Pydantic models, versioned from the start. A run request is self-contained
except for history: the client names the workspace, the thread, the resolved
agent spec, the model string, the output schema, and the prompt. The daemon
never reads the client's cwd and never reads a repository's config.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 3


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TurnRequest(_Model):
    """One turn. History is loaded by the daemon from ``thread_id``."""

    workspace: str
    thread_id: str
    agent_spec: dict[str, Any] = Field(default_factory=dict)
    tools: str | None = None
    agent_name: str | None = None
    model: str
    prompt: str
    output_schema: dict[str, Any] | None = None
    output_type: str | None = None
    max_transport_retries: int | None = Field(default=None, ge=0)


class RunMetadata(_Model):
    """Non-secret retry facts for one run."""

    requested_max_transport_retries: int | None = None
    effective_max_transport_retries: int | None = None
    provider_fallback: bool = False
    logical_turn_count: int = 1
    physical_model_request_count: int | None = None


class TurnResponse(_Model):
    """The recorded outcome of one turn."""

    run_id: str
    thread_id: str
    output: Any = None
    output_type: str | None = None
    messages: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, Any] | None = None
    run_metadata: RunMetadata = Field(default_factory=RunMetadata)


class StreamEnvelope(_Model):
    """One SSE frame.

    DBOS may re-execute a step after a crash, so events can repeat. Subscribers
    deduplicate on ``(run_id, request_ordinal, event_ordinal)``. The daemon
    stamps both ordinals: the request ordinal from ``ctx.run_step``, the event
    ordinal from its own counter, reset per request.
    """

    protocol: int = PROTOCOL_VERSION
    run_id: str
    request_ordinal: int
    event_ordinal: int
    kind: str
    event: dict[str, Any] = Field(default_factory=dict)
    final: bool = False


class NewThreadRequest(_Model):
    workspace: str
    name: str | None = None


class ThreadRef(_Model):
    workspace: str
    name: str


class ThreadInfo(_Model):
    id: str
    workspace: str
    name: str
    created_at: datetime
    updated_at: datetime
    short_id: str
    runs: int = 0


class ThreadList(_Model):
    threads: list[ThreadInfo] = Field(default_factory=list)


class RunInfo(_Model):
    id: str
    thread_id: str
    workspace: str
    agent: str
    model: str
    status: str
    prompt: str
    output_json: Any = None
    output_type: str | None = None
    run_metadata: RunMetadata = Field(default_factory=RunMetadata)
    created_at: datetime
    updated_at: datetime


class ThreadView(_Model):
    thread: ThreadInfo
    runs: list[RunInfo] = Field(default_factory=list)
    messages: list[dict[str, Any]] = Field(default_factory=list)


class StatusRequest(_Model):
    workspace: str
    thread_id: str | None = None
    name: str | None = None


class ResumeRequest(_Model):
    workspace: str
    run_id: str


class ResumeResponse(_Model):
    run_id: str


class UnresolvedEffect(_Model):
    run_id: str
    tool_call_id: str
    tool_name: str
    status: str
    started_at: datetime
    note: str = "unknown_after_crash"


class StatusView(_Model):
    thread: ThreadInfo | None = None
    latest_run: RunInfo | None = None
    unresolved_effects: list[UnresolvedEffect] = Field(default_factory=list)
    events: int = 0
    snapshots: int = 0
    message_rows: int = 0
    message_bytes: int = 0


class ErrorResponse(_Model):
    error: str
    detail: str | None = None


__all__ = [
    "PROTOCOL_VERSION",
    "ErrorResponse",
    "NewThreadRequest",
    "RunInfo",
    "RunMetadata",
    "ResumeRequest",
    "ResumeResponse",
    "StatusRequest",
    "StatusView",
    "StreamEnvelope",
    "ThreadInfo",
    "ThreadList",
    "ThreadRef",
    "ThreadView",
    "TurnRequest",
    "TurnResponse",
    "UnresolvedEffect",
]
