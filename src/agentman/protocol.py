"""The request and response types the socket carries.

Pydantic models, versioned from the start. A run request is self-contained
except for history: the client names the workspace, the thread, the resolved
agent spec, the model string, the output schema, and the prompt. The daemon
never reads the client's cwd and never reads a repository's config.

Compatibility rule:

- An additive change gets no version bump. Examples: a new optional request
  field, a new response field, a new event kind, a new non-final frame, a new
  status value, a new endpoint. The daemon lists each additive request feature
  in the health ``capabilities`` (``CAPABILITIES``). A client sends a new
  optional request field only when the daemon lists its capability.
- A breaking change bumps ``PROTOCOL_VERSION``. Examples: a removed or renamed
  field, a changed meaning, a new required field. The client rejects a daemon
  with another version.
- Clients ignore unknown response fields (``ResponseModel``). The daemon
  rejects unknown request fields with 422 (``RequestModel``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 4

# The features the daemon lists in the health response. Append a name here
# when a change adds an optional request feature without a version bump.
CAPABILITIES: tuple[str, ...] = ("cancel", "client-run-id")

# The pattern of a client run id. The run table column holds 64 characters.
RUN_ID_PATTERN = r"^[A-Za-z0-9._-]{1,64}$"


class RequestModel(BaseModel):
    """Base for a body the daemon receives. It rejects unknown fields."""

    model_config = ConfigDict(extra="forbid")


class ResponseModel(BaseModel):
    """Base for a payload the client parses. It ignores unknown fields.

    A newer daemon can add a field without a version bump.
    """

    model_config = ConfigDict(extra="ignore")


class TurnRequest(RequestModel):
    """One turn. History is loaded by the daemon from ``thread_id``.

    ``run_id`` is optional. The daemon uses it as the run id, which is the
    DBOS workflow id. The daemon answers 409 when a run or a live stream
    already uses the id. The daemon lists the ``client-run-id`` capability.
    """

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
    run_id: str | None = Field(default=None, pattern=RUN_ID_PATTERN)


class RunMetadata(ResponseModel):
    """Non-secret retry facts for one run."""

    requested_max_transport_retries: int | None = None
    effective_max_transport_retries: int | None = None
    provider_fallback: bool = False
    logical_turn_count: int = 1
    physical_model_request_count: int | None = None


class RunError(ResponseModel):
    """Why a run ended without a result.

    ``reason`` is ``exception``, ``cancel_requested``,
    ``client_disconnected``, or ``workflow_cancelled``. ``type`` and
    ``message`` name the exception when one exists.
    """

    reason: str
    type: str | None = None
    message: str | None = None


class TurnResponse(ResponseModel):
    """The recorded outcome of one turn."""

    run_id: str
    thread_id: str
    output: Any = None
    output_type: str | None = None
    messages: list[dict[str, Any]] = Field(default_factory=list)
    usage: dict[str, Any] | None = None
    run_metadata: RunMetadata = Field(default_factory=RunMetadata)


class StreamEnvelope(ResponseModel):
    """One SSE frame.

    DBOS may re-execute a step after a crash, so events can repeat. Subscribers
    deduplicate on ``(run_id, event_ordinal)``. The daemon stamps both ordinals.
    The request ordinal is ``ctx.run_step``.

    The event ordinal is one sequence for the whole run. It increases in
    publish order and never repeats for a different event. Inside a DBOS step it
    is ``(step_id << 20) | index``: the DBOS step id plus the index of the event
    in that step. A step that DBOS runs again after a crash yields the same
    ordinals, so the subscriber drops the copies. Outside a DBOS step, the
    daemon counts events for the run, from 1. The final frame has request and
    event ordinal ``-1``.

    The first frame of every stream has kind ``run_started``. It is a normal
    non-final frame with request and event ordinal ``0``, and its event is
    ``{"run_id": ...}``. The daemon sends it before the workflow starts, so the
    client knows the run id before any model event. Ordinal 0 belongs to this
    frame alone. The response header ``x-agentman-run-id`` carries the same id.
    """

    protocol: int = PROTOCOL_VERSION
    run_id: str
    request_ordinal: int
    event_ordinal: int
    kind: str
    event: dict[str, Any] = Field(default_factory=dict)
    final: bool = False


class NewThreadRequest(RequestModel):
    workspace: str
    name: str | None = None


class ThreadRef(RequestModel):
    workspace: str
    name: str


class ThreadInfo(ResponseModel):
    id: str
    workspace: str
    name: str
    created_at: datetime
    updated_at: datetime
    short_id: str
    runs: int = 0


class ThreadList(ResponseModel):
    threads: list[ThreadInfo] = Field(default_factory=list)


class RunInfo(ResponseModel):
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
    usage: dict[str, Any] | None = None
    error: RunError | None = None
    created_at: datetime
    updated_at: datetime


class ThreadView(ResponseModel):
    thread: ThreadInfo
    runs: list[RunInfo] = Field(default_factory=list)
    messages: list[dict[str, Any]] = Field(default_factory=list)


class StatusRequest(RequestModel):
    workspace: str
    thread_id: str | None = None
    name: str | None = None


class ResumeRequest(RequestModel):
    workspace: str
    run_id: str


class ResumeResponse(ResponseModel):
    run_id: str


class CancelRequest(RequestModel):
    workspace: str


class CancelResponse(ResponseModel):
    run_id: str
    status: str
    cancelled: bool


class UnresolvedEffect(ResponseModel):
    run_id: str
    tool_call_id: str
    tool_name: str
    status: str
    started_at: datetime
    note: str = "unknown_after_crash"


class StatusView(ResponseModel):
    thread: ThreadInfo | None = None
    latest_run: RunInfo | None = None
    unresolved_effects: list[UnresolvedEffect] = Field(default_factory=list)
    events: int = 0
    snapshots: int = 0
    message_rows: int = 0
    message_bytes: int = 0


class ErrorResponse(ResponseModel):
    error: str
    detail: str | None = None


__all__ = [
    "CAPABILITIES",
    "PROTOCOL_VERSION",
    "RUN_ID_PATTERN",
    "ErrorResponse",
    "CancelRequest",
    "CancelResponse",
    "NewThreadRequest",
    "RequestModel",
    "ResponseModel",
    "RunInfo",
    "RunMetadata",
    "ResumeRequest",
    "ResumeResponse",
    "RunError",
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
