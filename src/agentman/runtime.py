"""``execute()`` and the DBOS workflow.

One function, always wrapped as a DBOS workflow. The run id is the DBOS workflow
id, read at the top of the workflow and passed unchanged into pydantic-ai, into
``StepPersistence``, and into ``agentman.run``:

    agentman run id = pydantic-ai run id = StepPersistence run_id = DBOS workflow id

**agentman never serialises model calls.** No DBOS queue is registered on the
interactive path, there is no lock around ``execute()``, and there is no shared
exclusive connection. Postgres provides the concurrency boundary; agentman adds
none.

The product index is a durable boundary (F1). ``create_run``, ``update_run``,
``touch_thread`` and the history load are module-level ``@DBOS.step`` wrappers.
DBOS re-executes the workflow body on recovery and memoizes only steps, so a
plain ''INSERT'' in the body would raise a duplicate-key error on replay. A step
runs inline outside a workflow, so the injected test stores keep working.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Protocol

from dbos import DBOS
from dbos._error import DBOSWorkflowCancelledError
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.agent.abstract import EventStreamHandler
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter, ModelResponse
from pydantic_ai.models import Model
from pydantic_ai.usage import RunUsage
from pydantic_ai_harness.step_persistence import StepStore, continue_run

from agentman.agent import build_agent, turn_capabilities
from agentman.model import (
    DEFAULT_MAX_TRANSPORT_RETRIES,
    ModelResolutionError,
    TransportRequestCounter,
    bind_transport_request_counter,
    build_model,
)
from agentman.model import PREFIX as MODEL_PREFIX
from agentman.persistence.store import RUN_CANCELLED, RUN_COMPLETED, RUN_FAILED, RUN_RUNNING
from agentman.protocol import RunError, RunMetadata

logger = logging.getLogger(__name__)


class ProductStore(Protocol):
    """The thread and run surface ``run_turn`` uses.

    ``Store`` implements it against Postgres; tests implement it in memory.
    """

    async def create_run(
        self,
        *,
        run_id: str,
        thread_id: str,
        workspace: str,
        agent: str,
        model: str,
        prompt: str,
        run_metadata: dict[str, Any],
        status: str = "running",
    ) -> Any: ...  # pragma: no cover

    async def update_run(self, run_id: str, **fields: Any) -> None: ...  # pragma: no cover

    async def get_run(self, run_id: str) -> Any | None: ...  # pragma: no cover

    async def list_runs(self, *, thread_id: str) -> list[Any]: ...  # pragma: no cover

    async def touch_thread(self, thread_id: str) -> None: ...  # pragma: no cover


@dataclass(frozen=True)
class Turn:
    """Everything one turn needs, as plain data a DBOS workflow can record."""

    workspace: str
    thread_id: str
    agent_spec: dict[str, Any]
    agent_name: str | None
    model: str
    prompt: str
    tools: str | None = None
    output_schema: dict[str, Any] | None = None
    output_type: str | None = None
    router_url: str | None = None
    n_ctx: int | None = None
    max_transport_retries: int | None = None


@dataclass(frozen=True)
class TurnResult:
    """The recorded outcome of one turn."""

    run_id: str
    thread_id: str
    output: Any
    output_type: str | None
    messages: list[dict[str, Any]]
    usage: dict[str, Any] | None
    run_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Runtime:
    """The daemon-owned collaborators a workflow body needs."""

    step_store: StepStore
    product_store: ProductStore
    event_stream_handler: EventStreamHandler[Any] | None = None


_runtime: Runtime | None = None


def configure_runtime(
    *,
    step_store: StepStore,
    product_store: ProductStore,
    event_stream_handler: EventStreamHandler[Any] | None = None,
) -> None:
    """Install the collaborators the DBOS workflow body reads."""
    global _runtime
    reset_agent_cache()
    _runtime = Runtime(
        step_store=step_store,
        product_store=product_store,
        event_stream_handler=event_stream_handler,
    )


def runtime_configured() -> bool:
    return _runtime is not None


# -- the agent cache -----------------------------------------------------------
#
# Building a fresh Agent re-registers every DBOS step name (finding F8). The
# capabilities are run-agnostic within one agent shape. The cache key includes
# the workspace, tool declaration, model inputs, output schema, and
# collaborators by identity, so a different workspace or runtime never reuses a
# stale agent.

_agent_cache: dict[tuple[Any, ...], Agent[Any, Any]] = {}


def reset_agent_cache() -> None:
    """Drop every cached agent."""
    _agent_cache.clear()


def _agent_cache_key(
    turn: Turn,
    *,
    step_store: StepStore,
    event_stream_handler: EventStreamHandler[Any] | None,
) -> tuple[Any, ...]:
    shape = _agent_shape_key(turn)
    return (
        step_store,
        event_stream_handler,
        build_model,
        *shape,
    )


def _agent_shape_key(turn: Turn) -> tuple[Any, ...]:
    """Return the deterministic inputs that shape a cached agent.

    The first three cache-key entries are process-owned collaborators. They
    keep agents from crossing runtime configurations, but cannot be part of a
    DBOS operation name because their identities change across processes.
    """
    return (
        turn.workspace,
        turn.tools,
        json.dumps(turn.agent_spec, sort_keys=True),
        json.dumps(turn.output_schema, sort_keys=True),
        turn.agent_name,
        turn.model,
        turn.router_url,
        turn.n_ctx,
        turn.max_transport_retries,
    )


def _durability_name(turn: Turn) -> str:
    """Name DBOS operations from the stable, semantic cache-key shape."""
    payload = json.dumps(_agent_shape_key(turn), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return f"agentman_{digest}"


def _build_agent_for_turn(
    turn: Turn,
    *,
    step_store: StepStore,
    event_stream_handler: EventStreamHandler[Any] | None,
    model: Model | str,
) -> Agent[Any, Any]:
    capabilities = turn_capabilities(
        store=step_store,
        agent_name=turn.agent_name,
        event_stream_handler=event_stream_handler,
        workspace=turn.workspace,
        tools=turn.tools,
        durability_name=_durability_name(turn),
    )
    return build_agent(
        spec=turn.agent_spec,
        model=model,
        output_schema=turn.output_schema,
        capabilities=capabilities,
        name=turn.agent_name,
    )


def agent_for_turn(
    turn: Turn,
    *,
    step_store: StepStore,
    event_stream_handler: EventStreamHandler[Any] | None,
) -> Agent[Any, Any]:
    """Return the cached agent for the complete shape of this turn."""
    key = _agent_cache_key(turn, step_store=step_store, event_stream_handler=event_stream_handler)
    agent = _agent_cache.get(key)
    if agent is None:
        model = build_model(
            turn.model,
            router_url=turn.router_url,
            n_ctx=turn.n_ctx,
            max_transport_retries=turn.max_transport_retries,
        )
        agent = _build_agent_for_turn(
            turn, step_store=step_store, event_stream_handler=event_stream_handler, model=model
        )
        _agent_cache[key] = agent
    return agent


# -- durable boundary ----------------------------------------------------------
#
# The wrappers take the store and plain arguments. DBOS does not serialise step
# arguments, it passes them to the function; only the step output is recorded.
# Outside a workflow each wrapper runs inline, so tests that inject a store keep
# working unchanged.


@DBOS.step(name="agentman.create_run")
async def _create_run_step(
    product_store: ProductStore,
    *,
    run_id: str,
    thread_id: str,
    workspace: str,
    agent: str,
    model: str,
    prompt: str,
    run_metadata: dict[str, Any] | None = None,
) -> None:
    """Insert the product run row exactly once, even across recovery."""
    await product_store.create_run(
        run_id=run_id,
        thread_id=thread_id,
        workspace=workspace,
        agent=agent,
        model=model,
        prompt=prompt,
        run_metadata=run_metadata or {},
    )


@DBOS.step(name="agentman.update_run")
async def _update_run_step(product_store: ProductStore, run_id: str, fields: dict[str, Any]) -> None:
    """Update the product run row exactly once, even across recovery."""
    await product_store.update_run(run_id, **fields)


@DBOS.step(name="agentman.touch_thread")
async def _touch_thread_step(product_store: ProductStore, thread_id: str) -> None:
    """Touch the thread's ``updated_at`` exactly once, even across recovery."""
    await product_store.touch_thread(thread_id)


@DBOS.step(name="agentman.load_history")
async def _load_history_step(
    step_store: StepStore,
    product_store: ProductStore,
    thread_id: str,
    exclude_run_id: str | None = None,
) -> list[ModelMessage] | None:
    """Load the thread history exactly once, even across recovery."""
    return await load_history(step_store, product_store, thread_id, exclude_run_id=exclude_run_id)


ERROR_MESSAGE_LIMIT = 4000
"""The longest exception text a run row keeps, in characters."""


def exception_error(error: BaseException) -> dict[str, Any]:
    """Return the ``run.error`` value for a run that ended with ``error``."""
    return RunError(
        reason="exception",
        type=type(error).__name__,
        message=str(error)[:ERROR_MESSAGE_LIMIT],
    ).model_dump(mode="json")


def workflow_cancelled_error(error: BaseException) -> dict[str, Any]:
    """Return the run error value for a DBOS cancellation without an agentman request."""
    return RunError(
        reason="workflow_cancelled",
        type=type(error).__name__,
        message=str(error)[:ERROR_MESSAGE_LIMIT],
    ).model_dump(mode="json")


async def usage_so_far(step_store: StepStore, run_id: str) -> dict[str, Any] | None:
    """Return the usage of the model responses the journal holds for ``run_id``.

    A run that ends without a result has no pydantic-ai usage object. The
    latest snapshot holds every response that completed, so the sum is the
    usage known so far. A request that did not complete reports no usage: the
    server sends usage only in the last stream chunk. The read is best effort
    and returns ``None`` when the journal cannot be read.
    """
    try:
        snapshot = await step_store.latest_snapshot(run_id=run_id, include_interrupted=True)
    except Exception:
        logger.exception("could not read the usage of run %s", run_id)
        return None
    usage = RunUsage()
    if snapshot is not None:
        for message in snapshot.messages:
            if isinstance(message, ModelResponse) and message.run_id == run_id:
                usage.requests += 1
                usage.incr(message.usage)
    return _usage_dict(usage)


async def _record_failure(
    product_store: ProductStore,
    run_id: str,
    run_metadata: dict[str, Any],
    *,
    usage: dict[str, Any] | None,
    error: dict[str, Any],
) -> None:
    """Record ``failed`` without ever replacing the error that caused it.

    The recording is a Postgres write, and Postgres is often the reason the turn
    failed. A raise here would reach the caller in place of the original
    exception, which is the failure F1 exists to prevent. So the failure to
    record is logged and swallowed; the original exception continues to
    propagate.
    """
    try:
        await _update_run_step(
            product_store,
            run_id,
            {"status": RUN_FAILED, "run_metadata": run_metadata, "usage": usage, "error": error},
        )
    except Exception:
        logger.exception("could not record run %s as failed", run_id)


async def run_turn(
    turn: Turn,
    *,
    step_store: StepStore,
    product_store: ProductStore,
    run_id: str,
    event_stream_handler: EventStreamHandler[Any] | None = None,
    model: Model | str | None = None,
) -> TurnResult:
    """Run one turn and record it.

    ``model`` lets a test inject a model built against an in-process transport.
    The workflow body passes ``None``, and the agent comes from the cache keyed
    by the workspace, tools, spec, model inputs, output schema, and runtime
    collaborators.
    """
    if turn.max_transport_retries is not None and not turn.model.startswith(MODEL_PREFIX):
        raise ModelResolutionError(
            f"provider for {turn.model!r} cannot enforce `max_transport_retries`; "
            "the setting is supported for inferference models"
        )
    agent = (
        _build_agent_for_turn(turn, step_store=step_store, event_stream_handler=event_stream_handler, model=model)
        if model is not None
        else agent_for_turn(turn, step_store=step_store, event_stream_handler=event_stream_handler)
    )
    initial_metadata = _run_metadata(turn, physical_model_request_count=None)
    await _create_run_step(
        product_store,
        run_id=run_id,
        thread_id=turn.thread_id,
        workspace=turn.workspace,
        agent=turn.agent_name or "",
        model=turn.model,
        prompt=turn.prompt,
        run_metadata=initial_metadata,
    )
    track_physical_requests = model is None and turn.model.startswith(MODEL_PREFIX)
    request_counter = TransportRequestCounter() if track_physical_requests else None
    try:
        history = await _load_history_step(step_store, product_store, turn.thread_id, run_id)
        with bind_transport_request_counter(request_counter):
            result = await agent.run(
                turn.prompt,
                message_history=history,
                conversation_id=turn.thread_id,
                run_id=run_id,
            )
        messages = ModelMessagesTypeAdapter.dump_python(result.all_messages(), mode="json")
        usage = _usage_dict(result.usage)
        output = _jsonable(result.output)
        metadata = _run_metadata(
            turn,
            physical_model_request_count=request_counter.count if request_counter is not None else None,
        )
        await _update_run_step(
            product_store,
            run_id,
            {
                "status": RUN_COMPLETED,
                "output_json": output,
                "output_type": turn.output_type,
                "messages_json": messages,
                "usage": usage,
                "run_metadata": metadata,
            },
        )
        await _touch_thread_step(product_store, turn.thread_id)
        return TurnResult(
            run_id=run_id,
            thread_id=turn.thread_id,
            output=output,
            output_type=turn.output_type,
            messages=messages,
            usage=usage,
            run_metadata=metadata,
        )
    except DBOSWorkflowCancelledError as error:
        metadata = _run_metadata(
            turn,
            physical_model_request_count=request_counter.count if request_counter is not None else None,
        )
        fields: dict[str, Any] = {
            "usage": await usage_so_far(step_store, run_id),
            "run_metadata": metadata,
        }
        try:
            run = await product_store.get_run(run_id)
            if run is not None and run.status == RUN_RUNNING:
                fields.update(status=RUN_CANCELLED, error=workflow_cancelled_error(error))
            await product_store.update_run(run_id, **fields)
        except Exception:
            logger.exception("could not record run %s after DBOS cancellation", run_id)
        raise
    except Exception as error:
        metadata = _run_metadata(
            turn,
            physical_model_request_count=request_counter.count if request_counter is not None else None,
        )
        await _record_failure(
            product_store,
            run_id,
            metadata,
            usage=await usage_so_far(step_store, run_id),
            error=exception_error(error),
        )
        raise


def _run_metadata(turn: Turn, *, physical_model_request_count: int | None) -> dict[str, Any]:
    effective_retries = None
    if turn.model.startswith(MODEL_PREFIX):
        effective_retries = (
            turn.max_transport_retries if turn.max_transport_retries is not None else DEFAULT_MAX_TRANSPORT_RETRIES
        )
    return RunMetadata(
        requested_max_transport_retries=turn.max_transport_retries,
        effective_max_transport_retries=effective_retries,
        provider_fallback=False,
        logical_turn_count=1,
        physical_model_request_count=physical_model_request_count,
    ).model_dump(mode="json")


async def load_history(
    step_store: StepStore,
    product_store: ProductStore,
    thread_id: str,
    *,
    exclude_run_id: str | None = None,
) -> list[ModelMessage] | None:
    """Load a thread's history, preferring the harness snapshot.

    The snapshot is the record harness itself maintains. ``messages_json`` on the
    product run is only a fallback for a thread whose runs produced no snapshot.

    ``exclude_run_id`` keeps the current run's own snapshot out of its history.
    pydantic-ai raises ``UserError`` when a ``run_id`` already appears on
    ``message_history``, and a replayed body must not feed itself back in.
    """
    records = await step_store.list_runs(conversation_id=thread_id)
    for record in reversed(records):
        if exclude_run_id is not None and record.run_id == exclude_run_id:
            continue
        try:
            return await continue_run(step_store, run_id=record.run_id)
        except LookupError:
            continue
    runs = await product_store.list_runs(thread_id=thread_id)
    for prior in reversed(runs):
        if exclude_run_id is not None and prior.id == exclude_run_id:
            continue
        if prior.messages_json:
            return ModelMessagesTypeAdapter.validate_python(prior.messages_json)
    return None


@DBOS.workflow()
async def execute(turn: Turn) -> TurnResult:
    """Run one turn as a DBOS workflow.

    The workflow id is the run id. No queue, no lock, no shared connection.
    """
    if _runtime is None:
        raise RuntimeError("runtime is not configured; call configure_runtime() first")
    run_id = DBOS.workflow_id
    if not run_id:
        raise RuntimeError("execute() ran outside a DBOS workflow")
    return await run_turn(
        turn,
        step_store=_runtime.step_store,
        product_store=_runtime.product_store,
        run_id=run_id,
        event_stream_handler=_runtime.event_stream_handler,
    )


def _usage_dict(usage: Any) -> dict[str, Any] | None:
    if usage is None:
        return None
    if isinstance(usage, BaseModel):
        return usage.model_dump(mode="json")
    if is_dataclass(usage) and not isinstance(usage, type):
        return asdict(usage)
    return None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return str(value)


__all__ = [
    "Runtime",
    "ProductStore",
    "Turn",
    "TurnResult",
    "agent_for_turn",
    "configure_runtime",
    "exception_error",
    "execute",
    "load_history",
    "reset_agent_cache",
    "run_turn",
    "runtime_configured",
    "usage_so_far",
]
