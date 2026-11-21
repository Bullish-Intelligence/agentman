"""``agentman serve``: the transport over ``execute()``.

An ASGI app served over a Unix socket. The daemon is never a second
implementation of the turn: it calls the same :func:`agentman.runtime.execute`
that tests call directly.

It holds two pieces of mutable state. The first is a ``run_id -> subscriber``
registry. ``event_stream_handler`` fires inside the DBOS model step and
publishes to it; the matching HTTP connection drains it. When nobody is
listening, events are dropped, not buffered. The stream is presentation; the
record is authoritative.

The second is a map of live runs. The turn handler subscribes and adds the run
before it starts the workflow, so no event is lost. The map lets a cancel find a
run whose row does not exist yet.

The third is a map of the shell processes that each run started. A cancel stops
them. A daemon restart loses this map.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from dbos import DBOS, DBOSConfig, SetWorkflowID
from dbos._error import DBOSAwaitedWorkflowCancelledError
from inferference import InferferenceClient
from pydantic import TypeAdapter, ValidationError
from pydantic_ai.messages import AgentStreamEvent
from pydantic_ai_harness.step_persistence import ToolEffectRecord
from sqlalchemy.exc import IntegrityError

from agentman import __version__
from agentman.config import Config
from agentman.model import PREFIX as MODEL_PREFIX
from agentman.model import ModelResolutionError, require_schema, resolve_capability_model, router_api_key
from agentman.persistence.engine import create_engine
from agentman.persistence.inspect import Journal
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import (
    RUN_ACTIVE,
    RUN_CANCEL_REQUESTED,
    RUN_CANCELLED,
    RUN_COMPLETED,
    RUN_RUNNING,
    RUN_TERMINAL,
    Run,
    Store,
)
from agentman.processes import (
    TOOL_KILL_WAIT_SECONDS,
    TOOL_STOP_GRACE_SECONDS,
    ToolProcesses,
    group_alive,
    stop_process_groups,
)
from agentman.protocol import (
    CAPABILITIES,
    PROTOCOL_VERSION,
    CancelRequest,
    CancelResponse,
    ErrorResponse,
    NewThreadRequest,
    RequestModel,
    ResumeRequest,
    ResumeResponse,
    RunError,
    StatusRequest,
    StreamEnvelope,
    ThreadList,
    ThreadRef,
    ThreadView,
    TurnRequest,
    TurnResponse,
)
from agentman.recovery import ResumeRefused, authorize_resume
from agentman.runtime import Turn, configure_runtime, execute, usage_so_far
from agentman.status import compose_status, run_info, thread_info, thread_messages

_STREAM_ADAPTER = TypeAdapter(AgentStreamEvent)
logger = logging.getLogger(__name__)

# The effect summary of a tool that a cancel stopped.
_STOPPED_BY_CANCEL = "stopped by cancel"


# One handler call numbers its events in the low bits. The DBOS step id sits above them.
_STEP_INDEX_BITS = 20
_MAX_EVENTS_PER_STEP = 1 << _STEP_INDEX_BITS


class SubscriberRegistry:
    """``run_id -> subscriber`` queues. Nothing is buffered.

    The workflow can run on a worker thread while the HTTP connection drains on
    the event loop, so publishing is scheduled with ``call_soon_threadsafe``.
    A plain ``put_nowait`` from another thread would not wake the draining task.

    It also holds the fallback event counter of each run, for events that
    arrive outside a DBOS step. A lock guards the counters for the same reason.
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[tuple[asyncio.Queue[StreamEnvelope | None], asyncio.AbstractEventLoop]]] = {}
        self._counters: dict[str, int] = {}
        self._lock = threading.Lock()

    def subscribe(self, run_id: str) -> asyncio.Queue[StreamEnvelope | None]:
        queue: asyncio.Queue[StreamEnvelope | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        self._queues.setdefault(run_id, []).append((queue, loop))
        return queue

    def reserve(self, run_id: str) -> asyncio.Queue[StreamEnvelope | None] | None:
        """Subscribe to a run id that has no subscriber. Return ``None`` when one exists.

        The check and the subscribe have no await between them, so two requests
        cannot both reserve the same id.
        """
        if run_id in self._queues:
            return None
        return self.subscribe(run_id)

    def unsubscribe(self, run_id: str, queue: asyncio.Queue[StreamEnvelope | None]) -> None:
        subscribers = self._queues.get(run_id)
        if not subscribers:
            return
        remaining = [entry for entry in subscribers if entry[0] is not queue]
        if remaining:
            self._queues[run_id] = remaining
        else:
            self._queues.pop(run_id, None)
            with self._lock:
                self._counters.pop(run_id, None)

    def next_ordinal(self, run_id: str) -> int | None:
        """Return the next fallback event ordinal of a run, or ``None`` when nobody listens.

        The counter starts at 1. Ordinal 0 is free for a daemon-stamped frame.
        The registry forgets the counter when the last subscriber leaves. A late
        event then gets ``None`` and does not make a new counter.
        """
        with self._lock:
            if run_id not in self._queues:
                return None
            ordinal = self._counters.get(run_id, 0) + 1
            self._counters[run_id] = ordinal
            return ordinal

    def publish(self, run_id: str, envelope: StreamEnvelope | None) -> None:
        for queue, loop in self._queues.get(run_id, []):
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:  # pragma: no cover - publish always runs on a loop
                running = None
            if running is loop:
                queue.put_nowait(envelope)
            else:
                loop.call_soon_threadsafe(queue.put_nowait, envelope)


def make_event_handler(registry: SubscriberRegistry, tools: ToolProcesses | None = None):
    """Return the ``event_stream_handler`` that stamps and publishes events.

    The request ordinal is ``ctx.run_step``. The event ordinal is one sequence
    for the whole run. Neither ordinal exists on ``AgentStreamEvent``; the
    daemon stamps both.

    Inside a DBOS step, the event ordinal is ``(step_id << 20) | index``. The
    step id counts up in call order and stays the same when DBOS recovers the
    run. The index counts the events of this handler call. A step that DBOS runs
    again therefore gives the same ordinals, and the client drops the copies.
    Step ids start at 1, so these ordinals are 1 or more.

    Outside a DBOS step, the registry counts events for the run, from 1.

    With ``tools``, the handler also records the shell commands of the run: a
    cancel needs their process ids. The handler records an event before it
    checks for a subscriber, so a run with no listener still has its commands
    tracked.
    """

    async def handler(ctx: Any, events: Any) -> None:
        request_ordinal = ctx.run_step
        step_id = DBOS.step_id
        index = 0
        async for event in events:
            if tools is not None and ctx.run_id is not None:
                tools.observe(ctx.run_id, event)
            if step_id is None:
                ordinal = registry.next_ordinal(ctx.run_id)
                if ordinal is None:
                    continue
            else:
                if index >= _MAX_EVENTS_PER_STEP:
                    raise RuntimeError(
                        f"one handler call emitted {_MAX_EVENTS_PER_STEP} or more events; "
                        "the event ordinal cannot number them"
                    )
                ordinal = (step_id << _STEP_INDEX_BITS) | index
                index += 1
            envelope = StreamEnvelope(
                run_id=ctx.run_id,
                request_ordinal=request_ordinal,
                event_ordinal=ordinal,
                kind=getattr(event, "event_kind", type(event).__name__),
                event=_STREAM_ADAPTER.dump_python(event, mode="json"),
            )
            registry.publish(ctx.run_id, envelope)

    return handler


@dataclass
class _LiveRun:
    """A run between the reservation of its id and the end of its stream.

    The workflow writes the run row in its first step, so the row can be absent
    while the run is live. This record holds what a cancel needs in that window.
    """

    workspace: str
    thread_id: str
    agent: str
    model: str
    prompt: str
    # Set when the workflow has started, or when the start failed.
    started: asyncio.Event = field(default_factory=asyncio.Event)
    # Set by ``cancel_run`` before it asks DBOS to cancel.
    cancel_reason: str | None = None
    # The result of the tool stop of a cancel: the process ids that still live.
    # A cancel, a disconnect, and the stream share it. The stream waits for it
    # before it sends the final frame of a cancelled run.
    stopped: asyncio.Future[list[int]] = field(default_factory=lambda: asyncio.get_running_loop().create_future())
    stop_started: bool = False


class AgentmanApp:
    """The ASGI application."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.registry = SubscriberRegistry()
        self._live: dict[str, _LiveRun] = {}
        self.tools = ToolProcesses()
        # Tests set shorter times. The values are in seconds.
        self.tool_stop_grace = TOOL_STOP_GRACE_SECONDS
        self.tool_kill_wait = TOOL_KILL_WAIT_SECONDS
        self.engine: Any = None
        self.store: Store | None = None
        self.step_store: PostgresStepStore | None = None
        self.journal: Journal | None = None
        self.router_url: str | None = config.router_url.value

    # -- lifespan --------------------------------------------------------------

    async def startup(self) -> None:
        database_url = self.config.database_url.value
        if not database_url:
            raise RuntimeError("database_url is not configured; set it in $XDG_CONFIG_HOME/agentman/agentman.yaml")
        if self.router_url is None and (self.config.model.value or "").startswith(MODEL_PREFIX):
            from inferference import resolve_router_url

            self.router_url = resolve_router_url()
        # Build the engine, stores and journal first, then install the runtime,
        # then launch DBOS. Recovery runs on background threads as soon as
        # ``launch()`` returns, so a recovered workflow must find a configured
        # runtime. ``AsyncEngine`` creation is lazy, so this costs nothing.
        self.engine = create_engine(database_url)
        self.store = Store(self.engine)
        self.step_store = PostgresStepStore(self.engine)
        self.journal = Journal(self.engine)
        assert self.store is not None and self.step_store is not None
        configure_runtime(
            step_store=self.step_store,
            product_store=self.store,
            event_stream_handler=make_event_handler(self.registry, self.tools),
            router_key_env=self.config.router_key_env.value,
        )
        DBOS(
            config=DBOSConfig(
                name="agentman",
                system_database_url=database_url,
                dbos_system_schema="dbos",
            )
        )
        DBOS.launch()
        await self._repair_cancel_requests()

    async def shutdown(self) -> None:
        if self.engine is not None:
            await self.engine.dispose()
        DBOS.destroy()

    # -- ASGI ------------------------------------------------------------------

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope["type"] != "http":
            raise RuntimeError(f"unsupported scope type {scope['type']!r}")
        await self._http(scope, receive, send)

    async def _lifespan(self, receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                try:
                    await self.startup()
                except Exception as error:
                    await send({"type": "lifespan.startup.failed", "message": repr(error)})
                    return
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _http(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        method = scope["method"]
        path = scope["path"]
        query = _query(scope.get("query_string", b""))
        try:
            body = await _read_body(receive)
            await self._route(method, path, query, body, receive, send)
        except _HttpError as error:
            response = ErrorResponse(error=error.message, detail=error.detail)
            await _send_json(send, error.status, response.model_dump(mode="json"))

    async def _route(self, method: str, path: str, query: dict[str, str], body: bytes, receive: Any, send: Any) -> None:
        if method == "GET" and path == "/v1/health":
            await _send_json(
                send,
                200,
                {
                    "status": "ok",
                    "version": __version__,
                    "protocol": PROTOCOL_VERSION,
                    "capabilities": list(CAPABILITIES),
                },
            )
            return
        if method == "POST" and path == "/v1/turns":
            await self._handle_turn(body, receive, send)
            return
        if method == "POST" and path == "/v1/threads":
            request = _parse_body(NewThreadRequest, body)
            thread = await self._store().create_thread(workspace=request.workspace, name=request.name)
            await _send_json(send, 200, (await thread_info(self._store(), thread)).model_dump(mode="json"))
            return
        if method == "GET" and path == "/v1/threads":
            workspace = _required(query, "workspace")
            threads = await self._store().list_threads(workspace=workspace)
            infos = [await thread_info(self._store(), thread) for thread in threads]
            await _send_json(send, 200, ThreadList(threads=infos).model_dump(mode="json"))
            return
        if method == "POST" and path == "/v1/threads/resolve":
            request = _parse_body(ThreadRef, body)
            thread = await self._store().resolve_thread_name(workspace=request.workspace, name=request.name)
            if thread is None:
                raise _HttpError(404, f"no thread named {request.name!r} in {request.workspace}")
            await _send_json(send, 200, (await thread_info(self._store(), thread)).model_dump(mode="json"))
            return
        if method == "GET" and path.startswith("/v1/threads/"):
            thread_id = path.rsplit("/", 1)[-1]
            store = self._store()
            thread = await store.get_thread(thread_id)
            if thread is None:
                raise _HttpError(404, f"no thread {thread_id!r}")
            runs = await store.list_runs(thread_id=thread_id)
            messages = await thread_messages(self.step_store, runs)
            await _send_json(
                send,
                200,
                ThreadView(
                    thread=await thread_info(store, thread),
                    runs=[run_info(run) for run in runs],
                    messages=messages,
                ).model_dump(mode="json"),
            )
            return
        if method == "GET" and path.startswith("/v1/runs/"):
            run_id = path.rsplit("/", 1)[-1]
            run = await self._store().get_run(run_id)
            if run is None:
                raise _HttpError(404, f"no run {run_id!r}")
            await _send_json(send, 200, run_info(run).model_dump(mode="json"))
            return
        cancel_prefix = "/v1/runs/"
        cancel_suffix = "/cancel"
        if method == "POST" and path.startswith(cancel_prefix) and path.endswith(cancel_suffix):
            run_id = path[len(cancel_prefix) : -len(cancel_suffix)]
            if not run_id or "/" in run_id:
                raise _HttpError(404, f"no route for {method} {path}")
            request = _parse_body(CancelRequest, body)
            result = await self.cancel_run(run_id, workspace=request.workspace, reason="cancel_requested")
            await _send_json(send, 200, CancelResponse.model_validate(result).model_dump(mode="json"))
            return
        if method == "POST" and path == "/v1/status":
            await self._handle_status(body, send)
            return
        if method == "POST" and path == "/v1/resume":
            await self._handle_resume(body, send)
            return
        raise _HttpError(404, f"no route for {method} {path}")

    async def _handle_turn(self, body: bytes, receive: Any, send: Any) -> None:
        request = _parse_body(TurnRequest, body)
        store = self._store()
        thread = await store.get_thread(request.thread_id)
        if thread is None:
            raise _HttpError(404, f"no thread {request.thread_id!r}; create it first")

        model, router_url, n_ctx = await self._resolve_turn_model(request)

        turn = Turn(
            workspace=request.workspace,
            thread_id=request.thread_id,
            agent_spec=request.agent_spec,
            tools=request.tools,
            agent_name=request.agent_name,
            model=model,
            prompt=request.prompt,
            output_schema=request.output_schema,
            output_type=request.output_type,
            router_url=router_url,
            n_ctx=n_ctx,
            max_transport_retries=request.max_transport_retries,
            metadata=request.metadata,
        )
        run_id = request.run_id or str(uuid.uuid4())
        # Subscribe before the workflow starts, so no event is lost. The check
        # and the subscribe have no await between them.
        queue = self.registry.reserve(run_id)
        if queue is None:
            raise _HttpError(409, f"run {run_id!r} is already in use")
        live = _LiveRun(
            workspace=request.workspace,
            thread_id=request.thread_id,
            agent=request.agent_name or "",
            model=model,
            prompt=request.prompt,
        )
        self._live[run_id] = live
        drain_task: asyncio.Task[None] | None = None
        disconnect_task: asyncio.Task[None] | None = None
        result_task: asyncio.Task[Any] | None = None
        try:
            await self._refuse_existing_run(run_id)
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"text/event-stream"),
                        (b"cache-control", b"no-cache"),
                        (b"x-agentman-run-id", run_id.encode()),
                    ],
                }
            )
            self.registry.publish(
                run_id,
                StreamEnvelope(
                    run_id=run_id,
                    request_ordinal=0,
                    event_ordinal=0,
                    kind="run_started",
                    event={"run_id": run_id},
                ),
            )
            drain_task = asyncio.create_task(_drain(queue, send))
            disconnect_task = asyncio.create_task(_wait_disconnect(receive))
            try:
                with SetWorkflowID(run_id):
                    handle = await DBOS.start_workflow_async(execute, turn)
            except Exception:
                # The response already started. Nothing on this path may raise.
                logger.exception("could not start the workflow of run %s", run_id)
                live.started.set()
                self.registry.publish(run_id, None)
                await drain_task
                await _send_sse(
                    send,
                    StreamEnvelope(
                        run_id=run_id,
                        request_ordinal=-1,
                        event_ordinal=-1,
                        kind="turn_error",
                        event={"error": "turn failed before it started"},
                        final=True,
                    ),
                )
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                return
            live.started.set()
            result_task = asyncio.create_task(handle.get_result())
            done, _ = await asyncio.wait(
                (result_task, disconnect_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if disconnect_task in done:
                # The response already started. Nothing on this path may raise.
                accepted = await self._cancel_after_disconnect(run_id, workspace=request.workspace)
                drain_task.cancel()
                await asyncio.gather(drain_task, return_exceptions=True)
                if accepted:
                    try:
                        await result_task
                    except DBOSAwaitedWorkflowCancelledError:
                        pass
                    except Exception:
                        logger.exception("run %s ended with an error after the client disconnected", run_id)
                return

            disconnect_task.cancel()
            await asyncio.gather(disconnect_task, return_exceptions=True)
            try:
                result = result_task.result()
            except DBOSAwaitedWorkflowCancelledError:
                # The final frame waits until the tool processes are gone.
                live_processes = await self._live_tool_processes(run_id, live)
                run = await self._read_run_after_cancel(run_id)
                self.registry.publish(run_id, None)
                await drain_task
                if run is not None and run.status == RUN_COMPLETED:
                    # The turn finished before the cancel took effect. The row is the result.
                    await _send_sse(send, _result_frame(run))
                else:
                    if run is not None and run.error is not None:
                        reason = run.error.get("reason")
                    else:
                        reason = live.cancel_reason or "workflow_cancelled"
                    await _send_sse(
                        send,
                        StreamEnvelope(
                            run_id=run_id,
                            request_ordinal=-1,
                            event_ordinal=-1,
                            kind="turn_error",
                            event={
                                "error": "turn cancelled",
                                "reason": reason,
                                "run_id": run_id,
                                "live_tool_processes": live_processes,
                            },
                            final=True,
                        ),
                    )
            except Exception:
                self.registry.publish(run_id, None)
                await drain_task
                await _send_sse(
                    send,
                    StreamEnvelope(
                        run_id=run_id,
                        request_ordinal=-1,
                        event_ordinal=-1,
                        kind="turn_error",
                        event={"error": "turn failed before a result; inspect the durable run record"},
                        final=True,
                    ),
                )
            else:
                self.registry.publish(run_id, None)
                await drain_task
                final = StreamEnvelope(
                    run_id=run_id,
                    request_ordinal=-1,
                    event_ordinal=-1,
                    kind="turn_result",
                    event=TurnResponse(
                        run_id=result.run_id,
                        thread_id=result.thread_id,
                        output=result.output,
                        output_type=result.output_type,
                        messages=result.messages,
                        usage=result.usage,
                        run_metadata=getattr(result, "run_metadata", {}),
                    ).model_dump(mode="json"),
                    final=True,
                )
                await _send_sse(send, final)
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            live.started.set()
            self._live.pop(run_id, None)
            self.tools.discard(run_id)
            tasks = [task for task in (result_task, drain_task, disconnect_task) if task is not None]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.registry.unsubscribe(run_id, queue)

    async def _refuse_existing_run(self, run_id: str) -> None:
        """Raise 409 when a workflow or a run row already uses the id.

        DBOS does not refuse a duplicate workflow id. It returns a handle to the
        existing workflow, so the daemon checks for itself.
        """
        if await DBOS.get_workflow_status_async(run_id) is not None or await self._store().get_run(run_id) is not None:
            raise _HttpError(409, f"run {run_id!r} already exists")

    async def cancel_run(self, run_id: str, *, workspace: str, reason: str) -> dict[str, Any]:
        """Cancel a workflow, stop its tool processes, and record the run state.

        The order is:

        1. Write ``cancel_requested`` on the run row.
        2. Ask DBOS to cancel the workflow. DBOS then refuses the next step.
        3. Send SIGTERM to each shell process group that the run started and did
           not finish. After a grace time, send SIGKILL to the groups that remain.
        4. Write ``cancelled``.

        The call returns after step 4. If a group survives SIGKILL, the row stays
        ``cancel_requested``, the daemon logs it, and the call returns
        ``cancelled: false`` with that status.

        The first writer of a final status wins and sets the reason. Return
        ``cancelled: true`` when the run was ``running`` or ``cancel_requested``
        at the start of the request and is ``cancelled`` at the end, even if the
        workflow unwind wrote the status first. Otherwise return
        ``cancelled: false`` with the current status.

        The workflow writes the run row in its first step. A cancel can arrive
        before that. The live-run map then names the run, and the daemon writes
        the row itself after DBOS accepts the cancel.
        """
        store = self._store()
        run = await store.get_run(run_id)
        live = self._live.get(run_id)
        if run is not None:
            owner = run.workspace
        elif live is not None:
            owner = live.workspace
        else:
            raise _HttpError(404, f"no run {run_id!r}")
        if owner != workspace:
            raise _HttpError(409, f"run {run_id!r} belongs to a different workspace")
        if run is not None and run.status in RUN_TERMINAL:
            return {"run_id": run_id, "status": run.status, "cancelled": False}
        current = run.status if run is not None else RUN_RUNNING

        if live is not None:
            # The turn handler reads the reason when the cancel ends its stream.
            if live.cancel_reason is None:
                live.cancel_reason = reason
            # DBOS cannot cancel a workflow that does not exist yet.
            await live.started.wait()
        try:
            return await self._cancel_workflow(run_id, run, live, reason=reason, current=current)
        finally:
            self._release_stop(live)

    async def _cancel_workflow(
        self, run_id: str, run: Run | None, live: _LiveRun | None, *, reason: str, current: str
    ) -> dict[str, Any]:
        """Run steps 1 to 4 of ``cancel_run`` for a run that passed the owner check."""
        store = self._store()
        requested = await self._request_cancel(run, reason=reason)
        try:
            await DBOS.cancel_workflow_async(run_id)
            workflow_status = await DBOS.get_workflow_status_async(run_id)
        except BaseException:
            await self._reopen_run(run_id, requested)
            raise
        status = workflow_status.status if workflow_status is not None else None
        if status == "CANCELLED":
            survivors = await self._stop_tools(run_id, live)
            if survivors:
                latest = await store.get_run(run_id)
                return {
                    "run_id": run_id,
                    "status": latest.status if latest is not None else RUN_CANCEL_REQUESTED,
                    "cancelled": False,
                }
            latest = await self._finish_cancelled(run_id, reason=reason, live=live)
            if latest is not None and latest.status == RUN_CANCELLED:
                return {"run_id": run_id, "status": RUN_CANCELLED, "cancelled": True}
            return {"run_id": run_id, "status": latest.status if latest is not None else current, "cancelled": False}
        if status in {"SUCCESS", "ERROR"}:
            latest = await store.get_run(run_id)
            return {
                "run_id": run_id,
                "status": latest.status if latest is not None else current,
                "cancelled": False,
            }
        await self._reopen_run(run_id, requested)
        raise _HttpError(409, f"run {run_id!r} could not be cancelled")

    async def _request_cancel(self, run: Run | None, *, reason: str) -> bool:
        """Write ``cancel_requested`` on a ``running`` row. Return ``True`` when this call wrote it.

        A run with no row yet has nothing to write. A run that already says
        ``cancel_requested`` keeps the reason of the first request.
        """
        if run is None or run.status != RUN_RUNNING:
            return False
        error = RunError(reason=reason).model_dump(mode="json")
        return await self._store().request_cancel(run.id, error=error)

    async def _reopen_run(self, run_id: str, requested: bool) -> None:
        """Undo ``cancel_requested`` when DBOS did not cancel the workflow. Never raise."""
        if not requested:
            return
        try:
            await self._store().reopen_run(run_id)
        except Exception:
            logger.exception("could not move run %s back to running after a refused cancel", run_id)

    async def _stop_tools(self, run_id: str, live: _LiveRun | None) -> list[int]:
        """Stop the tool processes of a run. Return the process ids that still live.

        The cancel, the disconnect path, and the stream share one stop for each
        live run. The stop runs as its own task, so a dropped request does not
        end it half way.
        """
        if live is None:
            return await self._stop_tool_processes(run_id)
        if live.stopped.done() and live.stopped.result():
            # An earlier stop left a process alive. A new cancel tries again.
            live.stopped = asyncio.get_running_loop().create_future()
            live.stop_started = False
        if not live.stop_started:
            live.stop_started = True
            stopped = live.stopped
            task = asyncio.create_task(self._stop_tool_processes(run_id))
            task.add_done_callback(lambda done: _settle(stopped, done))
        return await asyncio.shield(live.stopped)

    def _release_stop(self, live: _LiveRun | None) -> None:
        """Release a stream that waits for a stop that will never start."""
        if live is not None and not live.stop_started and not live.stopped.done():
            live.stopped.set_result([])

    async def _stop_tool_processes(self, run_id: str) -> list[int]:
        tracked = self.tools.snapshot(run_id)
        if not tracked:
            return []
        try:
            return await self._stop_tracked(run_id, tracked)
        except Exception:
            logger.exception("could not stop the tool processes of run %s", run_id)
            return [pid for pid in tracked.values() if group_alive(pid)]

    async def _stop_tracked(self, run_id: str, tracked: dict[str, int]) -> list[int]:
        # Only a group that exists at this moment is a running tool process. The
        # tracker drops a command at its exit code, so a finished command is not here.
        running = {call_id: pid for call_id, pid in tracked.items() if group_alive(pid)}
        survivors = await stop_process_groups(
            running.values(), grace=self.tool_stop_grace, kill_wait=self.tool_kill_wait
        )
        stopped = {call_id: pid for call_id, pid in running.items() if pid not in survivors}
        for call_id in stopped:
            self.tools.remove(run_id, call_id)
        await self._record_stopped_effects(run_id, stopped)
        if survivors:
            logger.error(
                "run %s: tool process groups %s survived SIGKILL; the run stays cancel_requested",
                run_id,
                survivors,
            )
        return survivors

    async def _record_stopped_effects(self, run_id: str, stopped: dict[str, int]) -> None:
        """Mark the effect record of each stopped command ``failed``. Never raise.

        The cancelled workflow cannot run ``finish_tool_effect``, because DBOS
        refuses the step. Without this write, the record stays ``started`` and
        ``status`` lists it as unresolved, although the command has exited.
        """
        if not stopped or self.step_store is None:
            return
        for call_id in stopped:
            try:
                prior = await self.step_store.get_tool_effect(run_id=run_id, tool_call_id=call_id)
                if prior is None or prior.status != "started":
                    continue
                await self.step_store.record_tool_effect(
                    ToolEffectRecord(
                        tool_call_id=call_id,
                        tool_name=prior.tool_name,
                        run_id=run_id,
                        status="failed",
                        started_at=prior.started_at,
                        ended_at=datetime.now(UTC),
                        idempotency_key=prior.idempotency_key,
                        effect_summary=prior.effect_summary or _STOPPED_BY_CANCEL,
                    )
                )
            except Exception:
                logger.exception("could not record the stopped tool %s of run %s", call_id, run_id)

    async def _live_tool_processes(self, run_id: str, live: _LiveRun) -> list[int]:
        """Wait for the tool stop of a cancel. Return the process ids that still live.

        A workflow that DBOS cancelled without a daemon cancel has no stop to
        wait for. The result then lists the tracked groups that still exist.
        """
        if live.cancel_reason is not None:
            await asyncio.shield(live.stopped)
        return [pid for pid in self.tools.snapshot(run_id).values() if group_alive(pid)]

    async def _finish_cancelled(self, run_id: str, *, reason: str, live: _LiveRun | None = None) -> Run | None:
        """Write ``cancelled`` if the run is still active; return the row after the write.

        A ``cancel_requested`` row holds the reason of the first request. That
        reason wins over ``reason``, so concurrent cancels agree.

        ``live`` is the live-run record. With it, the function creates a
        ``cancelled`` row when the run has none. This happens when DBOS cancelled
        the workflow before its first step ran.
        """
        store = self._store()
        usage = await usage_so_far(self.step_store, run_id) if self.step_store is not None else None
        before = await store.get_run(run_id)
        if before is not None and before.status == RUN_CANCEL_REQUESTED and before.error is not None:
            reason = before.error.get("reason") or reason
        error = RunError(reason=reason).model_dump(mode="json")
        won = await store.finish_run(run_id, status=RUN_CANCELLED, usage=usage, error=error)
        latest = await store.get_run(run_id)
        if latest is None and live is not None:
            try:
                latest = await store.create_run(
                    run_id=run_id,
                    thread_id=live.thread_id,
                    workspace=live.workspace,
                    agent=live.agent,
                    model=live.model,
                    prompt=live.prompt,
                    status=RUN_CANCELLED,
                    error=error,
                )
                won = True
            except IntegrityError:
                # The workflow wrote the row first. Finish that row instead.
                won = await store.finish_run(run_id, status=RUN_CANCELLED, usage=usage, error=error)
                latest = await store.get_run(run_id)
        if won:
            logger.info("cancelled run %s with reason %s", run_id, reason)
        return latest

    async def _cancel_after_disconnect(self, run_id: str, *, workspace: str) -> bool:
        """Cancel the workflow of a client that left. Never raise.

        The run row may not exist yet: the workflow creates it. The DBOS cancel
        needs no row, so it does not wait for one. The function writes
        ``cancel_requested`` first when the row exists, then asks DBOS to cancel,
        then stops the tool processes, then records ``cancelled``. It returns
        ``True`` when DBOS accepted the cancel.
        """
        live = self._live.get(run_id)
        run: Run | None = None
        requested = False
        try:
            run = await self._store().get_run(run_id)
            if run is not None and run.workspace != workspace:
                run = None
            requested = await self._request_cancel(run, reason="client_disconnected")
        except Exception:
            logger.exception("could not mark run %s as cancel_requested after the client disconnected", run_id)
        try:
            await DBOS.cancel_workflow_async(run_id)
        except Exception:
            logger.exception("could not cancel run %s after the client disconnected", run_id)
            await self._reopen_run(run_id, requested)
            return False
        try:
            survivors = await self._stop_tools(run_id, live)
            run = await self._store().get_run(run_id)
            if survivors:
                return True
            if run is not None and run.workspace == workspace and run.status in RUN_ACTIVE:
                await self._finish_cancelled(run_id, reason="client_disconnected")
        except Exception:
            logger.exception("could not record run %s as cancelled after the client disconnected", run_id)
        return True

    async def _read_run_after_cancel(self, run_id: str) -> Run | None:
        try:
            return await self._store().get_run(run_id)
        except Exception:
            logger.exception("could not read run %s after the workflow was cancelled", run_id)
            return None

    async def _resolve_turn_model(self, request: TurnRequest) -> tuple[str, str | None, int | None]:
        if not request.model.startswith(MODEL_PREFIX):
            if request.max_transport_retries is not None:
                raise _HttpError(
                    422,
                    f"provider for {request.model!r} cannot enforce `max_transport_retries`; "
                    "the setting is supported for inferference models",
                )
            return request.model, self.router_url, None

        try:
            capabilities = await self._capabilities()
        except Exception as error:
            raise _HttpError(503, "cannot resolve the inferference router or read its capabilities") from error

        try:
            if request.output_schema is not None:
                require_schema(capabilities)
            reference = request.model[len(MODEL_PREFIX) :]
            resolved, model_info = resolve_capability_model(capabilities, reference)
        except ModelResolutionError as error:
            raise _HttpError(422, str(error)) from error
        return f"{MODEL_PREFIX}{resolved}", self.router_url, model_info.n_ctx

    async def _handle_status(self, body: bytes, send: Any) -> None:
        request = _parse_body(StatusRequest, body)
        if self.journal is None:
            raise _HttpError(503, "the daemon is not started")
        view = await compose_status(
            store=self._store(),
            journal=self.journal,
            workspace=request.workspace,
            thread_id=request.thread_id,
            name=request.name,
        )
        await _send_json(send, 200, view.model_dump(mode="json"))

    async def _handle_resume(self, body: bytes, send: Any) -> None:
        request = _parse_body(ResumeRequest, body)
        run = await self._store().get_run(request.run_id)
        if run is None:
            raise _HttpError(409, f"cannot resume {request.run_id}: not an agentman run")
        if run.workspace != request.workspace:
            raise _HttpError(409, f"cannot resume {request.run_id}: belongs to a different workspace")
        if self.journal is None:
            raise _HttpError(503, "the daemon is not started")
        try:
            await self._refuse_cancelled_workflow(run)
            await authorize_resume(store=self._store(), journal=self.journal, run_id=request.run_id)
            handle = await DBOS.resume_workflow_async(request.run_id)
            await handle.get_result()
        except ResumeRefused as error:
            raise _HttpError(409, str(error)) from error
        except DBOSAwaitedWorkflowCancelledError as error:
            raise _HttpError(409, f"run {request.run_id} was cancelled while it resumed") from error
        await _send_json(send, 200, ResumeResponse(run_id=request.run_id).model_dump(mode="json"))

    async def _refuse_cancelled_workflow(self, run: Run) -> None:
        """Refuse to resume a run that DBOS cancelled.

        A crash can fall between the DBOS cancel and the row write. Then the row
        says ``running`` or ``cancel_requested`` while DBOS says ``CANCELLED``,
        and DBOS would restart the workflow. This method repairs the row and
        refuses the resume. A ``cancel_requested`` row that DBOS has not
        cancelled is refused later, by ``authorize_resume``.
        """
        if run.status in RUN_TERMINAL:
            return
        workflow_status = await DBOS.get_workflow_status_async(run.id)
        if workflow_status is None or workflow_status.status != "CANCELLED":
            return
        await self._finish_cancelled(run.id, reason="workflow_cancelled")
        raise ResumeRefused(f"cannot resume {run.id}: the workflow was cancelled")

    async def _repair_cancel_requests(self) -> None:
        """Settle each ``cancel_requested`` row that a crash left behind. Never raise.

        The status is not final, and only a live daemon moves it on. After a
        restart no daemon holds the request, so the row would stay for ever.
        For each such row:

        - If DBOS cancelled the workflow, or has no record of it, record
          ``cancelled``.
        - If DBOS still runs the workflow, the crash fell between the row write
          and the DBOS cancel. Ask DBOS to cancel, then record ``cancelled``.
        - If the workflow finished, leave the row and log it.

        The restart lost the process map, so this repair cannot stop a shell
        command that the old daemon started.
        """
        try:
            rows = await self._store().list_runs_by_status(RUN_CANCEL_REQUESTED)
        except Exception:
            logger.exception("could not list the runs that wait for a cancel")
            return
        for row in rows:
            try:
                workflow_status = await DBOS.get_workflow_status_async(row.id)
                status = workflow_status.status if workflow_status is not None else None
                if status in {"SUCCESS", "ERROR"}:
                    logger.warning("run %s says cancel_requested but its workflow ended with %s", row.id, status)
                    continue
                if status != "CANCELLED" and status is not None:
                    await DBOS.cancel_workflow_async(row.id)
                reason = row.error.get("reason") if row.error is not None else None
                await self._finish_cancelled(row.id, reason=reason or "workflow_cancelled")
                logger.info("settled run %s after a restart: cancel_requested -> cancelled", row.id)
            except Exception:
                logger.exception("could not settle run %s after a restart", row.id)

    # -- helpers ---------------------------------------------------------------

    def _store(self) -> Store:
        if self.store is None:
            raise _HttpError(503, "the daemon is not started")
        return self.store

    async def _capabilities(self):
        if self.router_url is None:
            from inferference import resolve_router_url

            self.router_url = resolve_router_url()
        client = InferferenceClient(self.router_url, api_key=router_api_key(self.config.router_key_env.value))
        try:
            return await client.capabilities()
        finally:
            await client.aclose()


def _result_frame(run: Run) -> StreamEnvelope:
    """Build the final ``turn_result`` frame from a ``completed`` run row."""
    return StreamEnvelope(
        run_id=run.id,
        request_ordinal=-1,
        event_ordinal=-1,
        kind="turn_result",
        event=TurnResponse(
            run_id=run.id,
            thread_id=run.thread_id,
            output=run.output_json,
            output_type=run.output_type,
            messages=run.messages_json or [],
            usage=run.usage,
            run_metadata=run.run_metadata,
        ).model_dump(mode="json"),
        final=True,
    )


async def _drain(queue: asyncio.Queue[StreamEnvelope | None], send: Any) -> None:
    while True:
        envelope = await queue.get()
        if envelope is None:
            return
        await _send_sse(send, envelope)


def _settle(future: asyncio.Future[list[int]], task: asyncio.Task[list[int]]) -> None:
    """Copy the result of a stop task to the future that the waiters share."""
    if future.done():
        return
    if task.cancelled():
        future.set_result([])
    elif task.exception() is not None:
        future.set_result([])
    else:
        future.set_result(task.result())


class _HttpError(Exception):
    def __init__(self, status: int, message: str, detail: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.detail = detail


def _parse_body[M: RequestModel](model: type[M], body: bytes) -> M:
    """Validate a request body. Raise a 422 error for an unknown field, a wrong type, or bad JSON."""
    try:
        return model.model_validate_json(body)
    except ValidationError as error:
        problems = error.errors(include_url=False, include_input=False, include_context=False)
        detail = "; ".join(
            f"{'.'.join(str(part) for part in item['loc']) or 'body'}: {item['msg']}" for item in problems
        )
        raise _HttpError(422, "invalid request body", detail) from error


async def _read_body(receive: Any) -> bytes:
    body = b""
    while True:
        event = await receive()
        body += event.get("body", b"")
        if not event.get("more_body", False):
            return body


async def _wait_disconnect(receive: Any) -> None:
    """Wait until the ASGI client closes the request stream."""
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


async def _send_json(send: Any, status: int, payload: dict[str, Any]) -> None:
    data = json.dumps(payload).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(data)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": data})


async def _send_sse(send: Any, envelope: StreamEnvelope) -> None:
    frame = f"event: {envelope.kind}\ndata: {envelope.model_dump_json()}\n\n".encode()
    await send({"type": "http.response.body", "body": frame, "more_body": True})


def _query(raw: bytes) -> dict[str, str]:
    if not raw:
        return {}
    from urllib.parse import parse_qs

    parsed = parse_qs(raw.decode())
    return {key: values[-1] for key, values in parsed.items() if values}


def _required(query: dict[str, str], key: str) -> str:
    if key not in query:
        raise _HttpError(400, f"missing query parameter {key!r}")
    return query[key]


def build_app(config: Config) -> AgentmanApp:
    """Build the ASGI application for ``agentman serve``."""
    return AgentmanApp(config)


def migrate_database(config: Config) -> None:
    """Apply packaged Alembic migrations at the machine boundary.

    The daemon never creates tables during request handling. Machine startup
    invokes this explicit command before socket requests can reach the daemon.
    """
    database_url = config.database_url.value
    if not database_url:
        raise RuntimeError("database_url is not configured; set AGENTMAN_DATABASE_URL")

    from importlib.resources import files
    from pathlib import Path

    from alembic import command
    from alembic.config import Config as AlembicConfig

    try:
        migration_location = files("migrations")
    except ModuleNotFoundError:
        # The source devenv exposes ``src`` on PYTHONPATH, while the wheel
        # packages the top-level migration directory. Keep both execution
        # paths explicit; the machine path uses the packaged resource above.
        migration_location = Path(__file__).resolve().parents[2] / "migrations"

    alembic_config = AlembicConfig()
    alembic_config.set_main_option("script_location", str(migration_location))
    # ConfigParser treats percent signs as interpolation markers. The machine
    # URL normally uses peer auth, but preserve arbitrary password-bearing URLs
    # for explicit operator migrations too.
    alembic_config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    command.upgrade(alembic_config, "head")


def systemd_listen_fd(*, required: bool = False) -> int | None:
    """Return systemd's one listening fd, or ``None`` for normal serving."""
    raw_count = os.environ.get("LISTEN_FDS")
    if raw_count is None:
        if required:
            raise RuntimeError("--systemd-socket requires one LISTEN_FDS descriptor from systemd")
        return None
    try:
        count = int(raw_count)
    except ValueError as error:
        raise RuntimeError(f"invalid LISTEN_FDS value {raw_count!r}") from error
    if count != 1:
        raise RuntimeError(f"expected exactly one systemd listening descriptor, got {count}")
    raw_pid = os.environ.get("LISTEN_PID")
    if raw_pid is not None:
        try:
            pid = int(raw_pid)
        except ValueError as error:
            raise RuntimeError(f"invalid LISTEN_PID value {raw_pid!r}") from error
        if pid != os.getpid():
            raise RuntimeError(f"systemd listening descriptor belongs to pid {pid}, not {os.getpid()}")
    return 3


def serve(config: Config, *, systemd_socket: bool = False) -> None:
    """Run the daemon on a systemd fd or the normal Unix socket path."""
    import uvicorn

    from agentman.config import default_socket_path

    listen_fd = systemd_listen_fd(required=systemd_socket)
    app = build_app(config)
    if listen_fd is not None:
        server_config = uvicorn.Config(app, fd=listen_fd, log_level="info")
    else:
        socket_path = config.socket.value or str(default_socket_path())
        server_config = uvicorn.Config(app, uds=socket_path, log_level="info")
    server = uvicorn.Server(server_config)
    server.run()


__all__ = [
    "AgentmanApp",
    "SubscriberRegistry",
    "build_app",
    "make_event_handler",
    "migrate_database",
    "serve",
    "systemd_listen_fd",
]
