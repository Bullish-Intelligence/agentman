"""``agentman serve``: the transport over ``execute()``.

An ASGI app served over a Unix socket. The daemon is never a second
implementation of the turn: it calls the same :func:`agentman.runtime.execute`
that tests call directly.

It holds one piece of mutable state: a ``run_id -> subscriber`` registry.
``event_stream_handler`` fires inside the DBOS model step and publishes to it;
the matching HTTP connection drains it. When nobody is listening, events are
dropped, not buffered. The stream is presentation; the record is authoritative.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from dbos import DBOS, DBOSConfig
from dbos._error import DBOSAwaitedWorkflowCancelledError
from inferference import InferferenceClient
from pydantic import TypeAdapter
from pydantic_ai.messages import AgentStreamEvent

from agentman import __version__
from agentman.config import Config
from agentman.model import PREFIX as MODEL_PREFIX
from agentman.model import ModelResolutionError, require_schema, resolve_capability_model
from agentman.persistence.engine import create_engine
from agentman.persistence.inspect import Journal
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import RUN_CANCELLED, RUN_TERMINAL, Store
from agentman.protocol import (
    PROTOCOL_VERSION,
    CancelRequest,
    CancelResponse,
    ErrorResponse,
    NewThreadRequest,
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


class SubscriberRegistry:
    """``run_id -> subscriber`` queues. Nothing is buffered.

    The workflow can run on a worker thread while the HTTP connection drains on
    the event loop, so publishing is scheduled with ``call_soon_threadsafe``.
    A plain ``put_nowait`` from another thread would not wake the draining task.
    """

    def __init__(self) -> None:
        self._queues: dict[str, list[tuple[asyncio.Queue[StreamEnvelope | None], asyncio.AbstractEventLoop]]] = {}

    def subscribe(self, run_id: str) -> asyncio.Queue[StreamEnvelope | None]:
        queue: asyncio.Queue[StreamEnvelope | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        self._queues.setdefault(run_id, []).append((queue, loop))
        return queue

    def unsubscribe(self, run_id: str, queue: asyncio.Queue[StreamEnvelope | None]) -> None:
        subscribers = self._queues.get(run_id)
        if not subscribers:
            return
        remaining = [entry for entry in subscribers if entry[0] is not queue]
        if remaining:
            self._queues[run_id] = remaining
        else:
            self._queues.pop(run_id, None)

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


def make_event_handler(registry: SubscriberRegistry):
    """Return the ``event_stream_handler`` that stamps and publishes events.

    The request ordinal is ``ctx.run_step``. The event ordinal is this
    handler's own counter, reset per request. Neither ordinal exists on
    ``AgentStreamEvent``; the daemon stamps both.
    """

    async def handler(ctx: Any, events: Any) -> None:
        request_ordinal = ctx.run_step
        event_ordinal = 0
        async for event in events:
            envelope = StreamEnvelope(
                run_id=ctx.run_id,
                request_ordinal=request_ordinal,
                event_ordinal=event_ordinal,
                kind=getattr(event, "event_kind", type(event).__name__),
                event=_STREAM_ADAPTER.dump_python(event, mode="json"),
            )
            registry.publish(ctx.run_id, envelope)
            event_ordinal += 1

    return handler


class AgentmanApp:
    """The ASGI application."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.registry = SubscriberRegistry()
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
            event_stream_handler=make_event_handler(self.registry),
        )
        DBOS(
            config=DBOSConfig(
                name="agentman",
                system_database_url=database_url,
                dbos_system_schema="dbos",
            )
        )
        DBOS.launch()

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
            await _send_json(send, error.status, ErrorResponse(error=error.message).model_dump(mode="json"))

    async def _route(self, method: str, path: str, query: dict[str, str], body: bytes, receive: Any, send: Any) -> None:
        if method == "GET" and path == "/v1/health":
            await _send_json(
                send,
                200,
                {"status": "ok", "version": __version__, "protocol": PROTOCOL_VERSION, "capabilities": ["cancel"]},
            )
            return
        if method == "POST" and path == "/v1/turns":
            await self._handle_turn(body, receive, send)
            return
        if method == "POST" and path == "/v1/threads":
            request = NewThreadRequest.model_validate_json(body)
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
            request = ThreadRef.model_validate_json(body)
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
            request = CancelRequest.model_validate_json(body)
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
        request = TurnRequest.model_validate_json(body)
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
        )
        handle = await DBOS.start_workflow_async(execute, turn)
        run_id = handle.get_workflow_id()
        queue = self.registry.subscribe(run_id)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream"), (b"cache-control", b"no-cache")],
            }
        )
        result_task = asyncio.create_task(handle.get_result())
        drain_task = asyncio.create_task(_drain(queue, send))
        disconnect_task = asyncio.create_task(_wait_disconnect(receive))
        try:
            done, _ = await asyncio.wait(
                (result_task, disconnect_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if disconnect_task in done:
                await self.cancel_run(run_id, workspace=request.workspace, reason="client_disconnected")
                drain_task.cancel()
                await asyncio.gather(drain_task, return_exceptions=True)
                try:
                    await result_task
                except DBOSAwaitedWorkflowCancelledError:
                    pass
                return

            disconnect_task.cancel()
            await asyncio.gather(disconnect_task, return_exceptions=True)
            try:
                result = result_task.result()
            except DBOSAwaitedWorkflowCancelledError:
                run = await store.get_run(run_id)
                reason = run.error.get("reason") if run is not None and run.error is not None else "workflow_cancelled"
                self.registry.publish(run_id, None)
                await drain_task
                await _send_sse(
                    send,
                    StreamEnvelope(
                        run_id=run_id,
                        request_ordinal=-1,
                        event_ordinal=-1,
                        kind="turn_error",
                        event={"error": "turn cancelled", "reason": reason, "run_id": run_id},
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
            for task in (result_task, drain_task, disconnect_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(result_task, drain_task, disconnect_task, return_exceptions=True)
            self.registry.unsubscribe(run_id, queue)

    async def cancel_run(self, run_id: str, *, workspace: str, reason: str) -> dict[str, Any]:
        """Cancel a workflow and record the run state after DBOS accepts it."""
        store = self._store()
        run = await store.get_run(run_id)
        if run is None:
            raise _HttpError(404, f"no run {run_id!r}")
        if run.workspace != workspace:
            raise _HttpError(409, f"run {run_id!r} belongs to a different workspace")
        if run.status in RUN_TERMINAL:
            return {"run_id": run_id, "status": run.status, "cancelled": False}

        await DBOS.cancel_workflow_async(run_id)
        workflow_status = await DBOS.get_workflow_status_async(run_id)
        status = workflow_status.status if workflow_status is not None else None
        if status == "CANCELLED":
            latest = await store.get_run(run_id)
            if latest is not None and latest.status in RUN_TERMINAL:
                return {"run_id": run_id, "status": latest.status, "cancelled": False}
            usage = await usage_so_far(self.step_store, run_id) if self.step_store is not None else None
            await store.update_run(
                run_id,
                status=RUN_CANCELLED,
                usage=usage,
                error=RunError(reason=reason).model_dump(mode="json"),
            )
            logger.info("cancelled run %s with reason %s", run_id, reason)
            return {"run_id": run_id, "status": RUN_CANCELLED, "cancelled": True}
        if status in {"SUCCESS", "ERROR"}:
            latest = await store.get_run(run_id)
            return {
                "run_id": run_id,
                "status": latest.status if latest is not None else run.status,
                "cancelled": False,
            }
        raise _HttpError(409, f"run {run_id!r} could not be cancelled")

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
        request = StatusRequest.model_validate_json(body)
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
        request = ResumeRequest.model_validate_json(body)
        run = await self._store().get_run(request.run_id)
        if run is None:
            raise _HttpError(409, f"cannot resume {request.run_id}: not an agentman run")
        if run.workspace != request.workspace:
            raise _HttpError(409, f"cannot resume {request.run_id}: belongs to a different workspace")
        if self.journal is None:
            raise _HttpError(503, "the daemon is not started")
        try:
            await authorize_resume(store=self._store(), journal=self.journal, run_id=request.run_id)
            handle = await DBOS.resume_workflow_async(request.run_id)
            await handle.get_result()
        except ResumeRefused as error:
            raise _HttpError(409, str(error)) from error
        await _send_json(send, 200, ResumeResponse(run_id=request.run_id).model_dump(mode="json"))

    # -- helpers ---------------------------------------------------------------

    def _store(self) -> Store:
        if self.store is None:
            raise _HttpError(503, "the daemon is not started")
        return self.store

    async def _capabilities(self):
        if self.router_url is None:
            from inferference import resolve_router_url

            self.router_url = resolve_router_url()
        client = InferferenceClient(self.router_url)
        try:
            return await client.capabilities()
        finally:
            await client.aclose()


async def _drain(queue: asyncio.Queue[StreamEnvelope | None], send: Any) -> None:
    while True:
        envelope = await queue.get()
        if envelope is None:
            return
        await _send_sse(send, envelope)


class _HttpError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


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
