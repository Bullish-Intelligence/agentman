"""DBOS cancellation and recovery tests against Postgres."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import threading
import time
from pathlib import Path
from queue import Empty
from typing import Any

import httpx
import pytest
import uvicorn
from dbos import DBOS, DBOSConfig
from dbos._error import DBOSAwaitedWorkflowCancelledError
from support.openai_transport import build_tcp_model
from support.slow_upstream import SlowUpstream, UpstreamConnection

from agentman.config import Config, Value
from agentman.daemon import build_app
from agentman.persistence.engine import create_engine
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import RUN_COMPLETED, RUN_RUNNING, Store
from agentman.protocol import TurnRequest
from agentman.runtime import Turn, configure_runtime, execute

pytestmark = pytest.mark.integration

MODEL = "inferference:gemma"
WAIT_TIMEOUT = 20.0


def _dbos_config(database_url: str) -> DBOSConfig:
    return DBOSConfig(name="agentman", system_database_url=database_url, dbos_system_schema="dbos")


def _app_config(database_url: str, router_url: str) -> Config:
    return Config(
        agent=Value("live", "cli"),
        model=Value(None, "cli"),
        database_url=Value(database_url, "cli"),
        socket=Value("/tmp/agentman-cancel-test.sock", "cli"),
        router_url=Value(router_url, "cli"),
    )


async def _discard_events(ctx: Any, events: Any) -> None:
    async for _ in events:
        pass


def _child(
    mode: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str | None,
    router_url: str,
    result_queue: Any,
) -> None:
    try:
        asyncio.run(_child_async(mode, database_url, workspace, thread_id, run_id, router_url, result_queue))
    except Exception as error:  # pragma: no cover - reported in the parent
        import traceback

        result_queue.put({"error": repr(error), "trace": traceback.format_exc()})


async def _child_async(
    mode: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str | None,
    router_url: str,
    result_queue: Any,
) -> None:
    from agentman.runtime import execute

    engine = create_engine(database_url)
    store = Store(engine)
    configure_runtime(
        step_store=PostgresStepStore(engine),
        product_store=store,
        event_stream_handler=_discard_events,
    )
    DBOS(config=_dbos_config(database_url))
    DBOS.launch()
    try:
        if mode == "start":
            turn = Turn(
                workspace=workspace,
                thread_id=thread_id,
                agent_spec={"instructions": "continue"},
                agent_name="cancel-integration",
                model=MODEL,
                prompt="continue",
                router_url=router_url,
                n_ctx=32768,
            )
            handle = await DBOS.start_workflow_async(execute, turn)
            result_queue.put({"started": handle.get_workflow_id()})
            while True:
                await asyncio.sleep(1)

        if run_id is None:
            raise ValueError(f"mode {mode!r} needs a run id")
        if mode == "recover_cancelled":
            await asyncio.sleep(2.5)
            workflow_status = await DBOS.get_workflow_status_async(run_id)
            run = await store.get_run(run_id)
            result_queue.put(
                {
                    "dbos_status": workflow_status.status if workflow_status is not None else None,
                    "run_status": run.status if run is not None else None,
                }
            )
            return

        if mode == "recover_pending":
            deadline = time.monotonic() + WAIT_TIMEOUT
            while time.monotonic() < deadline:
                run = await store.get_run(run_id)
                if run is not None and run.status != RUN_RUNNING:
                    result_queue.put({"run_status": run.status})
                    return
                await asyncio.sleep(0.1)
            result_queue.put({"error": "recovered workflow did not finish"})
            return

        raise ValueError(f"unknown child mode {mode!r}")
    finally:
        DBOS.destroy()
        await engine.dispose()


def _spawn(
    mode: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str | None,
    router_url: str,
) -> tuple[Any, Any]:
    context = multiprocessing.get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_child,
        args=(mode, database_url, workspace, thread_id, run_id, router_url, result_queue),
    )
    process.start()
    return process, result_queue


async def _queue_result(result_queue: Any, *, timeout: float = WAIT_TIMEOUT) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return result_queue.get_nowait()
        except Empty:
            await asyncio.sleep(0.05)
    raise AssertionError("the child process returned no result")


async def _join(process: Any, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while process.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    process.join(timeout=0)


async def _cancel_run(
    database_url: str,
    workspace: str,
    upstream: SlowUpstream,
) -> tuple[str, str, float, UpstreamConnection, str | None]:
    app = build_app(_app_config(database_url, upstream.base_url))
    await app.startup()
    try:
        assert app.store is not None
        thread = await app.store.create_thread(workspace=workspace, name="cancel")
        turn = Turn(
            workspace=workspace,
            thread_id=thread.id,
            agent_spec={"instructions": "continue"},
            agent_name="cancel-integration",
            model=MODEL,
            prompt="continue",
            router_url=upstream.base_url,
            n_ctx=32768,
        )
        handle = await DBOS.start_workflow_async(execute, turn)
        run_id = handle.get_workflow_id()
        await upstream.wait_for_connections(1)
        await upstream.wait_for_chunks(20)
        cancel_time = time.monotonic()
        result = await app.cancel_run(run_id, workspace=workspace, reason="cancel_requested")
        assert result == {"run_id": run_id, "status": "cancelled", "cancelled": True}
        with pytest.raises(DBOSAwaitedWorkflowCancelledError):
            await asyncio.wait_for(handle.get_result(), timeout=5)
        connection = await upstream.wait_for_closed(timeout=2)
        workflow_status = await DBOS.get_workflow_status_async(run_id)
        status = workflow_status.status if workflow_status is not None else None
        return run_id, thread.id, cancel_time, connection, status
    finally:
        await app.shutdown()


def test_cancel_stops_the_model_step_and_closes_upstream(database_url: str, tmp_path: Path) -> None:
    asyncio.run(_cancel_stops_model_step(database_url, tmp_path))


async def _cancel_stops_model_step(database_url: str, tmp_path: Path) -> None:
    workspace = f"/repo/cancel-{time.time_ns()}"
    async with SlowUpstream() as upstream:
        run_id, _, cancel_time, connection, dbos_status = await _cancel_run(database_url, workspace, upstream)
        assert connection.closed_at is not None
        assert connection.closed_at - cancel_time < 2.0
        assert connection.chunks_sent > 0

    engine = create_engine(database_url)
    try:
        run = await Store(engine).get_run(run_id)
    finally:
        await engine.dispose()
    assert dbos_status == "CANCELLED"
    assert run is not None and run.status == "cancelled"
    assert run.error is not None and run.error["reason"] == "cancel_requested"
    assert run.usage is not None and run.usage["requests"] == 0


def test_a_cancelled_run_is_not_recovered_at_launch(database_url: str, tmp_path: Path) -> None:
    asyncio.run(_cancelled_run_stays_cancelled(database_url, tmp_path))


async def _cancelled_run_stays_cancelled(database_url: str, tmp_path: Path) -> None:
    workspace = f"/repo/cancel-recovery-{time.time_ns()}"
    async with SlowUpstream() as upstream:
        run_id, thread_id, _, connection, dbos_status = await _cancel_run(database_url, workspace, upstream)
        assert connection.closed.is_set()
        assert dbos_status == "CANCELLED"
        child, result_queue = _spawn("recover_cancelled", database_url, workspace, thread_id, run_id, upstream.base_url)
        try:
            result = await _queue_result(result_queue, timeout=WAIT_TIMEOUT)
            await _join(child, WAIT_TIMEOUT)
            assert child.exitcode == 0, f"recovery child exited {child.exitcode}"
            assert result == {"dbos_status": "CANCELLED", "run_status": "cancelled"}
            assert len(upstream.connections) == 1
        finally:
            if child.is_alive():
                child.kill()
                await _join(child, 10)


def test_a_restart_recovers_a_pending_turn(database_url: str) -> None:
    asyncio.run(_restart_recovers_pending_turn(database_url))


async def _restart_recovers_pending_turn(database_url: str) -> None:
    workspace = f"/repo/pending-recovery-{time.time_ns()}"
    engine = create_engine(database_url)
    try:
        thread = await Store(engine).create_thread(workspace=workspace, name="pending")
    finally:
        await engine.dispose()

    async with SlowUpstream(duration=1.5) as upstream:
        starter, starter_queue = _spawn("start", database_url, workspace, thread.id, None, upstream.base_url)
        try:
            await upstream.wait_for_connections(1, timeout=WAIT_TIMEOUT)
            started = await _queue_result(starter_queue, timeout=WAIT_TIMEOUT)
            assert "started" in started, started
        finally:
            if starter.is_alive():
                starter.kill()
                await _join(starter, 10)

        await upstream.wait_for_closed(0, timeout=5)
        run_id = started["started"]
        recoverer, result_queue = _spawn(
            "recover_pending", database_url, workspace, thread.id, run_id, upstream.base_url
        )
        try:
            await upstream.wait_for_connections(2, timeout=WAIT_TIMEOUT)
            result = await _queue_result(result_queue, timeout=WAIT_TIMEOUT)
            await _join(recoverer, WAIT_TIMEOUT)
            assert recoverer.exitcode == 0, f"recovery child exited {recoverer.exitcode}"
            assert result == {"run_status": RUN_COMPLETED}
            assert len(upstream.connections) >= 2
        finally:
            if recoverer.is_alive():
                recoverer.kill()
                await _join(recoverer, 10)


def test_disconnect_cancels_the_run_through_the_socket(
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_disconnect_cancels_run(database_url, tmp_path, monkeypatch))


async def _disconnect_cancels_run(
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SlowUpstream() as upstream:
        import agentman.runtime as runtime

        model = build_tcp_model(port=int(upstream.base_url.rsplit(":", 1)[1].split("/", 1)[0]))
        monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
        runtime.reset_agent_cache()
        socket_path = tmp_path / "agentman.sock"
        app = build_app(_app_config(database_url, upstream.base_url))
        server = uvicorn.Server(uvicorn.Config(app, uds=str(socket_path), log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.025)
        assert server.started, "the daemon did not start"

        engine = create_engine(database_url)
        store = Store(engine)
        try:
            transport = httpx.AsyncHTTPTransport(uds=str(socket_path))
            async with httpx.AsyncClient(base_url="http://agentman", transport=transport, timeout=15) as client:
                created = await client.post("/v1/threads", json={"workspace": "/repo/disconnect", "name": "test"})
                assert created.status_code == 200, created.text
                thread_id = created.json()["id"]
                turn = TurnRequest(
                    workspace="/repo/disconnect",
                    thread_id=thread_id,
                    agent_spec={"instructions": "continue"},
                    agent_name="disconnect-integration",
                    model="mock:model",
                    prompt="continue",
                )
                async with client.stream("POST", "/v1/turns", json=turn.model_dump(mode="json")) as response:
                    assert response.status_code == 200
                    await upstream.wait_for_connections(1, timeout=5)
                    await upstream.wait_for_chunks(2, timeout=5)
                    run_id: str | None = None
                    data_frames = 0
                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        frame = json.loads(line[len("data: ") :])
                        run_id = frame["run_id"]
                        data_frames += 1
                        if data_frames == 2:
                            break
                assert run_id is not None
                connection = await upstream.wait_for_closed(timeout=5)
                deadline = time.monotonic() + 5
                run = None
                while time.monotonic() < deadline:
                    run = await store.get_run(run_id)
                    if run is not None and run.status != RUN_RUNNING:
                        break
                    await asyncio.sleep(0.05)
                assert run is not None and run.status == "cancelled"
                assert run.error is not None and run.error["reason"] == "client_disconnected"
                assert connection.closed_at is not None
        finally:
            await engine.dispose()
            server.should_exit = True
            await _join(thread, 10)
            assert not thread.is_alive(), "the daemon did not stop"
            runtime.reset_agent_cache()
