"""Process-isolated DBOS recovery proof (check 1, finding F1/F3).

A turn runs in one spawned interpreter, is killed mid-model-request, then a
second interpreter boots DBOS and lets recovery finish it. The durable boundary
in ``agentman.runtime`` must make the replay safe: exactly one ``agentman.run``
row and one terminal status.

Set ``AGENTMAN_RUN_INTEGRATION=1`` to run it. It needs Postgres.
"""

from __future__ import annotations

import asyncio
import os
import time
from multiprocessing import get_context
from queue import Empty
from typing import Any, Protocol

import pytest

from agentman.persistence.engine import create_engine
from agentman.persistence.store import RUN_COMPLETED, Store

pytestmark = pytest.mark.integration

MODEL_DELAY_SECONDS = 2.0
RECOVERY_TIMEOUT_SECONDS = 60.0
START_TIMEOUT_SECONDS = 30.0


class _ResultQueue(Protocol):
    def put(self, item: dict[str, Any]) -> None: ...


_MODEL_START: dict[str, Any] = {"queue": None}
"""The queue the slow model signals on when its one request starts."""


def _slow_response_model() -> Any:
    """A model whose one request sleeps, so the parent can kill the turn.

    The start of the request signals the parent. Killing after that point means
    the create-run and load-history steps are recorded, and the model step is
    in flight, which is the recovery case the durable boundary must survive.
    """
    from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
    from pydantic_ai.models.function import AgentInfo, FunctionModel

    async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        queue = _MODEL_START.get("queue")
        if queue is not None:
            _MODEL_START["queue"] = None
            queue.put({"model_started": True})
        await asyncio.sleep(MODEL_DELAY_SECONDS)
        return ModelResponse(parts=[TextPart(content="recovered")])

    return FunctionModel(respond, model_name="slow-recovery")


def _patched_build_model(model_string: str, *, router_url: str | None, n_ctx: int | None) -> Any:
    """Match ``agentman.runtime.build_model`` so both interpreters build the same model."""
    return _slow_response_model()


def _child(
    mode: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    result_queue: _ResultQueue,
) -> None:
    try:
        asyncio.run(_child_async(mode, database_url, workspace, thread_id, result_queue))
    except Exception as error:  # pragma: no cover - surfaced in the parent assertion
        import traceback

        result_queue.put({"error": repr(error), "trace": traceback.format_exc()})


async def _child_async(
    mode: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    result_queue: _ResultQueue,
) -> None:
    import agentman.runtime as runtime
    from agentman.persistence.step_store import PostgresStepStore
    from agentman.runtime import Turn, configure_runtime, execute

    engine = create_engine(database_url)
    store = Store(engine)
    step_store = PostgresStepStore(engine)
    configure_runtime(step_store=step_store, product_store=store)
    # Both interpreters build the same model deterministically, so the DBOS
    # model-step name matches across the restart.
    runtime.build_model = _patched_build_model  # ty: ignore[invalid-assignment]

    from dbos import DBOS, DBOSConfig

    DBOS(
        config=DBOSConfig(
            name="agentman",
            system_database_url=database_url,
            dbos_system_schema="dbos",
        )
    )
    DBOS.launch()
    try:
        if mode == "start":
            turn = Turn(
                workspace=workspace,
                thread_id=thread_id,
                agent_spec={"instructions": "be brief"},
                agent_name="recovery",
                model="slow",
                prompt="hi",
            )
            _MODEL_START["queue"] = result_queue
            await DBOS.start_workflow_async(execute, turn)
            # Wait to be killed while the model request is in flight.
            while True:
                await asyncio.sleep(0.1)
        else:
            deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                runs = await store.list_runs(thread_id=thread_id)
                if runs and runs[0].status != "running":
                    result_queue.put({"ok": True, "status": runs[0].status, "count": len(runs)})
                    return
                await asyncio.sleep(0.2)
            runs = await store.list_runs(thread_id=thread_id)
            result_queue.put({"error": "recovery did not finish", "count": len(runs)})
    finally:
        await engine.dispose()
        DBOS.destroy()


async def _create_thread(database_url: str, workspace: str) -> str:
    engine = create_engine(database_url)
    try:
        thread = await Store(engine).create_thread(workspace=workspace, name="recovery")
    finally:
        await engine.dispose()
    return thread.id


async def _list_runs(database_url: str, thread_id: str) -> list[Any]:
    engine = create_engine(database_url)
    try:
        return await Store(engine).list_runs(thread_id=thread_id)
    finally:
        await engine.dispose()


def _spawn(mode: str, database_url: str, workspace: str, thread_id: str):
    context = get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_child, args=(mode, database_url, workspace, thread_id, queue))
    process.start()
    return process, queue


def _await_result(queue: Any, timeout: float) -> dict[str, Any]:
    try:
        return queue.get(timeout=timeout)
    except Empty as error:
        raise AssertionError("the child returned no result") from error


def test_recovered_turn_completes_exactly_once(database_url: str) -> None:
    workspace = f"/repo/recovery-{os.getpid()}-{int(time.time())}"
    thread_id = asyncio.run(_create_thread(database_url, workspace))

    starter, starter_queue = _spawn("start", database_url, workspace, thread_id)
    try:
        started = _await_result(starter_queue, START_TIMEOUT_SECONDS)
        assert started.get("model_started") is True, started
    finally:
        starter.kill()
        starter.join(timeout=10)

    recoverer, recoverer_queue = _spawn("recover", database_url, workspace, thread_id)
    recoverer.join(timeout=RECOVERY_TIMEOUT_SECONDS + 15)
    assert recoverer.exitcode == 0, f"recovery child exited {recoverer.exitcode}"
    recovered = _await_result(recoverer_queue, 5)
    assert "error" not in recovered, recovered.get("trace", recovered.get("error"))
    assert recovered.get("ok") is True

    runs = asyncio.run(_list_runs(database_url, thread_id))
    assert len(runs) == 1, f"expected exactly one run row, found {len(runs)}"
    assert runs[0].status == RUN_COMPLETED
