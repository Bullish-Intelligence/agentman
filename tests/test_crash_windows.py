"""Process-isolated evidence for the four tool-effect crash windows."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from multiprocessing import get_context
from pathlib import Path
from queue import Empty
from typing import Any, Protocol

import pytest
from pydantic_ai.messages import ModelMessage, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, FunctionModel
from pydantic_ai_harness.filesystem._toolset import FileSystemToolset
from pydantic_ai_harness.step_persistence import StepPersistence

from agentman.persistence.engine import create_engine
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import Store
from agentman.runtime import Turn, configure_runtime, execute

pytestmark = pytest.mark.integration

_CRASH_STATE: dict[str, Any] = {}


class _ResultQueue(Protocol):
    def put(self, item: dict[str, Any]) -> None: ...


def _block_then_exit() -> None:
    ready = Path(_CRASH_STATE["ready"])
    ready.touch()
    _CRASH_STATE["queue"].put({"ready": True, "run_id": _CRASH_STATE["run_id"]})
    kill = Path(_CRASH_STATE["kill"])
    while not kill.exists():
        time.sleep(0.01)
    os._exit(91)


async def _tool_model_stream(messages: list[ModelMessage], info: AgentInfo):
    if any(isinstance(part, ToolReturnPart) for message in messages for part in message.parts):
        yield "done"
        return
    yield {
        0: DeltaToolCall(
            name="write_file",
            json_args=json.dumps({"path": "window.txt", "content": "hello"}),
            tool_call_id="call-window",
        )
    }


def _build_crash_model(
    model_string: str,
    *,
    router_url: str | None,
    n_ctx: int | None,
    max_transport_retries: int | None = None,
) -> FunctionModel:
    return FunctionModel(stream_function=_tool_model_stream, model_name="crash-window")


def _install_crash_hook(window: str) -> None:
    if window == "before_record":
        original = StepPersistence.before_tool_execute

        async def before(self: Any, *args: Any, **kwargs: Any) -> Any:
            _block_then_exit()
            return await original(self, *args, **kwargs)

        StepPersistence.before_tool_execute = before  # type: ignore[method-assign]
    elif window == "before_mutation":
        original = FileSystemToolset._write_file

        async def write(self: Any, *args: Any, **kwargs: Any) -> Any:
            _block_then_exit()
            return await original(self, *args, **kwargs)

        FileSystemToolset._write_file = write  # type: ignore[method-assign]
    elif window == "after_mutation":
        original = StepPersistence.after_tool_execute

        async def after(self: Any, *args: Any, **kwargs: Any) -> Any:
            _block_then_exit()
            return await original(self, *args, **kwargs)

        StepPersistence.after_tool_execute = after  # type: ignore[method-assign]
    elif window == "after_completion":
        original = StepPersistence.after_tool_execute

        async def after(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = await original(self, *args, **kwargs)
            _block_then_exit()
            return result

        StepPersistence.after_tool_execute = after  # type: ignore[method-assign]
    else:  # pragma: no cover - pytest parameterization owns the values
        raise AssertionError(f"unknown crash window {window}")


def _child(
    window: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    ready_path: str,
    kill_path: str,
    result_queue: _ResultQueue,
) -> None:
    try:
        asyncio.run(_child_async(window, database_url, workspace, thread_id, ready_path, kill_path, result_queue))
    except Exception as error:  # pragma: no cover - surfaced in the parent assertion
        import traceback

        result_queue.put({"error": repr(error), "trace": traceback.format_exc()})


def _resume_child(
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str,
    result_queue: _ResultQueue,
) -> None:
    try:
        asyncio.run(_resume_child_async(database_url, workspace, thread_id, run_id, result_queue))
    except Exception as error:  # pragma: no cover - surfaced in the parent assertion
        import traceback

        result_queue.put({"error": repr(error), "trace": traceback.format_exc()})


async def _child_async(
    window: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    ready_path: str,
    kill_path: str,
    result_queue: _ResultQueue,
) -> None:
    from dbos import DBOS, DBOSConfig

    import agentman.runtime as runtime
    from agentman.persistence.step_store import PostgresStepStore

    _CRASH_STATE.update(ready=ready_path, kill=kill_path, queue=result_queue, run_id=None)
    _install_crash_hook(window)
    runtime.build_model = _build_crash_model  # ty: ignore[invalid-assignment]
    engine = create_engine(database_url)
    store = Store(engine)
    step_store = PostgresStepStore(engine)
    configure_runtime(step_store=step_store, product_store=store)
    DBOS(
        config=DBOSConfig(
            name="agentman",
            system_database_url=database_url,
            dbos_system_schema="dbos",
        )
    )
    DBOS.launch()
    turn = Turn(
        workspace=workspace,
        thread_id=thread_id,
        agent_spec={"instructions": "write the file"},
        agent_name="crash-window",
        model="mock:crash-window",
        prompt="write the file",
        tools="coder",
    )
    handle = await DBOS.start_workflow_async(execute, turn)
    _CRASH_STATE["run_id"] = handle.get_workflow_id()
    await asyncio.Event().wait()


async def _resume_child_async(
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str,
    result_queue: _ResultQueue,
) -> None:
    from dbos import DBOS, DBOSConfig

    import agentman.runtime as runtime
    from agentman.persistence.inspect import Journal
    from agentman.persistence.step_store import PostgresStepStore
    from agentman.recovery import authorize_resume

    engine = create_engine(database_url)
    store = Store(engine)
    step_store = PostgresStepStore(engine)
    configure_runtime(step_store=step_store, product_store=store)
    runtime.build_model = _build_crash_model  # ty: ignore[invalid-assignment]
    DBOS(
        config=DBOSConfig(
            name="agentman",
            system_database_url=database_url,
            dbos_system_schema="dbos",
        )
    )
    DBOS.launch()
    try:
        await authorize_resume(store=store, journal=Journal(engine), run_id=run_id)
        handle = await DBOS.resume_workflow_async(run_id)
        await handle.get_result()
        runs = await store.list_runs(thread_id=thread_id)
        result_queue.put({"ok": True, "status": runs[0].status, "count": len(runs)})
    finally:
        await engine.dispose()
        DBOS.destroy()


async def _create_thread(database_url: str, workspace: str) -> str:
    engine = create_engine(database_url)
    try:
        thread = await Store(engine).create_thread(workspace=workspace, name="crash-window")
        return thread.id
    finally:
        await engine.dispose()


async def _inspect(database_url: str, thread_id: str, run_id: str) -> tuple[list[Any], Any]:
    engine = create_engine(database_url)
    try:
        store = Store(engine)
        step_store = PostgresStepStore(engine)
        return await store.list_runs(thread_id=thread_id), await step_store.get_tool_effect(
            run_id=run_id, tool_call_id="call-window"
        )
    finally:
        await engine.dispose()


def _spawn(
    window: str,
    database_url: str,
    workspace: str,
    thread_id: str,
    ready_path: Path,
    kill_path: Path,
) -> tuple[Any, Any]:
    context = get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_child,
        args=(window, database_url, workspace, thread_id, str(ready_path), str(kill_path), result_queue),
    )
    process.start()
    return process, result_queue


def _spawn_resumer(
    database_url: str,
    workspace: str,
    thread_id: str,
    run_id: str,
) -> tuple[Any, Any]:
    context = get_context("spawn")
    result_queue = context.Queue()
    process = context.Process(
        target=_resume_child,
        args=(database_url, workspace, thread_id, run_id, result_queue),
    )
    process.start()
    return process, result_queue


def _await_ready(queue: Any, timeout: float) -> dict[str, Any]:
    try:
        return queue.get(timeout=timeout)
    except Empty as error:
        raise AssertionError("crash child reached no synchronized hook") from error


@pytest.mark.parametrize("window", ["before_record", "before_mutation", "after_mutation", "after_completion"])
def test_tool_crash_window_is_recorded_by_evidence(
    database_url: str,
    tmp_path: Path,
    window: str,
) -> None:
    workspace = str(tmp_path)
    thread_id = asyncio.run(_create_thread(database_url, workspace))
    ready_path = tmp_path / "child-ready"
    kill_path = tmp_path / "parent-kill"
    child, result_queue = _spawn(window, database_url, workspace, thread_id, ready_path, kill_path)
    try:
        ready = _await_ready(result_queue, 30)
        assert ready.get("ready") is True, ready
        run_id = ready["run_id"]
        kill_path.touch()
        child.join(timeout=30)
        assert child.exitcode == 91, f"crash child exited {child.exitcode}"
    finally:
        if child.is_alive():
            child.kill()
            child.join(timeout=10)

    runs, effect = asyncio.run(_inspect(database_url, thread_id, run_id))
    assert len(runs) == 1
    target = tmp_path / "window.txt"
    if window == "before_record":
        assert effect is None
        assert not target.exists()
    else:
        assert effect is not None and effect.status == ("completed" if window == "after_completion" else "started")
        assert effect.effect_summary is not None
        summary = json.loads(effect.effect_summary)
        assert summary["pre_image"] == "absent"
        if window == "before_mutation":
            assert "post_image" not in summary
            assert not target.exists()
        else:
            assert target.read_text() == "hello"
            assert summary["post_image"] == hashlib.sha256(b"hello").hexdigest()

    resumer, resume_queue = _spawn_resumer(database_url, workspace, thread_id, run_id)
    resumer.join(timeout=60)
    assert resumer.exitcode == 0, f"resume child exited {resumer.exitcode}"
    resumed = _await_ready(resume_queue, 5)
    assert "error" not in resumed, resumed.get("trace", resumed.get("error"))
    assert resumed.get("ok") is True
    assert resumed.get("status") == "completed"
    runs, _ = asyncio.run(_inspect(database_url, thread_id, run_id))
    assert len(runs) == 1
    assert runs[0].status == "completed"
