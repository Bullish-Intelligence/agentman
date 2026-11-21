"""A cancel stops the shell processes of the run.

These tests run real shell commands through the harness shell tool. The model is
a ``FunctionModel``. No test waits for a full ``sleep 30``: a cancel ends it.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore
from test_daemon_contracts import (
    _app,
    _CancelStore,
    _dbos,
    _frames_after_run_started,
    _run,
    _turn_request,
    _wait_forever_receive,
)

import agentman.daemon as daemon
import agentman.processes as processes
from agentman.daemon import AgentmanApp, make_event_handler
from agentman.persistence.store import RUN_ACTIVE
from agentman.processes import ToolProcesses, group_alive, stop_process_groups
from agentman.protocol import StreamEnvelope
from agentman.runtime import Turn, run_turn

RUN_ID = "run-1"
GRACE = 0.3
KILL_WAIT = 2.0


async def _assert_gone(pgid: int) -> None:
    """Assert that the group has no running member.

    ``killpg`` must find no member, or find only zombies. A zombie has exited. In
    a sandbox its parent can leave it unread for a long time.
    """
    deadline = time.monotonic() + 2
    while True:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        if processes._has_running_member(pgid) is False:
            return
        assert time.monotonic() < deadline, f"process group {pgid} still has a running member"
        await asyncio.sleep(0.02)


@dataclass
class _Rig:
    """A Coder turn with a real shell tool. The second model step waits for ``gate``."""

    app: AgentmanApp
    workspace: Path
    task: asyncio.Task[Any]
    queue: asyncio.Queue[StreamEnvelope | None]
    gate: asyncio.Event
    step_store: InMemoryStepStore
    events: list[dict[str, Any]] = field(default_factory=list)

    async def wait_for(self, kind: str) -> dict[str, Any]:
        """Return the first shell event of ``kind``. Read the queue until it shows."""
        while True:
            for event in self.events:
                if event["kind"] == kind:
                    return event
            item = await asyncio.wait_for(self.queue.get(), timeout=10)
            if item is not None and item.kind == "capability":
                self.events.append(item.event)

    async def finish(self) -> Any:
        self.gate.set()
        return await asyncio.wait_for(self.task, timeout=10)


async def _start(tmp_path: Path, command: str, **tool_args: Any) -> _Rig:
    app = _app()
    app.tool_stop_grace = GRACE
    app.tool_kill_wait = KILL_WAIT
    app.step_store = cast(Any, InMemoryStepStore())
    queue = app.registry.subscribe(RUN_ID)
    gate = asyncio.Event()
    calls = {"n": 0}

    async def stream_fn(messages: Any, info: Any) -> AsyncIterator[Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            args = {"command": command, **tool_args}
            yield {0: DeltaToolCall(name="shell", json_args=json.dumps(args), tool_call_id="call-1")}
        else:
            await gate.wait()
            yield "Done."

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    product = FakeProductStore()
    thread = await product.create_thread(workspace=str(workspace), name="t")
    turn = Turn(
        workspace=str(workspace),
        thread_id=thread.id,
        agent_spec={"instructions": "x"},
        agent_name="probe",
        model="test",
        prompt="go",
        tools="coder",
    )
    task = asyncio.create_task(
        run_turn(
            turn,
            step_store=cast(Any, app.step_store),
            product_store=product,
            run_id=RUN_ID,
            event_stream_handler=make_event_handler(app.registry, app.tools),
            model=FunctionModel(stream_function=stream_fn),
        )
    )
    return _Rig(app, workspace, task, queue, gate, cast(InMemoryStepStore, app.step_store))


async def _wait_until(predicate: Any, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "the condition did not hold in time"
        await asyncio.sleep(0.02)


async def test_cancel_stops_a_running_command_before_it_writes(tmp_path: Path) -> None:
    rig = await _start(tmp_path, "sleep 30; echo late >> note.md")
    started = await rig.wait_for("shell.command_started")
    pid = started["pid"]
    assert rig.app.tools.snapshot(RUN_ID) == {"call-1": pid}

    began = time.monotonic()
    survivors = await rig.app._stop_tool_processes(RUN_ID)
    assert survivors == []
    assert time.monotonic() - began < GRACE + KILL_WAIT
    await _assert_gone(pid)

    await rig.finish()
    await asyncio.sleep(GRACE)
    note = rig.workspace / "note.md"
    assert not note.exists() or "late" not in note.read_text()
    assert rig.app.tools.snapshot(RUN_ID) == {}


async def test_cancel_stops_the_children_of_a_command(tmp_path: Path) -> None:
    rig = await _start(tmp_path, "sh -c 'sleep 30 & wait'; echo late >> note.md")
    pid = (await rig.wait_for("shell.command_started"))["pid"]
    # Give the inner shell time to start its child.
    await asyncio.sleep(0.3)
    assert group_alive(pid)

    assert await rig.app._stop_tool_processes(RUN_ID) == []
    await _assert_gone(pid)
    await rig.finish()
    assert not (rig.workspace / "note.md").exists()


@pytest.mark.parametrize("tool_args", [{"mode": "background"}, {"timeout": 0.5}], ids=["background", "timeout"])
async def test_a_command_that_the_tool_stopped_waiting_for_is_still_stopped(
    tmp_path: Path, tool_args: dict[str, Any]
) -> None:
    rig = await _start(tmp_path, "sleep 30; echo late >> note.md", **tool_args)
    pid = (await rig.wait_for("shell.command_started"))["pid"]
    finished = await rig.wait_for("shell.command_finished")
    # The tool stopped waiting. The command did not exit.
    assert finished["exit_code"] is None
    assert rig.app.tools.snapshot(RUN_ID) == {"call-1": pid}
    assert group_alive(pid)

    assert await rig.app._stop_tool_processes(RUN_ID) == []
    await _assert_gone(pid)
    await rig.finish()
    assert not (rig.workspace / "note.md").exists()


async def test_a_finished_command_is_never_signaled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rig = await _start(tmp_path, "echo hi")
    finished = await rig.wait_for("shell.command_finished")
    assert finished["exit_code"] == 0
    assert rig.app.tools.snapshot(RUN_ID) == {}

    calls: list[tuple[int, int]] = []

    def spy(pgid: int, signum: int) -> None:
        calls.append((pgid, signum))

    monkeypatch.setattr(os, "killpg", spy)
    assert await rig.app._stop_tool_processes(RUN_ID) == []
    assert calls == []
    monkeypatch.undo()
    await rig.finish()


async def test_a_group_with_only_zombies_counts_as_gone() -> None:
    """A zombie has exited. It stays until the parent reads its status, so the group looks alive to killpg."""
    import subprocess

    process = subprocess.Popen(["sh", "-c", "exit 0"], start_new_session=True)
    try:
        os.killpg(process.pid, 0)
        await _wait_until(lambda: not processes._has_running_member(process.pid))
        os.killpg(process.pid, 0)  # the zombie still holds the group
        assert group_alive(process.pid) is False
        assert await stop_process_groups([process.pid], grace=0.1, kill_wait=0.1) == []
    finally:
        process.wait()
    assert group_alive(process.pid) is False


async def test_a_group_that_is_gone_gets_no_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A tracked pid whose group has gone is not signaled, so a reused pid is safe."""
    calls: list[int] = []

    def spy(pgid: int, signum: int) -> None:
        calls.append(signum)
        raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", spy)
    assert await stop_process_groups([424242], grace=0.1, kill_wait=0.1) == []
    assert calls == [0], "only the existence check ran"


async def test_a_command_that_ignores_sigterm_gets_sigkill_after_the_grace_time(tmp_path: Path) -> None:
    rig = await _start(tmp_path, "trap '' TERM; sleep 30; echo late >> note.md")
    pid = (await rig.wait_for("shell.command_started"))["pid"]
    await asyncio.sleep(0.2)

    began = time.monotonic()
    survivors = await rig.app._stop_tool_processes(RUN_ID)
    took = time.monotonic() - began
    assert survivors == []
    assert took >= GRACE, "the command ignored SIGTERM, so the stop waited for the grace time"
    await _assert_gone(pid)
    await rig.finish()
    assert not (rig.workspace / "note.md").exists()


async def test_the_effect_record_of_a_stopped_command_is_completed(tmp_path: Path) -> None:
    rig = await _start(tmp_path, "sleep 30")
    await rig.wait_for("shell.command_started")
    before = await rig.step_store.get_tool_effect(run_id=RUN_ID, tool_call_id="call-1")
    assert before is not None and before.status == "started"

    assert await rig.app._stop_tool_processes(RUN_ID) == []
    after = await rig.step_store.get_tool_effect(run_id=RUN_ID, tool_call_id="call-1")
    assert after is not None
    assert (after.status, after.tool_name) == ("failed", "shell")
    assert after.ended_at is not None and after.started_at == before.started_at
    assert after.effect_summary == "stopped by cancel"
    assert await rig.step_store.list_unresolved_tool_effects(run_id=RUN_ID) == []
    await rig.finish()


# -- the tracker ---------------------------------------------------------------


def _event(kind: str, **fields: Any) -> SimpleNamespace:
    return SimpleNamespace(kind=kind, tool_call_id="call-1", **fields)


def test_the_tracker_drops_a_command_only_when_it_has_an_exit_code() -> None:
    tools = ToolProcesses()
    tools.observe(RUN_ID, _event("shell.command_started", pid=11, command="x"))
    assert tools.snapshot(RUN_ID) == {"call-1": 11}
    tools.observe(RUN_ID, _event("shell.command_finished", pid=11, exit_code=None))
    assert tools.snapshot(RUN_ID) == {"call-1": 11}
    tools.observe(RUN_ID, _event("shell.command_finished", pid=11, exit_code=0))
    assert tools.snapshot(RUN_ID) == {}


def test_the_tracker_ignores_other_events_and_other_runs() -> None:
    tools = ToolProcesses()
    tools.observe(RUN_ID, SimpleNamespace(kind="shell.command_output", text="x"))
    tools.observe(RUN_ID, SimpleNamespace(event_kind="part_start"))
    tools.observe(RUN_ID, _event("shell.command_started", pid=11, command="x"))
    assert tools.snapshot("other") == {}
    tools.discard(RUN_ID)
    assert tools.snapshot(RUN_ID) == {}


# -- the daemon contract -------------------------------------------------------


async def _spawn_stubborn_process(app: AgentmanApp) -> int:
    """Start a process group that ignores SIGTERM, and track it as a run command."""
    process = await asyncio.create_subprocess_exec(
        "sh", "-c", "trap '' TERM; sleep 30", start_new_session=True, stdout=asyncio.subprocess.DEVNULL
    )
    reaper = asyncio.create_task(process.wait())
    reaper.add_done_callback(lambda _: None)
    app.tools.add(RUN_ID, "call-1", process.pid)
    await asyncio.sleep(0.2)
    assert group_alive(process.pid)
    return process.pid


async def _get_status(app: AgentmanApp) -> str:
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app._route("GET", f"/v1/runs/{RUN_ID}", {}, b"", _wait_forever_receive, send)
    return json.loads(sent[-1]["body"])["status"]


async def test_the_run_reads_cancel_requested_until_the_processes_are_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.tool_stop_grace = 0.5
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, InMemoryStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))
    pid = await _spawn_stubborn_process(app)

    cancel = asyncio.create_task(app.cancel_run(RUN_ID, workspace="/repo", reason="cancel_requested"))
    await asyncio.sleep(0.05)
    seen = [await _get_status(app)]
    while not cancel.done():
        seen.append(await _get_status(app))
        await asyncio.sleep(0.05)
    result = await cancel
    seen.append(await _get_status(app))

    assert result == {"run_id": RUN_ID, "status": "cancelled", "cancelled": True}
    assert seen[0] == "cancel_requested"
    assert set(seen[:-1]) == {"cancel_requested"}
    assert seen[-1] == "cancelled"
    assert store.statuses == ["cancel_requested"]
    assert store.run is not None and store.run.error is not None
    assert store.run.error["reason"] == "cancel_requested"
    await _assert_gone(pid)


async def test_a_group_that_survives_sigkill_leaves_the_run_cancel_requested(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app()
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, InMemoryStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))
    app.tools.add(RUN_ID, "call-1", 4242)

    async def stubborn(pgids: Any, *, grace: float, kill_wait: float) -> list[int]:
        return [4242]

    monkeypatch.setattr(daemon, "stop_process_groups", stubborn)
    monkeypatch.setattr(daemon, "group_alive", lambda pgid: True)

    result = await app.cancel_run(RUN_ID, workspace="/repo", reason="cancel_requested")
    assert result == {"run_id": RUN_ID, "status": "cancel_requested", "cancelled": False}
    assert store.run is not None and store.run.status == "cancel_requested"
    assert "survived SIGKILL" in caplog.text
    assert app.tools.snapshot(RUN_ID) == {"call-1": 4242}

    # A second cancel tries again.
    async def gone(pgids: Any, *, grace: float, kill_wait: float) -> list[int]:
        return []

    monkeypatch.setattr(daemon, "stop_process_groups", gone)
    result = await app.cancel_run(RUN_ID, workspace="/repo", reason="cancel_requested")
    assert result == {"run_id": RUN_ID, "status": "cancelled", "cancelled": True}


async def test_a_refused_dbos_cancel_moves_the_run_back_to_running(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[], cancel_error=RuntimeError("DBOS is down")))
    with pytest.raises(RuntimeError, match="DBOS is down"):
        await app.cancel_run(RUN_ID, workspace="/repo", reason="cancel_requested")
    assert store.run is not None and store.run.status == "running" and store.run.error is None
    assert store.statuses == ["cancel_requested", "running"]


async def test_the_final_frame_waits_for_the_processes_and_lists_none(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.tool_stop_grace = 0.3
    store = _CancelStore(_run("/repo"), hidden=True)
    app.store = cast(Any, store)
    app.step_store = cast(Any, InMemoryStepStore())
    cancel_done = asyncio.Event()
    state: dict[str, Any] = {}

    class _Handle:
        def get_workflow_id(self) -> str:
            return RUN_ID

        async def get_result(self) -> object:
            await state["dbos_cancelled"].wait()
            # The workflow unwinds while a process still ignores SIGTERM.
            raise daemon.DBOSAwaitedWorkflowCancelledError(RUN_ID)

    class _DBOS:
        @staticmethod
        async def get_workflow_status_async(run_id: str) -> SimpleNamespace | None:
            return SimpleNamespace(status="CANCELLED") if state.get("started") else None

        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            store.reveal()
            state["started"] = True
            state["dbos_cancelled"] = asyncio.Event()
            state["pid"] = await _spawn_stubborn_process(app)
            state["cancel"] = asyncio.create_task(_cancel_and_note())
            return _Handle()

        @staticmethod
        async def cancel_workflow_async(run_id: str) -> None:
            state["dbos_cancelled"].set()

    async def _cancel_and_note() -> None:
        state["result"] = await app.cancel_run(RUN_ID, workspace="/repo", reason="cancel_requested")
        cancel_done.set()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body" and message.get("body"):
            # A frame must not leave while a tool process still lives.
            if '"final":true' in message["body"].decode():
                state["alive_at_final"] = group_alive(state["pid"])
        sent.append(message)

    await app._handle_turn(
        _turn_request(model="mock:model", run_id=RUN_ID).model_dump_json().encode(), _wait_forever_receive, send
    )
    await cancel_done.wait()

    frames = _frames_after_run_started(sent)
    assert len(frames) == 1
    payload = json.loads(frames[0].split("data: ", 1)[1])
    assert payload["final"] is True and payload["kind"] == "turn_error"
    assert payload["event"]["error"] == "turn cancelled"
    assert payload["event"]["live_tool_processes"] == []
    assert state["alive_at_final"] is False
    assert state["result"]["cancelled"] is True
    assert app.tools.snapshot(RUN_ID) == {}, "the daemon forgets the run when its stream ends"


async def test_the_final_frame_lists_a_process_that_a_cancel_did_not_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A workflow that DBOS cancelled outside the daemon has no stop. The frame names the live groups."""
    app = _app()
    store = _CancelStore(_run("/repo"), hidden=True)
    app.store = cast(Any, store)
    pid_holder: dict[str, int] = {}

    class _Handle:
        def get_workflow_id(self) -> str:
            return RUN_ID

        async def get_result(self) -> object:
            raise daemon.DBOSAwaitedWorkflowCancelledError(RUN_ID)

    class _DBOS:
        @staticmethod
        async def get_workflow_status_async(run_id: str) -> None:
            return None

        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            store.reveal()
            pid_holder["pid"] = await _spawn_stubborn_process(app)
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    try:
        await app._handle_turn(
            _turn_request(model="mock:model", run_id=RUN_ID).model_dump_json().encode(), _wait_forever_receive, send
        )
    finally:
        os.killpg(pid_holder["pid"], signal.SIGKILL)
    payload = json.loads(_frames_after_run_started(sent)[0].split("data: ", 1)[1])
    assert payload["event"]["live_tool_processes"] == [pid_holder["pid"]]


async def test_a_disconnect_stops_the_tool_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.tool_stop_grace = 0.3
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, InMemoryStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))
    pid = await _spawn_stubborn_process(app)

    assert await app._cancel_after_disconnect(RUN_ID, workspace="/repo") is True
    await _assert_gone(pid)
    assert store.statuses == ["cancel_requested"]
    assert store.run is not None and store.run.status == "cancelled"
    assert store.run.error is not None and store.run.error["reason"] == "client_disconnected"


# -- recovery after a restart --------------------------------------------------


@pytest.mark.parametrize("workflow_status", ["CANCELLED", None, "PENDING"])
async def test_startup_settles_a_cancel_requested_row(
    monkeypatch: pytest.MonkeyPatch, workflow_status: str | None
) -> None:
    app = _app()
    stuck = replace(
        _run("/repo", status="cancel_requested"),
        error={"reason": "cancel_requested", "type": None, "message": None},
    )
    store = _CancelStore(stuck)
    app.store = cast(Any, store)
    app.step_store = cast(Any, InMemoryStepStore())
    cancelled: list[str] = []
    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status=workflow_status, cancelled=cancelled))

    await app._repair_cancel_requests()
    assert store.run is not None and store.run.status == "cancelled"
    assert store.run.error is not None and store.run.error["reason"] == "cancel_requested"
    assert cancelled == (["run-1"] if workflow_status == "PENDING" else [])


async def test_startup_leaves_a_row_whose_workflow_finished(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app()
    store = _CancelStore(_run("/repo", status="cancel_requested"))
    app.store = cast(Any, store)
    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status="SUCCESS", cancelled=[]))
    await app._repair_cancel_requests()
    assert store.run is not None and store.run.status == "cancel_requested"
    assert "ended with SUCCESS" in caplog.text


async def test_resume_refuses_a_run_with_a_cancel_in_progress(tmp_path: Path) -> None:
    from agentman.recovery import ResumeRefused, authorize_resume

    class _Journal:
        async def recovery(self, *, run_id: str) -> Any:
            raise AssertionError("the refusal comes first")

    store = _CancelStore(_run(str(tmp_path), status="cancel_requested"))
    with pytest.raises(ResumeRefused, match="a cancel is in progress"):
        await authorize_resume(store=store, journal=_Journal(), run_id="run-1")


def test_the_active_states_hold_running_and_cancel_requested() -> None:
    assert RUN_ACTIVE == {"running", "cancel_requested"}
    assert processes.TOOL_STOP_GRACE_SECONDS == 2.0
