"""Event ordinals: one sequence for each run, stable across a DBOS step replay."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic_ai.messages import PartDeltaEvent, PartStartEvent, TextPart, TextPartDelta
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore

import agentman.daemon as daemon
from agentman.client import _deduplicate
from agentman.daemon import SubscriberRegistry, make_event_handler
from agentman.protocol import StreamEnvelope
from agentman.runtime import Turn, run_turn

RUN_ID = "run-1"


async def _stream_fn(state: dict[str, int], messages: Any, info: Any) -> AsyncIterator[Any]:
    """Stream text and a tool call, then a shell call, then a final text."""
    state["n"] += 1
    if state["n"] == 1:
        yield "Plan: write a note. "
        yield {
            1: DeltaToolCall(
                name="write_file", json_args=json.dumps({"path": "a.md", "content": "x\n"}), tool_call_id="c1"
            )
        }
    elif state["n"] == 2:
        yield {0: DeltaToolCall(name="shell", json_args=json.dumps({"command": "echo hi >> a.md"}), tool_call_id="c2")}
    else:
        yield "Done."


async def _probe_frames(workspace: Path) -> list[StreamEnvelope]:
    """Run a turn with the coder tools and the real handler. Return the published frames."""
    registry = SubscriberRegistry()
    queue = registry.subscribe(RUN_ID)
    state = {"n": 0}

    async def stream_fn(messages: Any, info: Any) -> AsyncIterator[Any]:
        async for item in _stream_fn(state, messages, info):
            yield item

    step_store, product = InMemoryStepStore(), FakeProductStore()
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
    await run_turn(
        turn,
        step_store=step_store,
        product_store=product,
        run_id=RUN_ID,
        event_stream_handler=make_event_handler(registry),
        model=FunctionModel(stream_function=stream_fn),
    )
    frames: list[StreamEnvelope] = []
    while not queue.empty():
        item = queue.get_nowait()
        assert isinstance(item, StreamEnvelope)
        frames.append(item)
    return frames


async def _one(event: Any) -> AsyncIterator[Any]:
    yield event


def _delta(text: str) -> PartDeltaEvent:
    return PartDeltaEvent(index=0, delta=TextPartDelta(content_delta=text))


def _drain(queue: asyncio.Queue[StreamEnvelope | None]) -> list[StreamEnvelope]:
    frames: list[StreamEnvelope] = []
    while not queue.empty():
        item = queue.get_nowait()
        assert isinstance(item, StreamEnvelope)
        frames.append(item)
    return frames


def _set_step_id(monkeypatch: pytest.MonkeyPatch, step_id: int | None) -> None:
    monkeypatch.setattr(daemon, "DBOS", SimpleNamespace(step_id=step_id))


async def test_run_turn_frames_have_distinct_increasing_ordinals(tmp_path: Path) -> None:
    frames = await _probe_frames(tmp_path)
    assert any(frame.kind == "function_tool_call" for frame in frames)
    assert any(frame.kind == "function_tool_result" for frame in frames)
    keys = [(frame.run_id, frame.event_ordinal) for frame in frames]
    assert len(set(keys)) == len(keys)
    ordinals = [frame.event_ordinal for frame in frames]
    assert ordinals == sorted(ordinals)
    assert ordinals[0] == 1


async def test_deduplicate_keeps_every_frame_of_a_probe_run(tmp_path: Path) -> None:
    frames = await _probe_frames(tmp_path)

    async def source() -> AsyncIterator[StreamEnvelope]:
        for frame in frames:
            yield frame

    kept = [frame async for frame in _deduplicate(source())]
    assert len(kept) == len(frames) > 10


async def test_one_event_per_handler_call_gives_distinct_increasing_ordinals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DBOS path calls the handler once for each tool event, in its own step."""
    registry = SubscriberRegistry()
    queue = registry.subscribe(RUN_ID)
    handler = make_event_handler(registry)
    for step_id in (3, 5, 6, 9):
        _set_step_id(monkeypatch, step_id)
        await handler(SimpleNamespace(run_id=RUN_ID, run_step=1), _one(_delta(str(step_id))))
    ordinals = [frame.event_ordinal for frame in _drain(queue)]
    assert ordinals == [3 << 20, 5 << 20, 6 << 20, 9 << 20]


async def test_events_of_one_step_use_the_step_id_and_an_index(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = SubscriberRegistry()
    queue = registry.subscribe(RUN_ID)
    handler = make_event_handler(registry)
    _set_step_id(monkeypatch, 7)

    async def events() -> AsyncIterator[Any]:
        yield PartStartEvent(index=0, part=TextPart(content=""))
        yield _delta("a")
        yield _delta("b")

    await handler(SimpleNamespace(run_id=RUN_ID, run_step=1), events())
    _set_step_id(monkeypatch, 8)
    await handler(SimpleNamespace(run_id=RUN_ID, run_step=2), _one(_delta("c")))
    ordinals = [frame.event_ordinal for frame in _drain(queue)]
    base = 7 << 20
    assert ordinals == [base, base + 1, base + 2, 8 << 20]
    assert ordinals == sorted(set(ordinals))


async def test_replayed_step_gives_the_same_ordinals_and_the_client_drops_the_copies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = SubscriberRegistry()
    queue = registry.subscribe(RUN_ID)
    handler = make_event_handler(registry)

    async def events() -> AsyncIterator[Any]:
        yield _delta("a")
        yield _delta("b")

    ctx = SimpleNamespace(run_id=RUN_ID, run_step=2)
    _set_step_id(monkeypatch, 12)
    await handler(ctx, events())
    first = _drain(queue)
    await handler(ctx, events())  # DBOS runs the in-flight step again after recovery.
    second = _drain(queue)
    assert [frame.event_ordinal for frame in first] == [frame.event_ordinal for frame in second]

    async def source() -> AsyncIterator[StreamEnvelope]:
        for frame in (*first, *second):
            yield frame

    kept = [frame async for frame in _deduplicate(source())]
    assert kept == first


async def test_a_step_with_too_many_events_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = SubscriberRegistry()
    registry.subscribe(RUN_ID)
    handler = make_event_handler(registry)
    _set_step_id(monkeypatch, 4)

    async def events() -> AsyncIterator[Any]:
        for _ in range(2**20 + 1):
            yield _delta("x")

    with pytest.raises(RuntimeError, match="cannot number"):
        await handler(SimpleNamespace(run_id=RUN_ID, run_step=1), events())


async def test_registry_forgets_the_counter_when_the_last_subscriber_leaves() -> None:
    registry = SubscriberRegistry()
    queue = registry.subscribe(RUN_ID)
    assert registry.next_ordinal(RUN_ID) == 1
    assert registry.next_ordinal(RUN_ID) == 2
    registry.unsubscribe(RUN_ID, queue)
    assert registry._counters == {}
    assert registry.next_ordinal(RUN_ID) is None
    assert registry._counters == {}


async def test_the_client_keeps_the_final_frame() -> None:
    event = StreamEnvelope(run_id=RUN_ID, request_ordinal=1, event_ordinal=1, kind="part_delta")
    final = StreamEnvelope(run_id=RUN_ID, request_ordinal=-1, event_ordinal=-1, kind="turn_result", final=True)

    async def source() -> AsyncIterator[StreamEnvelope]:
        for item in (event, event, final):
            yield item

    kept = [frame async for frame in _deduplicate(source())]
    assert kept == [event, final]
