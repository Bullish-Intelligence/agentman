"""Phase F: the hermetic parts of the eight slice-1 checks.

Checks 1-3 and 8 against real infrastructure live in the integration modules.
These cover the shapes that fail silently: SSE ordinal stamping and dedup, the
Markdown render, a nested schema round trip, and wall-clock overlap.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    UserPromptPart,
)
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore
from support.openai_transport import MockOpenAI, build_mock_model

from agentman.client import DaemonError, _deduplicate, _parse_frame
from agentman.daemon import SubscriberRegistry, make_event_handler
from agentman.protocol import StreamEnvelope
from agentman.render import render_messages
from agentman.runtime import Turn, run_turn


@dataclass
class FakeContext:
    run_id: str = "run-1"
    run_step: int = 3
    extras: list[object] = field(default_factory=list)


async def _events(*items: object) -> AsyncIterator[object]:
    for item in items:
        yield item


async def test_event_handler_stamps_both_ordinals() -> None:
    registry = SubscriberRegistry()
    queue = registry.subscribe("run-1")
    handler = make_event_handler(registry)
    ctx = FakeContext(run_step=5)
    await handler(
        ctx,
        _events(
            PartStartEvent(index=0, part=TextPart(content="")),
            PartDeltaEvent(index=0, delta=TextPartDelta(content_delta="hi")),
        ),
    )
    first = queue.get_nowait()
    second = queue.get_nowait()
    assert isinstance(first, StreamEnvelope) and isinstance(second, StreamEnvelope)
    assert first.run_id == "run-1"
    assert first.request_ordinal == 5
    assert (first.event_ordinal, second.event_ordinal) == (0, 1)


async def test_event_ordinal_resets_for_each_request() -> None:
    registry = SubscriberRegistry()
    queue = registry.subscribe("run-1")
    handler = make_event_handler(registry)
    await handler(FakeContext(run_step=1), _events(PartDeltaEvent(index=0, delta=TextPartDelta(content_delta="a"))))
    await handler(FakeContext(run_step=2), _events(PartDeltaEvent(index=0, delta=TextPartDelta(content_delta="b"))))
    first = queue.get_nowait()
    second = queue.get_nowait()
    assert isinstance(first, StreamEnvelope) and isinstance(second, StreamEnvelope)
    assert (first.request_ordinal, first.event_ordinal) == (1, 0)
    assert (second.request_ordinal, second.event_ordinal) == (2, 0)


async def test_subscriber_deduplicates_on_the_triple() -> None:
    repeated = StreamEnvelope(run_id="r", request_ordinal=1, event_ordinal=2, kind="part_delta")
    distinct = StreamEnvelope(run_id="r", request_ordinal=1, event_ordinal=3, kind="part_delta")

    async def source() -> AsyncIterator[StreamEnvelope]:
        for item in (repeated, repeated, distinct):
            yield item

    kept = [envelope async for envelope in _deduplicate(source())]
    assert len(kept) == 2


def test_client_parses_an_sse_frame() -> None:
    envelope = StreamEnvelope(run_id="r", request_ordinal=1, event_ordinal=0, kind="part_delta")
    frame = f"event: part_delta\ndata: {envelope.model_dump_json()}\n"
    assert _parse_frame(frame) == envelope


def test_client_rejects_an_unsupported_protocol_version() -> None:
    envelope = StreamEnvelope(protocol=1, run_id="r", request_ordinal=1, event_ordinal=0, kind="part_delta")
    frame = f"event: part_delta\ndata: {envelope.model_dump_json()}\n"
    try:
        _parse_frame(frame)
    except DaemonError as error:
        assert "protocol 1" in str(error)
    else:
        raise AssertionError("the client accepted an unsupported protocol")


async def test_events_published_before_subscription_are_dropped() -> None:
    registry = SubscriberRegistry()
    event = StreamEnvelope(run_id="run-1", request_ordinal=1, event_ordinal=0, kind="part_delta")
    registry.publish("run-1", event)
    queue = registry.subscribe("run-1")
    assert queue.empty()


def test_render_marks_user_and_assistant() -> None:
    messages = [
        ModelRequest(parts=[UserPromptPart(content="hello")]),
        ModelResponse(parts=[TextPart(content="world")]),
    ]
    rendered = render_messages(messages)
    assert "## User" in rendered
    assert "hello" in rendered
    assert "## Assistant" in rendered
    assert "world" in rendered


NESTED_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "address": {
            "type": "object",
            "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
            "required": ["street", "city"],
        },
    },
    "required": ["title", "address"],
    "$defs": {
        "Address": {
            "type": "object",
            "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
            "required": ["street", "city"],
        }
    },
}


async def test_nested_schema_round_trips_through_the_mock() -> None:
    payload = {"title": "home", "address": {"street": "1 Main", "city": "Springfield"}}
    mock = MockOpenAI(content=json.dumps(payload))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    turn = Turn(
        workspace="/repo",
        thread_id=thread.id,
        agent_spec={"instructions": "x"},
        agent_name="nested",
        model="mock:model",
        prompt="hi",
        output_schema=NESTED_SCHEMA,
        output_type="models:Summary",
    )
    result = await run_turn(
        turn, step_store=step_store, product_store=product, run_id="run-nested", model=build_mock_model(mock)
    )
    assert result.output == payload
    assert mock.request is not None
    assert mock.request["response_format"]["type"] == "json_schema"


async def test_concurrent_turns_overlap_in_wall_clock() -> None:
    """Check 8, hermetically: agentman adds no lock, so turns overlap."""
    delay = 0.3
    mock = MockOpenAI(content=json.dumps({"title": "x", "rating": 1}), delay=delay)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    model = build_mock_model(mock)
    schema = {"type": "object", "properties": {"title": {"type": "string"}, "rating": {"type": "integer"}}}

    async def one(index: int):
        turn = Turn(
            workspace="/repo",
            thread_id=thread.id,
            agent_spec={"instructions": "x"},
            agent_name="batch",
            model="mock:model",
            prompt="hi",
            output_schema=schema,
        )
        return await run_turn(
            turn,
            step_store=step_store,
            product_store=product,
            run_id=f"run-{index}",
            model=model,
        )

    await asyncio.gather(*(one(index) for index in range(4)))
    assert mock.max_active_calls > 1, f"model requests did not overlap: {mock.max_active_calls} active call(s)"
