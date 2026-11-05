"""Hermetic tests for the client's bounded fan-out collection."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from agentman.client import Client, TurnStreamError
from agentman.protocol import StreamEnvelope, TurnRequest, TurnResponse


def test_stream_turns_preserves_input_order_and_bounds_in_flight_requests() -> None:
    state = {"active": 0, "max_active": 0}

    class FakeClient(Client):
        async def stream_turn(self, request: TurnRequest) -> AsyncIterator[StreamEnvelope]:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            try:
                await asyncio.sleep(0.01 * (4 - int(request.prompt)))
                response = TurnResponse(
                    run_id=f"run-{request.prompt}",
                    thread_id=request.thread_id,
                    output=f"output-{request.prompt}",
                )
                yield StreamEnvelope(
                    run_id=response.run_id,
                    request_ordinal=-1,
                    event_ordinal=-1,
                    kind="turn_result",
                    event=response.model_dump(mode="json"),
                    final=True,
                )
            finally:
                state["active"] -= 1

    client = FakeClient.__new__(FakeClient)
    requests = [
        TurnRequest(workspace="/repo", thread_id="thread", model="mock", prompt=str(index)) for index in range(4)
    ]

    outcomes = asyncio.run(client.stream_turns(requests, concurrency=2))

    assert [outcome.index for outcome in outcomes] == [0, 1, 2, 3]
    assert [outcome.response.output for outcome in outcomes if outcome.response is not None] == [
        "output-0",
        "output-1",
        "output-2",
        "output-3",
    ]
    assert state["max_active"] == 2


def test_stream_turns_maps_terminal_error_frame_to_finding() -> None:
    class FakeClient(Client):
        async def stream_turn(self, request: TurnRequest) -> AsyncIterator[StreamEnvelope]:
            yield StreamEnvelope(
                run_id="run-1",
                request_ordinal=-1,
                event_ordinal=-1,
                kind="turn_error",
                event={"error": "turn failed; inspect the durable run record"},
                final=True,
            )

    client = FakeClient.__new__(FakeClient)
    request = TurnRequest(workspace="/repo", thread_id="thread", model="mock", prompt="hello")
    outcome = asyncio.run(client.stream_turns([request], concurrency=1))[0]

    assert outcome.response is None
    assert isinstance(outcome.error, TurnStreamError)
    assert str(outcome.error) == "turn failed; inspect the durable run record"
