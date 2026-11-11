"""Hermetic tests for the client's bounded fan-out collection."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing

import httpx

from agentman.client import Client, TurnStreamError
from agentman.protocol import StreamEnvelope, TurnRequest, TurnResponse


def test_stream_turns_preserves_input_order_and_bounds_in_flight_requests() -> None:
    state = {"active": 0, "max_active": 0}

    class FakeClient(Client):
        async def stream_turn(self, request: TurnRequest) -> AsyncGenerator[StreamEnvelope]:
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
        async def stream_turn(self, request: TurnRequest) -> AsyncGenerator[StreamEnvelope]:
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


def test_capabilities_reads_cancel_support_from_health() -> None:
    class FakeClient(Client):
        async def _get(self, path: str, params: dict[str, str] | None = None) -> object:
            assert path == "/v1/health"
            assert params is None
            return {"status": "ok", "capabilities": ["cancel", 2]}

    client = FakeClient.__new__(FakeClient)

    assert asyncio.run(client.capabilities()) == ["cancel"]


def test_cancel_run_posts_workspace_and_parses_response() -> None:
    class FakeClient(Client):
        async def _post(self, path: str, payload: object) -> object:
            assert path == "/v1/runs/run-1/cancel"
            assert payload == {"workspace": "/repo"}
            return {"run_id": "run-1", "status": "cancelled", "cancelled": True}

    client = FakeClient.__new__(FakeClient)

    response = asyncio.run(client.cancel_run("run-1", workspace="/repo"))

    assert response.run_id == "run-1"
    assert response.status == "cancelled"
    assert response.cancelled


def _final_error_frame() -> StreamEnvelope:
    return StreamEnvelope(
        run_id="run-1",
        request_ordinal=-1,
        event_ordinal=-1,
        kind="turn_error",
        event={"error": "turn cancelled"},
        final=True,
    )


def _delta_frame(ordinal: int) -> StreamEnvelope:
    return StreamEnvelope(
        run_id="run-1",
        request_ordinal=0,
        event_ordinal=ordinal,
        kind="model_event",
        event={"event_kind": "part_delta"},
        final=False,
    )


def test_turn_stream_has_no_read_timeout_but_unary_calls_keep_one() -> None:
    seen: dict[str, dict[str, float | None]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen[request.url.path] = dict(request.extensions["timeout"])
        if request.url.path == "/v1/turns":
            return httpx.Response(200, text="")
        return httpx.Response(200, json={"status": "ok"})

    async def scenario() -> None:
        client = Client("/tmp/agentman-test.sock", timeout=12.0)
        await client._client.aclose()
        client._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://agentman", timeout=12.0
        )
        async with client:
            await client.capabilities()
            request = TurnRequest(workspace="/repo", thread_id="thread", model="mock", prompt="hello")
            async with aclosing(client.stream_turn(request)) as stream:
                async for _ in stream:
                    pass

    asyncio.run(scenario())

    assert seen["/v1/health"]["read"] == 12.0
    assert seen["/v1/turns"]["read"] is None
    assert seen["/v1/turns"]["connect"] == 12.0
    assert seen["/v1/turns"]["write"] == 12.0
    assert seen["/v1/turns"]["pool"] == 12.0


def test_stream_turn_closes_the_inner_generators_when_the_consumer_stops() -> None:
    closed: list[str] = []

    class ChainClient(Client):
        async def _raw_stream(self, request: TurnRequest) -> AsyncGenerator[StreamEnvelope]:
            try:
                yield _delta_frame(0)
                yield _delta_frame(1)
            finally:
                closed.append("raw")

    async def scenario() -> None:
        client = ChainClient.__new__(ChainClient)
        request = TurnRequest(workspace="/repo", thread_id="thread", model="mock", prompt="hello")
        async with aclosing(client.stream_turn(request)) as stream:
            async for _ in stream:
                break
        # No event-loop turn has run, so the garbage collector cannot have closed it.
        assert closed == ["raw"]

    asyncio.run(scenario())


def test_stream_turns_closes_the_stream_after_a_final_turn_error() -> None:
    closed: list[str] = []

    class ChainClient(Client):
        async def _raw_stream(self, request: TurnRequest) -> AsyncGenerator[StreamEnvelope]:
            try:
                yield _final_error_frame()
                yield _delta_frame(1)
            finally:
                closed.append("raw")

    async def scenario() -> None:
        client = ChainClient.__new__(ChainClient)
        request = TurnRequest(workspace="/repo", thread_id="thread", model="mock", prompt="hello")
        outcome = (await client.stream_turns([request], concurrency=1))[0]
        assert isinstance(outcome.error, TurnStreamError)
        assert closed == ["raw"]

    asyncio.run(scenario())
