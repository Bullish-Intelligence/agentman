"""Model stream cancellation tests."""

from __future__ import annotations

import asyncio
import time

import pytest
from pydantic_ai import Agent
from support.slow_upstream import SlowUpstream, UpstreamConnection

from agentman.model import build_model

MODEL_STRINGS = ("inferference:gemma", "openai:gpt-4o-mini")


async def _cancel_stream(
    upstream: SlowUpstream,
    model_string: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[float, UpstreamConnection]:
    if model_string.startswith("openai:"):
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
        monkeypatch.setenv("OPENAI_BASE_URL", upstream.base_url)
        router_url = None
    else:
        monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
        router_url = upstream.base_url
    model = build_model(model_string, router_url=router_url, n_ctx=None)
    agent = Agent(model=model, output_type=str)

    async def stream() -> None:
        async with agent.run_stream("continue until stopped") as result:
            async for _ in result.stream_text(delta=True):
                pass

    task = asyncio.create_task(stream())
    await upstream.wait_for_connections(1)
    await asyncio.sleep(0.5)
    cancel_time = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    connection = await upstream.wait_for_closed(timeout=2.0)
    return cancel_time, connection


@pytest.mark.parametrize("model_string", MODEL_STRINGS)
async def test_cancelling_a_streamed_request_closes_the_upstream_connection(
    model_string: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SlowUpstream() as upstream:
        cancel_time, connection = await _cancel_stream(upstream, model_string, monkeypatch)

    assert connection.closed_at is not None
    assert connection.closed_at - cancel_time < 1.0
    assert connection.chunks_sent < 1200


@pytest.mark.parametrize("model_string", MODEL_STRINGS)
async def test_cancel_does_not_trigger_a_transport_retry(
    model_string: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with SlowUpstream() as upstream:
        _, connection = await _cancel_stream(upstream, model_string, monkeypatch)

    assert len(upstream.connections) == 1
    assert connection.chunks_sent > 0
