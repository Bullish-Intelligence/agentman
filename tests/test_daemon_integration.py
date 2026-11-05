"""Phase D and F: the daemon as a transport, over real Postgres and DBOS.

Two checks live here. Check 6: SSE must deliver tokens before the model request
completes. Check 8: the daemon adds no lock, so concurrent turns overlap. The
daemon is driven over real HTTP (a threaded uvicorn server), because an
in-process ASGI transport buffers the response body and would hide live
delivery. The model is a mock served over real TCP for the same reason.

One daemon serves the whole module: DBOS is process-global, so it starts once
and stops once.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Iterator

import httpx
import pytest
import uvicorn
from support.openai_transport import MockOpenAI, build_tcp_model, free_port, serve_openai_mock

from agentman.config import Config, Value
from agentman.daemon import build_app
from agentman.protocol import TurnRequest

pytestmark = pytest.mark.integration

SCHEMA = {
    "type": "object",
    "properties": {"title": {"type": "string"}, "rating": {"type": "integer"}},
    "required": ["title", "rating"],
}

MODEL_DELAY_SECONDS = 0.3


@pytest.fixture(scope="module")
def daemon(database_url: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[int]:
    """Start one daemon over a TCP port with a slow mock model, and stop it."""
    import agentman.runtime as runtime

    previous_build_model = runtime.build_model
    mock = MockOpenAI(
        content=json.dumps({"title": "hello", "rating": 4}),
        delay=MODEL_DELAY_SECONDS,
        chunk_delay=MODEL_DELAY_SECONDS,
    )
    with serve_openai_mock(mock) as mock_port:
        model = build_tcp_model(port=mock_port)
        runtime.build_model = lambda *args, **kwargs: model  # ty: ignore[invalid-assignment]
        runtime.reset_agent_cache()
        config = Config(
            agent=Value("live", "cli"),
            model=Value("mock:model", "cli"),
            database_url=Value(database_url, "cli"),
            socket=Value(str(tmp_path_factory.mktemp("daemon") / "agentman.sock"), "cli"),
            router_url=Value(None, "cli"),
        )
        app = build_app(config)
        port = free_port()
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(0.025)
        assert server.started, "the daemon did not start"
        try:
            yield port
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            runtime.build_model = previous_build_model
            runtime.reset_agent_cache()


async def _new_thread(http: httpx.AsyncClient, workspace: str, name: str) -> str:
    created = await http.post("/v1/threads", json={"workspace": workspace, "name": name})
    return created.json()["id"]


def test_sse_delivers_tokens_before_the_request_completes(daemon: int) -> None:
    asyncio.run(_sse_scenario(daemon))


async def _sse_scenario(port: int) -> None:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=60) as http:
        thread_id = await _new_thread(http, "/repo/sse", "sse")
        request = TurnRequest(
            workspace="/repo/sse",
            thread_id=thread_id,
            agent_spec={"instructions": "x"},
            agent_name="sse",
            model="mock:model",
            prompt="hi",
            output_schema=SCHEMA,
        )
        loop = asyncio.get_event_loop()
        start = loop.time()
        first_token: float | None = None
        final: float | None = None
        async with http.stream("POST", "/v1/turns", json=request.model_dump(mode="json")) as response:
            assert response.status_code == httpx.codes.OK
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                envelope = json.loads(line[len("data: ") :])
                now = loop.time() - start
                if envelope["final"]:
                    final = now
                elif first_token is None:
                    first_token = now
        assert first_token is not None, "the stream delivered no event frames"
        assert final is not None, "the stream delivered no final frame"
        assert first_token < final - 0.05, (
            f"tokens were buffered until completion: first {first_token:.3f}s, final {final:.3f}s"
        )


def test_daemon_path_runs_turns_concurrently(daemon: int) -> None:
    """Check 8 through the daemon: it adds no lock, so turns overlap."""
    asyncio.run(_concurrent_scenario(daemon))


async def _concurrent_scenario(port: int, *, turns: int = 4) -> None:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=60) as http:
        thread_id = await _new_thread(http, "/repo/concurrent", "concurrent")
        request = TurnRequest(
            workspace="/repo/concurrent",
            thread_id=thread_id,
            agent_spec={"instructions": "x"},
            agent_name="concurrent",
            model="mock:model",
            prompt="hi",
            output_schema=SCHEMA,
        )
        payload = request.model_dump(mode="json")
        loop = asyncio.get_event_loop()
        intervals: list[tuple[float, float]] = []

        async def one() -> None:
            started = loop.time()
            async with http.stream("POST", "/v1/turns", json=payload) as response:
                assert response.status_code == httpx.codes.OK
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        envelope = json.loads(line[len("data: ") :])
                        if envelope.get("final"):
                            intervals.append((started, loop.time()))
                            return
            raise AssertionError("the turn stream ended without a final frame")

        await asyncio.gather(*(one() for _ in range(turns)))

    assert max(start for start, _ in intervals) < min(end for _, end in intervals), (
        f"daemon turns did not overlap: {intervals}"
    )
