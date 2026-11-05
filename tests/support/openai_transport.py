"""In-process OpenAI-compatible ASGI mock for wire tests.

Answers ``POST /v1/chat/completions`` with a controllable assistant message,
both buffered and streamed, and records every request so a test can assert the
outgoing payload. It is driven through pydantic-ai's real wire path via
``OpenAIProvider(http_client=httpx.AsyncClient(transport=ASGITransport(app=mock)))``,
so the model layer is exercised without a network or a GPU.

This is modelled on lodestar's ``tests/support/openai_transport.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
import time
from collections.abc import Callable, Iterator, MutableMapping
from typing import Any

import httpx
import uvicorn
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from agentman.model import profile_for


class MockOpenAI:
    """A minimal OpenAI chat-completions server."""

    def __init__(
        self,
        content: str = "{}",
        *,
        model: str = "mock",
        delay: float = 0.0,
        chunk_delay: float = 0.0,
        content_for: Callable[[dict[str, Any]], str] | None = None,
        status_for: Callable[[dict[str, Any]], int] | None = None,
    ) -> None:
        self.content = content
        self.model = model
        self.delay = delay
        self.chunk_delay = chunk_delay
        self.content_for = content_for
        self.status_for = status_for
        self.requests: list[dict[str, Any]] = []
        self.request_paths: list[str] = []
        self.active_calls = 0
        self.max_active_calls = 0

    @property
    def request(self) -> dict[str, Any] | None:
        return self.requests[-1] if self.requests else None

    async def __call__(self, scope: MutableMapping[str, Any], receive: Any, send: Any) -> None:
        body = b""
        while True:
            event = await receive()
            body += event.get("body", b"")
            if not event.get("more_body", False):
                break
        request = json.loads(body) if body else {}
        self.requests.append(request)
        self.request_paths.append(str(scope.get("path", "")))
        self.active_calls += 1
        self.max_active_calls = max(self.max_active_calls, self.active_calls)

        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            is_chat_completion = self.request_paths[-1].endswith("/chat/completions")
            status = self.status_for(request) if self.status_for is not None and is_chat_completion else 200
            if status != 200:
                await _send_json(
                    send,
                    status,
                    {"error": {"message": "retryable mock failure"}},
                    headers=[(b"retry-after-ms", b"1")],
                )
                return
            if request.get("stream"):
                await self._stream(send, request)
            else:
                await self._buffered(send, request)
        finally:
            self.active_calls -= 1

    async def _buffered(self, send: Any, request: dict[str, Any]) -> None:
        content = self.content_for(request) if self.content_for is not None else self.content
        payload = {
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 0,
            "model": request.get("model", self.model),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
        }
        await _send_json(send, 200, payload)

    async def _stream(self, send: Any, request: dict[str, Any]) -> None:
        content = self.content_for(request) if self.content_for is not None else self.content
        headers = [(b"content-type", b"text/event-stream")]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        chunks: list[dict[str, Any]] = [
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": request.get("model", self.model),
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": content}, "finish_reason": None}],
            },
            {
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": request.get("model", self.model),
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            },
        ]
        for chunk in chunks:
            frame = f"data: {json.dumps(chunk)}\n\n".encode()
            await send({"type": "http.response.body", "body": frame, "more_body": True})
            if self.chunk_delay:
                await asyncio.sleep(self.chunk_delay)
        await send({"type": "http.response.body", "body": b"data: [DONE]\n\n", "more_body": False})


async def _send_json(
    send: Any,
    status: int,
    payload: dict[str, Any],
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), *(headers or [])],
        }
    )
    await send({"type": "http.response.body", "body": json.dumps(payload).encode()})


def build_mock_model(mock: MockOpenAI, *, model_name: str = "gemma", n_ctx: int | None = 32768) -> OpenAIChatModel:
    """Build an ``OpenAIChatModel`` that talks to ``mock`` over ASGI in process.

    ``httpx.ASGITransport`` buffers a streamed response, so this cannot prove
    live SSE. Use :func:`serve_openai_mock` with :func:`build_tcp_model` for that.
    """
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock), base_url="http://mock/v1")
    provider = OpenAIProvider(base_url="http://mock/v1", http_client=client)
    return OpenAIChatModel(model_name, provider=provider, profile=profile_for(n_ctx))


def build_tcp_model(*, port: int, model_name: str = "gemma", n_ctx: int | None = 32768) -> OpenAIChatModel:
    """Build an ``OpenAIChatModel`` aimed at a real HTTP server on ``port``."""
    provider = OpenAIProvider(base_url=f"http://127.0.0.1:{port}/v1")
    return OpenAIChatModel(model_name, provider=provider, profile=profile_for(n_ctx))


class RecordingTransport(httpx.AsyncBaseTransport):
    """Wrap a transport and keep every outgoing request object."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


def build_recording_model(
    *, base_url: str, model_name: str = "gemma", n_ctx: int | None = 32768
) -> tuple[OpenAIChatModel, RecordingTransport]:
    """Build a model that records the bytes it sends to the real server at ``base_url``.

    The returned transport keeps the outgoing ``httpx.Request`` objects, so a
    test can assert on exactly what crossed the wire.
    """
    transport = RecordingTransport(httpx.AsyncHTTPTransport())
    client = httpx.AsyncClient(transport=transport)
    provider = OpenAIProvider(base_url=base_url, http_client=client)
    return OpenAIChatModel(model_name, provider=provider, profile=profile_for(n_ctx)), transport


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextlib.contextmanager
def serve_openai_mock(mock: MockOpenAI) -> Iterator[int]:
    """Serve ``mock`` over real TCP in a background thread; yield its port.

    Real TCP is what makes a streamed response actually stream. The in-process
    ASGI transport buffers the whole body.
    """
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(mock, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.025)
    if not server.started:  # pragma: no cover - startup failure is infra
        raise RuntimeError("the mock OpenAI server did not start")
    try:
        yield port
    finally:
        server.should_exit = True
        thread.join(timeout=5)
