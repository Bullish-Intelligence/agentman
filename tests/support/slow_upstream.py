"""Serve a slow OpenAI-compatible stream over a local TCP socket."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class UpstreamConnection:
    opened_at: float
    closed_at: float | None = None
    chunks_sent: int = 0
    closed: asyncio.Event = field(default_factory=asyncio.Event)


class SlowUpstream:
    """Send one server-sent event every 50 ms until the client closes."""

    def __init__(self, *, chunk_interval: float = 0.05, duration: float = 60.0) -> None:
        self.chunk_interval = chunk_interval
        self.duration = duration
        self.connections: list[UpstreamConnection] = []
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._changed = asyncio.Event()
        self._port: int | None = None

    @property
    def base_url(self) -> str:
        if self._port is None:
            raise RuntimeError("the upstream server is not running")
        return f"http://127.0.0.1:{self._port}/v1"

    async def __aenter__(self) -> SlowUpstream:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        address = self._server.sockets[0].getsockname()
        self._port = int(address[1])
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        tasks = [task for task in self._tasks if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_for_connections(self, count: int, *, timeout: float = 5.0) -> None:
        await self._wait_until(lambda: len(self.connections) >= count, timeout=timeout)

    async def wait_for_chunks(self, count: int, *, timeout: float = 5.0) -> None:
        await self._wait_until(lambda: sum(item.chunks_sent for item in self.connections) >= count, timeout=timeout)

    async def wait_for_closed(self, index: int = 0, *, timeout: float = 5.0) -> UpstreamConnection:
        await self.wait_for_connections(index + 1, timeout=timeout)
        connection = self.connections[index]
        await asyncio.wait_for(connection.closed.wait(), timeout=timeout)
        return connection

    async def _wait_until(self, predicate: Any, *, timeout: float) -> None:
        async def wait() -> None:
            while not predicate():
                self._changed.clear()
                if predicate():
                    return
                await self._changed.wait()

        await asyncio.wait_for(wait(), timeout=timeout)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        connection = UpstreamConnection(opened_at=time.monotonic())
        self.connections.append(connection)
        self._changed.set()
        disconnect_task: asyncio.Task[bytes] | None = None
        try:
            request = await _read_request(reader)
            model = str(request.get("model", "slow-model"))
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Transfer-Encoding: chunked\r\n"
                b"Connection: keep-alive\r\n\r\n"
            )
            await writer.drain()
            disconnect_task = asyncio.create_task(reader.read(1))
            planned_chunks = max(1, int(self.duration / self.chunk_interval))
            for index in range(planned_chunks):
                await asyncio.sleep(self.chunk_interval)
                if disconnect_task.done():
                    break
                payload = _chunk(model=model, index=index)
                await _write_chunk(writer, payload)
                connection.chunks_sent += 1
                self._changed.set()
            else:
                await _write_chunk(writer, _final_chunk(model=model))
                await _write_chunk(writer, b"data: [DONE]\n\n")
                writer.write(b"0\r\n\r\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            if disconnect_task is not None:
                if not disconnect_task.done():
                    disconnect_task.cancel()
                await asyncio.gather(disconnect_task, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            connection.closed_at = time.monotonic()
            connection.closed.set()
            self._changed.set()
            if task is not None:
                self._tasks.discard(task)


async def _read_request(reader: asyncio.StreamReader) -> dict[str, Any]:
    first_line = await reader.readline()
    if not first_line:
        return {}
    headers: dict[str, str] = {}
    while line := await reader.readline():
        if line in {b"\r\n", b"\n"}:
            break
        name, _, value = line.decode().partition(":")
        headers[name.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    body = await reader.readexactly(length) if length else b""
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _chunk(*, model: str, index: int) -> bytes:
    delta: dict[str, str] = {"content": f"part {index} "}
    if index == 0:
        delta["role"] = "assistant"
    payload = {
        "id": "chatcmpl-slow-upstream",
        "object": "chat.completion.chunk",
        "created": 1_700_000_000,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload)}\n\n".encode()


def _final_chunk(*, model: str) -> bytes:
    payload = {
        "id": "chatcmpl-slow-upstream",
        "object": "chat.completion.chunk",
        "created": 1_700_000_000,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }
    return f"data: {json.dumps(payload)}\n\n".encode()


async def _write_chunk(writer: asyncio.StreamWriter, payload: bytes) -> None:
    writer.write(f"{len(payload):X}\r\n".encode() + payload + b"\r\n")
    await writer.drain()


__all__ = ["SlowUpstream", "UpstreamConnection"]
