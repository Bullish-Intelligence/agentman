"""The socket client. Drains SSE for ``send`` and ``pipe``.

The normal development path uses a foreground ``agentman serve`` process. The
daemon also supports systemd socket activation, but there is still no
spawn-if-absent fallback in the client: a missing socket reports a clear
message naming the foreground command.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx

from agentman.protocol import (
    PROTOCOL_VERSION,
    NewThreadRequest,
    ResumeRequest,
    ResumeResponse,
    RunInfo,
    StatusRequest,
    StatusView,
    StreamEnvelope,
    ThreadInfo,
    ThreadList,
    ThreadRef,
    ThreadView,
    TurnRequest,
    TurnResponse,
)


class DaemonNotRunning(Exception):
    """The socket is absent, so ``agentman serve`` is not running."""


class DaemonError(Exception):
    """The daemon answered with an error.

    ``status_code`` lets the CLI distinguish an HTTP configuration/infrastructure
    failure from a result finding without changing the daemon protocol.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class TurnStreamError(Exception):
    """The daemon completed a turn stream without a turn result."""


@dataclass(frozen=True)
class TurnOutcome:
    """One collected turn result, kept in input order by ``stream_turns``."""

    index: int
    response: TurnResponse | None = None
    error: Exception | None = None


class Client:
    """An async client over the daemon's Unix socket."""

    def __init__(self, socket_path: str | Path, *, timeout: float = 300.0) -> None:
        self.socket_path = str(socket_path)
        transport = httpx.AsyncHTTPTransport(uds=self.socket_path)
        self._client = httpx.AsyncClient(transport=transport, base_url="http://agentman", timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    # -- reads -----------------------------------------------------------------

    async def health(self) -> bool:
        try:
            response = await self._client.get("/v1/health")
        except httpx.TransportError:
            return False
        return response.status_code == httpx.codes.OK

    async def new_thread(self, *, workspace: str, name: str | None = None) -> ThreadInfo:
        request = NewThreadRequest(workspace=workspace, name=name)
        payload = await self._post("/v1/threads", request.model_dump(mode="json"))
        return ThreadInfo.model_validate(payload)

    async def list_threads(self, *, workspace: str) -> list[ThreadInfo]:
        payload = await self._get("/v1/threads", params={"workspace": workspace})
        return ThreadList.model_validate(payload).threads

    async def resolve_thread(self, *, workspace: str, name: str) -> ThreadInfo:
        request = ThreadRef(workspace=workspace, name=name)
        payload = await self._post("/v1/threads/resolve", request.model_dump(mode="json"))
        return ThreadInfo.model_validate(payload)

    async def get_thread(self, thread_id: str) -> ThreadView:
        payload = await self._get(f"/v1/threads/{thread_id}")
        return ThreadView.model_validate(payload)

    async def get_run(self, run_id: str) -> RunInfo:
        payload = await self._get(f"/v1/runs/{run_id}")
        return RunInfo.model_validate(payload)

    async def status(self, *, workspace: str, thread_id: str | None = None, name: str | None = None) -> StatusView:
        payload = await self._post(
            "/v1/status",
            StatusRequest(workspace=workspace, thread_id=thread_id, name=name).model_dump(mode="json"),
        )
        return StatusView.model_validate(payload)

    async def resume(self, *, workspace: str, run_id: str) -> ResumeResponse:
        payload = await self._post(
            "/v1/resume", ResumeRequest(workspace=workspace, run_id=run_id).model_dump(mode="json")
        )
        return ResumeResponse.model_validate(payload)

    # -- the turn --------------------------------------------------------------

    async def stream_turn(self, request: TurnRequest) -> AsyncIterator[StreamEnvelope]:
        async for envelope in _deduplicate(self._raw_stream(request)):
            yield envelope

    async def stream_turns(self, requests: list[TurnRequest], *, concurrency: int) -> list[TurnOutcome]:
        """Collect several normal turn streams with a local in-flight bound.

        The semaphore belongs to this invocation and only gates socket requests
        created here. It never crosses the daemon boundary or wraps runtime
        execution.
        """
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        limiter = asyncio.Semaphore(concurrency)

        async def one(index: int, request: TurnRequest) -> TurnOutcome:
            async with limiter:
                try:
                    final: TurnResponse | None = None
                    async for envelope in self.stream_turn(request):
                        if envelope.final:
                            if envelope.kind == "turn_error":
                                message = envelope.event.get("error")
                                return TurnOutcome(
                                    index=index,
                                    error=TurnStreamError(
                                        message if isinstance(message, str) else "the daemon reported a failed turn"
                                    ),
                                )
                            if envelope.kind != "turn_result":
                                return TurnOutcome(
                                    index=index,
                                    error=TurnStreamError(f"the daemon sent unknown final frame {envelope.kind!r}"),
                                )
                            final = TurnResponse.model_validate(envelope.event)
                    return TurnOutcome(index=index, response=final)
                except Exception as error:
                    return TurnOutcome(index=index, error=error)

        return list(await asyncio.gather(*(one(index, request) for index, request in enumerate(requests))))

    async def _raw_stream(self, request: TurnRequest) -> AsyncIterator[StreamEnvelope]:
        data = request.model_dump(mode="json")
        try:
            async with self._client.stream("POST", "/v1/turns", json=data) as response:
                if response.status_code != httpx.codes.OK:
                    body = (await response.aread()).decode()
                    raise DaemonError(
                        f"daemon returned {response.status_code}: {body}", status_code=response.status_code
                    )
                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        frame, buffer = buffer.split("\n\n", 1)
                        envelope = _parse_frame(frame)
                        if envelope is not None:
                            yield envelope
        except httpx.ConnectError as error:
            raise DaemonNotRunning(f"cannot connect to {self.socket_path}; start `agentman serve`") from error

    # -- transport -------------------------------------------------------------

    async def _get(self, path: str, params: dict[str, str] | None = None) -> object:
        try:
            response = await self._client.get(path, params=params)
        except httpx.ConnectError as error:
            raise DaemonNotRunning(f"cannot connect to {self.socket_path}; start `agentman serve`") from error
        return _decode(response)

    async def _post(self, path: str, payload: object) -> object:
        try:
            response = await self._client.post(path, json=payload)
        except httpx.ConnectError as error:
            raise DaemonNotRunning(f"cannot connect to {self.socket_path}; start `agentman serve`") from error
        return _decode(response)


def _decode(response: httpx.Response) -> object:
    if response.status_code != httpx.codes.OK:
        raise DaemonError(f"daemon returned {response.status_code}: {response.text}", status_code=response.status_code)
    return response.json()


def _parse_frame(frame: str) -> StreamEnvelope | None:
    for line in frame.splitlines():
        if line.startswith("data: "):
            envelope = StreamEnvelope.model_validate_json(line[len("data: ") :])
            if envelope.protocol != PROTOCOL_VERSION:
                raise DaemonError(
                    f"daemon protocol {envelope.protocol} is incompatible with client protocol {PROTOCOL_VERSION}",
                    status_code=409,
                )
            return envelope
    return None


async def _deduplicate(
    source: AsyncIterator[StreamEnvelope],
) -> AsyncIterator[StreamEnvelope]:
    """Drop repeated frames on ``(run_id, request ordinal, event ordinal)``.

    DBOS may re-execute a step after a crash, so events can repeat. The
    subscriber deduplicates; the record is authoritative and complete either
    way.
    """
    seen: set[tuple[str, int, int]] = set()
    async for envelope in source:
        key = (envelope.run_id, envelope.request_ordinal, envelope.event_ordinal)
        if key in seen:
            continue
        seen.add(key)
        yield envelope


__all__ = ["Client", "DaemonError", "DaemonNotRunning", "TurnOutcome", "TurnStreamError"]
