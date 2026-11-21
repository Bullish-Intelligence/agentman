"""Caller metadata on a run: validation, the daemon path, the client check."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore
from support.openai_transport import MockOpenAI, build_mock_model

import agentman.daemon as daemon
from agentman.client import Client, DaemonError
from agentman.config import Config, Value
from agentman.daemon import AgentmanApp, _HttpError
from agentman.persistence.store import Run
from agentman.protocol import CAPABILITIES, MAX_METADATA_BYTES, RunInfo, TurnRequest
from agentman.runtime import Turn, reset_agent_cache, run_turn
from agentman.status import run_info

METADATA = {"origin": "devman:inbox", "labels": ["a", "b"]}


def _app() -> AgentmanApp:
    return AgentmanApp(
        Config(
            agent=Value(None, "default"),
            model=Value(None, "default"),
            database_url=Value("postgresql://unused", "test"),
            socket=Value("/tmp/agentman-test.sock", "test"),
            router_url=Value("http://router.test/v1", "test"),
        )
    )


def _body(**extra: Any) -> bytes:
    fields = {"workspace": "/repo", "thread_id": "thread-1", "model": "mock:model", "prompt": "hello", **extra}
    return json.dumps(fields).encode()


async def _wait_forever_receive() -> dict[str, Any]:
    await asyncio.Future()
    raise AssertionError("the receive task should be cancelled")


class _Store:
    def __init__(self) -> None:
        self.created: list[object] = []

    async def get_thread(self, thread_id: str) -> object:
        return SimpleNamespace(id=thread_id)

    async def get_run(self, run_id: str) -> None:
        return None

    async def create_run(self, **fields: object) -> None:
        self.created.append(fields)


# -- validation ------------------------------------------------------------------


def test_metadata_defaults_to_none() -> None:
    assert TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p").metadata is None


def test_metadata_with_reserved_and_other_keys_is_accepted() -> None:
    request = TurnRequest(
        workspace="/repo",
        thread_id="t",
        model="m",
        prompt="p",
        metadata={**METADATA, "ticket": 7, "extra": {"nested": True}},
    )

    assert request.metadata == {**METADATA, "ticket": 7, "extra": {"nested": True}}


@pytest.mark.parametrize(
    "metadata",
    [
        {"origin": 3},
        {"origin": None},
        {"origin": ["user"]},
        {"labels": "a"},
        {"labels": ["a", 1]},
        {"labels": {"a": 1}},
        {"labels": None},
        {"note": "x" * MAX_METADATA_BYTES},
    ],
)
def test_bad_metadata_is_rejected(metadata: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p", metadata=metadata)


def test_the_size_limit_counts_compact_json() -> None:
    # `{"k":"` and `"}` take 8 bytes in compact JSON.
    fits = {"k": "x" * (MAX_METADATA_BYTES - 8)}
    assert len(json.dumps(fits, separators=(",", ":"))) == MAX_METADATA_BYTES
    TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p", metadata=fits)

    too_big = {"k": "x" * (MAX_METADATA_BYTES - 7)}
    with pytest.raises(ValidationError, match="limit"):
        TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p", metadata=too_big)


def test_the_run_metadata_capability_is_listed() -> None:
    assert "run-metadata" in CAPABILITIES


# -- the daemon ------------------------------------------------------------------


@pytest.mark.parametrize(
    "metadata",
    [
        {"origin": 3},
        {"labels": ["a", 1]},
        {"labels": "a"},
        {"note": "x" * (MAX_METADATA_BYTES + 1)},
    ],
)
def test_invalid_metadata_returns_422_and_creates_no_run(
    monkeypatch: pytest.MonkeyPatch, metadata: dict[str, Any]
) -> None:
    app = _app()
    store = _Store()
    app.store = cast(Any, store)
    started: list[object] = []

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*args: object) -> object:
            started.append(args)
            raise AssertionError("a rejected request must not start a workflow")

    monkeypatch.setattr(daemon, "DBOS", _DBOS)

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("a rejected request must not start an SSE response")

        with pytest.raises(_HttpError) as error:
            await app._handle_turn(_body(metadata=metadata), _wait_forever_receive, send)
        assert error.value.status == 422

    asyncio.run(exercise())
    assert started == []
    assert store.created == []


def test_the_daemon_passes_metadata_to_the_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.store = cast(Any, _Store())
    turns: list[Turn] = []

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            raise RuntimeError("stop after the turn is built")

    class _DBOS:
        @staticmethod
        async def get_workflow_status_async(run_id: str) -> None:
            return None

        @staticmethod
        async def start_workflow_async(_: object, turn: Turn) -> _Handle:
            turns.append(turn)
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)

    async def exercise(body: bytes) -> None:
        async def send(_: Any) -> None:
            return None

        await app._handle_turn(body, _wait_forever_receive, send)

    asyncio.run(exercise(_body(metadata=METADATA)))
    asyncio.run(exercise(_body()))

    assert [turn.metadata for turn in turns] == [METADATA, None]


def test_a_turn_recorded_before_the_upgrade_still_loads() -> None:
    turn = Turn(workspace="/repo", thread_id="t", agent_spec={}, agent_name=None, model="m", prompt="p")

    assert turn.metadata is None


# -- the run row and the response -------------------------------------------------


async def _run_with(metadata: dict[str, Any] | None) -> tuple[FakeProductStore, Run]:
    mock = MockOpenAI(content="done")
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    turn = Turn(
        workspace="/repo",
        thread_id=thread.id,
        agent_spec={"instructions": "be brief"},
        agent_name="summary",
        model="inferference:gemma",
        prompt="hello",
        output_type="str",
        metadata=metadata,
    )
    reset_agent_cache()
    await run_turn(
        turn,
        step_store=InMemoryStepStore(),
        product_store=product,
        run_id="run-1",
        model=build_mock_model(mock),
    )
    run = await product.get_run("run-1")
    assert run is not None
    return product, run


async def test_the_run_row_stores_the_caller_metadata() -> None:
    _, run = await _run_with(METADATA)

    assert run.caller_metadata == METADATA
    assert "origin" not in run.run_metadata
    assert "labels" not in run.run_metadata


async def test_a_run_without_metadata_stores_an_empty_object() -> None:
    _, run = await _run_with(None)

    assert run.caller_metadata == {}
    assert run_info(run).metadata == {}


async def test_get_run_returns_the_metadata() -> None:
    product, run = await _run_with(METADATA)
    app = _app()
    app.store = cast(Any, product)
    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app._route("GET", f"/v1/runs/{run.id}", {}, b"", _wait_forever_receive, send)

    info = RunInfo.model_validate_json(sent[-1]["body"])
    assert info.metadata == METADATA
    assert info.run_metadata.requested_max_transport_retries is None


def test_run_info_metadata_defaults_to_an_empty_object() -> None:
    now = datetime.now(UTC)
    info = RunInfo(
        id="r", thread_id="t", workspace="/repo", agent="a", model="m", status="running", prompt="p",
        created_at=now, updated_at=now,
    )  # fmt: skip

    assert info.metadata == {}


# -- the client ------------------------------------------------------------------


def _client_with_capabilities(capabilities: list[str]) -> tuple[Client, list[str]]:
    reads: list[str] = []

    class FakeClient(Client):
        async def _get(self, path: str, params: dict[str, str] | None = None) -> object:
            reads.append(path)
            return {"status": "ok", "capabilities": capabilities}

    client = FakeClient.__new__(FakeClient)
    client.socket_path = "/tmp/none.sock"
    return client, reads


def test_the_client_refuses_metadata_when_the_daemon_lacks_the_capability() -> None:
    client, _ = _client_with_capabilities(["cancel"])
    request = TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p", metadata=METADATA)

    async def drain() -> None:
        async for _ in client._raw_stream(request):
            raise AssertionError("the client must not send the request")

    with pytest.raises(DaemonError, match="run-metadata"):
        asyncio.run(drain())


def test_the_client_reads_the_capabilities_once() -> None:
    client, reads = _client_with_capabilities(["cancel", "run-metadata"])

    async def read_twice() -> list[str]:
        await client.capabilities()
        return await client.capabilities()

    assert asyncio.run(read_twice()) == ["cancel", "run-metadata"]
    assert reads == ["/v1/health"]


def test_the_client_does_not_check_capabilities_without_metadata() -> None:
    client, reads = _client_with_capabilities([])
    request = TurnRequest(workspace="/repo", thread_id="t", model="m", prompt="p")

    class _Boom(Exception):
        pass

    class _Stream:
        def __init__(self, *_: object, **__: object) -> None:
            raise _Boom

    client._client = cast(Any, SimpleNamespace(stream=_Stream))
    client._stream_timeout = cast(Any, None)

    async def drain() -> None:
        async for _ in client._raw_stream(request):
            pass

    with pytest.raises(_Boom):
        asyncio.run(drain())
    assert reads == []
