"""Check 4 and 5 against the live rig.

A mock can prove the daemon sends ``response_format.type == "json_schema"``; it
cannot prove the inferference router forwards that schema unchanged. This test
does, by asking a live constrained model for a two-level nested object and
validating the reply against the same pydantic type the client would use.

Set ``AGENTMAN_RUN_LIVE_RIG=1`` to run it. The router must serve ``gemma``.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from inferference import InferferenceClient, resolve_router_url
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore
from support.openai_transport import build_recording_model
from typer.testing import CliRunner

import agentman.cli as cli
from agentman.cli import app
from agentman.client import TurnOutcome
from agentman.config import CONFIG_FILENAME
from agentman.model import resolve_capability_model, resolve_model, resolve_parallelism
from agentman.protocol import ThreadInfo, TurnResponse
from agentman.runtime import Turn, run_turn

pytestmark = pytest.mark.skipif(
    os.environ.get("AGENTMAN_RUN_LIVE_RIG") != "1",
    reason="set AGENTMAN_RUN_LIVE_RIG=1 to run against the live router",
)
LIVE_MODEL = os.environ.get("AGENTMAN_LIVE_MODEL", "gemma")
LIVE_MODEL_REF = f"inferference:{LIVE_MODEL}"

NESTED_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "rating": {"type": "integer"},
        "address": {
            "type": "object",
            "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
            "required": ["street", "city"],
        },
    },
    "required": ["title", "rating", "address"],
    "$defs": {
        "Address": {
            "type": "object",
            "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
            "required": ["street", "city"],
        }
    },
}


async def test_live_nested_schema_survives_the_router() -> None:
    router = resolve_router_url()
    model = await resolve_model(LIVE_MODEL_REF, router_url=router)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="live")
    turn = Turn(
        workspace="/repo",
        thread_id=thread.id,
        agent_spec={"instructions": "Answer with the requested object only."},
        agent_name="live",
        model=LIVE_MODEL_REF,
        prompt="Give a title, a rating of 3, and an address in Springfield.",
        output_schema=NESTED_SCHEMA,
        output_type="models:Summary",
        router_url=router,
        n_ctx=32768,
    )
    result = await run_turn(
        turn,
        step_store=step_store,
        product_store=product,
        run_id="run-live",
        model=model,
    )
    assert set(result.output) == {"title", "rating", "address"}
    assert int(result.output["rating"]) == 3
    assert set(result.output["address"]) == {"street", "city"}
    snapshot = await step_store.latest_snapshot(run_id="run-live")
    assert snapshot is not None and snapshot.messages


async def test_live_outgoing_request_carries_a_flattened_schema() -> None:
    """Check 4 with byte capture: the router receives a flattened JSON schema."""
    router = resolve_router_url()
    model, transport = build_recording_model(base_url=router, model_name=LIVE_MODEL)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="bytes")
    turn = Turn(
        workspace="/repo",
        thread_id=thread.id,
        agent_spec={"instructions": "Answer with the requested object only."},
        agent_name="bytes",
        model=LIVE_MODEL_REF,
        prompt="Give a title, a rating of 3, and an address in Springfield.",
        output_schema=NESTED_SCHEMA,
        output_type="models:Summary",
        router_url=router,
        n_ctx=32768,
    )
    await run_turn(
        turn,
        step_store=step_store,
        product_store=product,
        run_id="run-bytes",
        model=model,
    )
    assert transport.requests, "no request reached the transport"
    body = json.loads(transport.requests[-1].content)
    response_format = body["response_format"]
    assert response_format["type"] == "json_schema"
    schema_text = json.dumps(response_format["json_schema"]["schema"])
    assert "$defs" not in schema_text, "StructuredDict must inline $defs"
    assert "$ref" not in schema_text, "StructuredDict must inline $ref"


async def test_live_concurrent_turns_overlap() -> None:
    """Check 8 against the real rig: turns run concurrently, not serialised.

    Wall-clock overlap is the assertion. A lock would start each turn after the
    previous one ended, leaving the intervals disjoint.
    """
    router = resolve_router_url()
    model = await resolve_model(LIVE_MODEL_REF, router_url=router)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="batch")
    loop = asyncio.get_event_loop()
    intervals: list[tuple[float, float]] = []

    async def one(index: int) -> None:
        turn = Turn(
            workspace="/repo",
            thread_id=thread.id,
            agent_spec={"instructions": "Answer in one short sentence."},
            agent_name="batch",
            model=LIVE_MODEL_REF,
            prompt="Name a colour.",
            router_url=router,
            n_ctx=32768,
        )
        started = loop.time()
        await run_turn(
            turn,
            step_store=step_store,
            product_store=product,
            run_id=f"run-batch-{index}",
            model=model,
        )
        intervals.append((started, loop.time()))

    await asyncio.gather(*(one(index) for index in range(4)))
    assert max(start for start, _ in intervals) < min(end for _, end in intervals), (
        f"concurrent turns did not overlap: {intervals}"
    )


async def test_live_default_fanout_parallelism_uses_the_advertised_slot_count() -> None:
    """The no-value ``-j`` path must use the resident model's declaration."""
    router = resolve_router_url()
    client = InferferenceClient(router)
    try:
        capabilities = await client.capabilities()
    finally:
        await client.aclose()
    _, info = resolve_capability_model(capabilities, LIVE_MODEL)
    slots = await resolve_parallelism(LIVE_MODEL_REF, jobs=None, router_url=router)

    assert info.n_parallel is not None and slots == info.n_parallel


def test_live_cli_no_value_jobs_uses_the_advertised_slot_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the actual Typer path, including ``pipe --each -j``."""
    workspace = tmp_path / "workspace"
    (workspace / ".agentman").mkdir(parents=True)
    (workspace / ".agentman" / CONFIG_FILENAME).write_text(f"agent: default\nmodel: {LIVE_MODEL_REF}\n")
    (workspace / ".agentman" / "default.yaml").write_text("instructions: answer briefly\n")

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    socket_path = tmp_path / "agentman.sock"
    (config_home / "agentman" / CONFIG_FILENAME).write_text(f"socket: {socket_path}\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(workspace)

    expected = asyncio.run(resolve_parallelism(LIVE_MODEL_REF, jobs=None, router_url=resolve_router_url()))

    class FakeClient:
        def __init__(self) -> None:
            self.concurrency: int | None = None
            self.requests = []

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def new_thread(self, *, workspace: str, name: str | None = None) -> ThreadInfo:
            now = datetime.now(UTC)
            return ThreadInfo(
                id="thread-live",
                workspace=workspace,
                name=name or "",
                created_at=now,
                updated_at=now,
                short_id="thread-l",
            )

        async def stream_turns(self, requests, *, concurrency: int) -> list[TurnOutcome]:
            self.requests.extend(requests)
            self.concurrency = concurrency
            return [
                TurnOutcome(
                    index=index,
                    response=TurnResponse(run_id=f"run-{index}", thread_id=request.thread_id, output="ok"),
                )
                for index, request in enumerate(requests)
            ]

    client = FakeClient()
    monkeypatch.setattr(cli, "_client", lambda _: client)

    result = CliRunner().invoke(app, ["pipe", "--each", "-j", "--json"], input="one\ntwo\n")

    assert result.exit_code == 0, result.output
    assert client.concurrency == expected
    assert len(client.requests) == 2
    assert {request.thread_id for request in client.requests} == {"thread-live"}
