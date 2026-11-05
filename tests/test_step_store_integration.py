"""Phase B: the harness ``StepStore`` adapter against a real Postgres.

Hermetic tests cover the message codec and the protocol conformance. The rest
carry the ``integration`` marker.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, UserPromptPart
from pydantic_ai_harness.step_persistence import (
    ContinuableSnapshot,
    InMemoryStepStore,
    RunRecord,
    StepEvent,
    StepStore,
    ToolEffectRecord,
)
from pydantic_ai_harness.step_persistence._context import current_run_id
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from agentman.effects import FileEffectEvidence
from agentman.persistence.engine import create_engine
from agentman.persistence.step_store import (
    PostgresStepStore,
    message_documents,
    messages_from_documents,
)
from agentman.persistence.tables import messages, snapshot_idempotency_keys, snapshots, tool_effects

pytestmark = pytest.mark.integration


def test_adapter_satisfies_the_runtime_checkable_protocol() -> None:
    store = object.__new__(PostgresStepStore)
    assert isinstance(store, StepStore)


def test_message_codec_is_lossless_and_content_addressed() -> None:
    history: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content="hello")]),
        ModelResponse(parts=[TextPart(content="world")]),
        ModelRequest(parts=[UserPromptPart(content="again")]),
    ]
    documents = message_documents(history)
    assert len(documents) == 3
    assert len({digest for digest, _ in documents}) == 3
    restored = messages_from_documents([document for _, document in documents])
    assert restored == history


def test_repeated_message_is_one_document() -> None:
    message = ModelRequest(parts=[UserPromptPart(content="same")])
    documents = message_documents([message, message])
    assert documents[0][0] == documents[1][0]


@pytest.fixture()
async def engine(database_url: str) -> Any:
    engine = create_engine(database_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture()
async def clean(engine: Any) -> None:
    from sqlalchemy import delete

    from agentman.persistence.tables import run, runs, thread

    async with engine.begin() as connection:
        for table in (
            snapshot_idempotency_keys,
            snapshots,
            messages,
            runs,
            run,
            thread,
        ):
            await connection.execute(delete(table))


def _run_record(run_id: str) -> RunRecord:
    return RunRecord(run_id=run_id, agent_name="test", registration_id="registration")


async def test_register_run_inserts_and_duplicate_raises(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    await store.register_run(_run_record(run_id))
    with pytest.raises(IntegrityError):
        await store.register_run(_run_record(run_id))
    fetched = await store.get_run(run_id=run_id)
    assert fetched is not None and fetched.agent_name == "test"


async def test_repeated_event_with_same_key_inserts_one_row(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    event = StepEvent(run_id=run_id, kind="run_started", step_index=0, idempotency_key="0:0:run_started:")
    await store.append_event(event)
    await store.append_event(event)
    stored = await store.list_events(run_id=run_id)
    assert len(stored) == 1


async def test_snapshot_saved_twice_inserts_one_row(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    snapshot = ContinuableSnapshot(
        run_id=run_id,
        step_index=1,
        messages=[ModelRequest(parts=[UserPromptPart(content="hi")])],
        idempotency_key="0:1:complete",
    )
    await store.save_snapshot(snapshot)
    await store.save_snapshot(snapshot)
    async with engine.connect() as connection:
        rows = (await connection.execute(select(snapshots).where(snapshots.c.run_id == run_id))).mappings().all()
    assert len(rows) == 1


async def test_idempotency_ledger_survives_snapshot_deletion(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    snapshot = ContinuableSnapshot(
        run_id=run_id,
        step_index=1,
        messages=[ModelRequest(parts=[UserPromptPart(content="hi")])],
        idempotency_key="0:1:complete",
    )
    await store.save_snapshot(snapshot)
    from sqlalchemy import delete

    async with engine.begin() as connection:
        await connection.execute(delete(snapshots).where(snapshots.c.run_id == run_id))
    await store.save_snapshot(snapshot)
    async with engine.connect() as connection:
        row_count = len((await connection.execute(select(snapshots).where(snapshots.c.run_id == run_id))).all())
    assert row_count == 0


async def test_latest_snapshot_gates_interrupted(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    complete = ContinuableSnapshot(
        run_id=run_id,
        step_index=1,
        messages=[ModelRequest(parts=[UserPromptPart(content="complete")])],
        state="complete",
        idempotency_key="0:1:complete",
    )
    interrupted = ContinuableSnapshot(
        run_id=run_id,
        step_index=2,
        messages=[ModelRequest(parts=[UserPromptPart(content="interrupted")])],
        state="interrupted",
        idempotency_key="1:2:interrupted",
    )
    await store.save_snapshot(complete)
    await store.save_snapshot(interrupted)
    latest = await store.latest_snapshot(run_id=run_id)
    assert latest is not None and latest.step_index == 1
    newest = await store.latest_snapshot(run_id=run_id, include_interrupted=True)
    assert newest is not None and newest.step_index == 2


async def test_history_of_n_messages_across_m_snapshots_stores_n_rows(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    first = ModelRequest(parts=[UserPromptPart(content="one")])
    second = ModelRequest(parts=[UserPromptPart(content="two")])
    third = ModelResponse(parts=[TextPart(content="three")])
    await store.save_snapshot(
        ContinuableSnapshot(run_id=run_id, step_index=0, messages=[first], idempotency_key="0:0:complete")
    )
    await store.save_snapshot(
        ContinuableSnapshot(run_id=run_id, step_index=1, messages=[first, second], idempotency_key="1:1:complete")
    )
    await store.save_snapshot(
        ContinuableSnapshot(
            run_id=run_id, step_index=2, messages=[first, second, third], idempotency_key="2:2:complete"
        )
    )
    async with engine.connect() as connection:
        rows = (await connection.execute(select(messages).where(messages.c.run_id == run_id))).all()
    assert len(rows) == 3
    latest = await store.latest_snapshot(run_id=run_id)
    assert latest is not None
    assert latest.messages == [first, second, third]


async def test_snapshot_round_trip_is_lossless(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    history: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content="hi")]),
        ModelResponse(parts=[TextPart(content="there")]),
    ]
    await store.save_snapshot(
        ContinuableSnapshot(
            run_id=run_id,
            step_index=0,
            messages=history,
            conversation_id="thread-1",
            agent_name="test",
            idempotency_key="0:0:complete",
        )
    )
    latest = await store.latest_snapshot(run_id=run_id)
    assert latest is not None
    assert latest.messages == history
    assert latest.conversation_id == "thread-1"


async def test_tool_effect_upserts_on_run_and_tool_call(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    started = ToolEffectRecord(run_id=run_id, tool_call_id="call-1", tool_name="write", status="started")
    await store.record_tool_effect(started)
    finished = ToolEffectRecord(
        run_id=run_id,
        tool_call_id="call-1",
        tool_name="write",
        status="completed",
        ended_at=datetime.now(UTC),
    )
    await store.record_tool_effect(finished)
    fetched = await store.get_tool_effect(run_id=run_id, tool_call_id="call-1")
    assert fetched is not None and fetched.status == "completed"
    assert await store.list_unresolved_tool_effects(run_id=run_id) == []


async def test_unresolved_effect_is_reported(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    await store.record_tool_effect(
        ToolEffectRecord(run_id=run_id, tool_call_id="call-1", tool_name="write", status="started")
    )
    unresolved = await store.list_unresolved_tool_effects(run_id=run_id)
    assert [record.tool_call_id for record in unresolved] == ["call-1"]


async def test_file_effect_evidence_reaches_postgres(engine: Any, clean: None, tmp_path: Any) -> None:
    store = PostgresStepStore(engine)
    run_id = uuid4().hex
    await store.record_tool_effect(
        ToolEffectRecord(run_id=run_id, tool_call_id="call-1", tool_name="write_file", status="started")
    )
    capability = FileEffectEvidence(store=store, workspace=str(tmp_path))
    ctx = cast(Any, SimpleNamespace(run_id=run_id, tool_call_id="call-1", tool_name="write_file"))
    tool_def = cast(Any, SimpleNamespace(name="write_file"))
    call = cast(Any, SimpleNamespace())
    token = current_run_id.set(run_id)
    try:
        args = {"path": "new.txt", "content": "hello"}
        await capability.before_tool_execute(ctx, call=call, tool_def=tool_def, args=args)
        (tmp_path / "new.txt").write_text("hello")
        await capability.after_tool_execute(ctx, call=call, tool_def=tool_def, args=args, result="ok")
    finally:
        current_run_id.reset(token)

    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    select(tool_effects).where(
                        tool_effects.c.run_id == run_id,
                        tool_effects.c.tool_call_id == "call-1",
                    )
                )
            )
            .mappings()
            .one()
        )
    assert row["idempotency_key"] is None
    assert row["effect_summary"] is not None
    summary = json.loads(row["effect_summary"])
    assert summary["pre_image"] == "absent"
    assert summary["post_image"] == hashlib.sha256(b"hello").hexdigest()


async def test_agentman_store_uses_the_adapter_and_in_memory_store_agrees(engine: Any, clean: None) -> None:
    """The two stores agree on the parts slice 1 exercises."""
    postgres = PostgresStepStore(engine)
    memory = InMemoryStepStore()
    run_id = uuid4().hex
    snapshot = ContinuableSnapshot(
        run_id=run_id,
        step_index=0,
        messages=[ModelRequest(parts=[UserPromptPart(content="hi")])],
        idempotency_key="0:0:complete",
    )
    for store in (postgres, memory):
        await store.register_run(_run_record(run_id))
        await store.save_snapshot(snapshot)
        latest = await store.latest_snapshot(run_id=run_id)
        assert latest is not None and latest.messages == snapshot.messages
