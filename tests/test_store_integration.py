"""Phase B: agentman thread and run CRUD, and the journal read model."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic_ai.messages import ModelRequest, UserPromptPart
from pydantic_ai_harness.step_persistence import ContinuableSnapshot, ToolEffectRecord
from sqlalchemy import delete

from agentman.persistence.engine import create_engine
from agentman.persistence.inspect import Journal
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import RUN_CANCELLED, RUN_COMPLETED, Store, short_id
from agentman.persistence.tables import messages, run, runs, snapshot_idempotency_keys, snapshots, thread
from agentman.status import compose_status

pytestmark = pytest.mark.integration


@pytest.fixture()
async def engine(database_url: str) -> Any:
    engine = create_engine(database_url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture()
async def clean(engine: Any) -> None:
    async with engine.begin() as connection:
        for table in (snapshot_idempotency_keys, snapshots, messages, runs, run, thread):
            await connection.execute(delete(table))


async def test_create_and_list_threads_are_workspace_scoped(engine: Any, clean: None) -> None:
    store = Store(engine)
    await store.create_thread(workspace="/repo/one", name="parser")
    await store.create_thread(workspace="/repo/two", name="parser")

    first = await store.list_threads(workspace="/repo/one")
    second = await store.list_threads(workspace="/repo/two")
    assert [t.name for t in first] == ["parser"]
    assert [t.name for t in second] == ["parser"]
    assert first[0].id != second[0].id


async def test_same_name_resolves_to_most_recently_updated(engine: Any, clean: None) -> None:
    store = Store(engine)
    older = await store.create_thread(workspace="/repo", name="parser")
    await store.create_thread(workspace="/repo", name="parser")
    newest = await store.create_thread(workspace="/repo", name="parser")
    await store.touch_thread(newest.id)
    resolved = await store.resolve_thread_name(workspace="/repo", name="parser")
    assert resolved is not None
    assert resolved.id == newest.id
    assert older.id != resolved.id


async def test_short_id_is_the_first_eight_characters() -> None:
    assert short_id("0123456789abcdef") == "01234567"


async def test_run_records_agent_and_model_per_run(engine: Any, clean: None) -> None:
    store = Store(engine)
    created = await store.create_thread(workspace="/repo", name="t")
    first = await store.create_run(
        run_id="run-1",
        thread_id=created.id,
        workspace="/repo",
        agent="reviewer",
        model="inferference:gemma",
        prompt="hello",
        run_metadata={"requested_max_transport_retries": 0, "effective_max_transport_retries": 0},
    )
    assert first.status == "running"
    await store.update_run(
        "run-1",
        status=RUN_COMPLETED,
        output_json={"text": "hi"},
        output_type="str",
        messages_json=[{"role": "user"}],
        usage={"total_tokens": 3},
    )
    fetched = await store.get_run("run-1")
    assert fetched is not None
    assert fetched.status == RUN_COMPLETED
    assert fetched.output_json == {"text": "hi"}
    assert fetched.run_metadata == {
        "requested_max_transport_retries": 0,
        "effective_max_transport_retries": 0,
    }
    runs = await store.list_runs(thread_id=created.id)
    assert [record.id for record in runs] == ["run-1"]


async def test_run_error_round_trips_through_store(engine: Any, clean: None) -> None:
    store = Store(engine)
    created = await store.create_thread(workspace="/repo", name="t")
    await store.create_run(
        run_id="run-cancelled",
        thread_id=created.id,
        workspace="/repo",
        agent="reviewer",
        model="openai:slow",
        prompt="hello",
    )
    error = {"reason": "cancel_requested", "type": None, "message": None}

    await store.update_run("run-cancelled", status=RUN_CANCELLED, error=error)

    fetched = await store.get_run("run-cancelled")
    assert fetched is not None
    assert fetched.status == RUN_CANCELLED
    assert fetched.error == error


async def test_finish_run_writes_only_while_the_run_is_running(engine: Any, clean: None) -> None:
    store = Store(engine)
    created = await store.create_thread(workspace="/repo", name="t")
    await store.create_run(
        run_id="run-final",
        thread_id=created.id,
        workspace="/repo",
        agent="reviewer",
        model="openai:slow",
        prompt="hello",
    )
    first = {"reason": "cancel_requested", "type": None, "message": None}
    second = {"reason": "workflow_cancelled", "type": None, "message": None}

    assert await store.finish_run("run-final", status=RUN_CANCELLED, error=first, usage={"requests": 1})
    assert not await store.finish_run("run-final", status=RUN_COMPLETED, output_json={"late": True})
    assert not await store.finish_run("run-final", status=RUN_CANCELLED, error=second)
    assert not await store.finish_run("run-absent", status=RUN_CANCELLED, error=second)

    fetched = await store.get_run("run-final")
    assert fetched is not None
    assert fetched.status == RUN_CANCELLED
    assert fetched.error == first
    assert fetched.usage == {"requests": 1}
    assert fetched.output_json is None


async def test_finish_run_refuses_a_status_that_is_not_final(engine: Any, clean: None) -> None:
    with pytest.raises(ValueError, match="final status"):
        await Store(engine).finish_run("run-1", status="running")


async def test_postgres_accepts_the_sanitized_text_of_a_hostile_error(engine: Any, clean: None) -> None:
    from agentman.persistence.store import RUN_FAILED
    from agentman.runtime import exception_error

    store = Store(engine)
    created = await store.create_thread(workspace="/repo", name="t")
    await store.create_run(
        run_id="run-hostile",
        thread_id=created.id,
        workspace="/repo",
        agent="reviewer",
        model="openai:slow",
        prompt="hello",
    )
    error = exception_error(RuntimeError("nul\x00 and lone surrogate \ud800"))

    assert await store.finish_run("run-hostile", status=RUN_FAILED, error=error)

    fetched = await store.get_run("run-hostile")
    assert fetched is not None and fetched.status == RUN_FAILED
    assert fetched.error == error


async def test_journal_lists_events_snapshots_and_unresolved_effects(engine: Any, clean: None) -> None:
    store = PostgresStepStore(engine)
    journal = Journal(engine)
    run_id = "run-journal"
    await store.save_snapshot(
        ContinuableSnapshot(
            run_id=run_id,
            step_index=0,
            messages=[ModelRequest(parts=[UserPromptPart(content="hi")])],
            idempotency_key="0:0:complete",
        )
    )
    snapshots_found = await journal.snapshots(run_id=run_id)
    assert len(snapshots_found) == 1
    assert await journal.unresolved_tool_effects(run_id=run_id) == []


async def test_status_reports_unresolved_effects_from_earlier_runs(engine: Any, clean: None) -> None:
    store = Store(engine)
    step_store = PostgresStepStore(engine)
    journal = Journal(engine)
    thread_record = await store.create_thread(workspace="/repo/status", name="status")
    await store.create_run(
        run_id="run-old",
        thread_id=thread_record.id,
        workspace=thread_record.workspace,
        agent="coder",
        model="mock:model",
        prompt="old",
    )
    await store.create_run(
        run_id="run-new",
        thread_id=thread_record.id,
        workspace=thread_record.workspace,
        agent="coder",
        model="mock:model",
        prompt="new",
    )
    await step_store.record_tool_effect(
        ToolEffectRecord(run_id="run-old", tool_call_id="call-1", tool_name="write_file", status="started")
    )
    await step_store.save_snapshot(
        ContinuableSnapshot(
            run_id="run-old",
            step_index=0,
            messages=[ModelRequest(parts=[UserPromptPart(content="old")])],
            idempotency_key="old-snapshot",
        )
    )
    await step_store.save_snapshot(
        ContinuableSnapshot(
            run_id="run-new",
            step_index=0,
            messages=[ModelRequest(parts=[UserPromptPart(content="new")])],
            idempotency_key="new-snapshot",
        )
    )

    view = await compose_status(
        store=store,
        journal=journal,
        workspace=thread_record.workspace,
        thread_id=thread_record.id,
    )

    assert view.latest_run is not None and view.latest_run.id == "run-new"
    assert [(effect.run_id, effect.note) for effect in view.unresolved_effects] == [("run-old", "unknown_after_crash")]
    assert view.message_rows == 2
    assert view.message_bytes > 0
