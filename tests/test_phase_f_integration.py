"""Phase F: checks that need real Postgres.

Checks 2 and 7 are covered in ``test_store_integration.py`` and
``test_step_store_integration.py``. Check 1 is here: a thread and its runs must
survive the daemon going away, which is what a fresh engine proves.
"""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from pydantic_ai.messages import ModelRequest, UserPromptPart
from sqlalchemy import delete
from support.openai_transport import MockOpenAI, build_mock_model

from agentman.persistence.engine import create_engine
from agentman.persistence.step_store import PostgresStepStore
from agentman.persistence.store import RUN_COMPLETED, Store
from agentman.persistence.tables import snapshots
from agentman.runtime import Turn, load_history, run_turn

pytestmark = pytest.mark.integration

SCHEMA = {
    "type": "object",
    "properties": {"title": {"type": "string"}, "rating": {"type": "integer"}},
    "required": ["title", "rating"],
}


async def test_thread_and_run_survive_a_fresh_engine(database_url: str) -> None:
    workspace = f"/repo/restart-{uuid4().hex}"
    engine = create_engine(database_url)
    store = Store(engine)
    step_store = PostgresStepStore(engine)
    thread = await store.create_thread(workspace=workspace, name="parser")
    mock = MockOpenAI(content=json.dumps({"title": "x", "rating": 1}))
    turn = Turn(
        workspace=workspace,
        thread_id=thread.id,
        agent_spec={"instructions": "x"},
        agent_name="parser",
        model="mock:model",
        prompt="hi",
        output_schema=SCHEMA,
    )
    await run_turn(
        turn,
        step_store=step_store,
        product_store=store,
        run_id="run-restart",
        model=build_mock_model(mock),
    )
    await engine.dispose()

    restarted = create_engine(database_url)
    try:
        restarted_store = Store(restarted)
        threads = await restarted_store.list_threads(workspace=workspace)
        assert [found.id for found in threads] == [thread.id]
        run = await restarted_store.get_run("run-restart")
        assert run is not None
        assert run.status == RUN_COMPLETED
    finally:
        await restarted.dispose()


async def test_two_repositories_use_the_same_thread_name_without_collision(database_url: str) -> None:
    engine = create_engine(database_url)
    try:
        store = Store(engine)
        first_workspace = f"/repo/one-{uuid4().hex}"
        second_workspace = f"/repo/two-{uuid4().hex}"
        first = await store.create_thread(workspace=first_workspace, name="shared")
        second = await store.create_thread(workspace=second_workspace, name="shared")
        assert first.id != second.id
        assert [thread.id for thread in await store.list_threads(workspace=first_workspace)] == [first.id]
        assert [thread.id for thread in await store.list_threads(workspace=second_workspace)] == [second.id]
        resolved = await store.resolve_thread_name(workspace=first_workspace, name="shared")
        assert resolved is not None and resolved.id == first.id
    finally:
        await engine.dispose()


async def test_run_turn_with_a_real_postgres_journal(database_url: str) -> None:
    engine = create_engine(database_url)
    try:
        store = Store(engine)
        step_store = PostgresStepStore(engine)
        thread = await store.create_thread(workspace=f"/repo/journal-{uuid4().hex}", name="j")
        mock = MockOpenAI(content=json.dumps({"title": "x", "rating": 1}))
        turn = Turn(
            workspace=thread.workspace,
            thread_id=thread.id,
            agent_spec={"instructions": "x"},
            agent_name="journal",
            model="mock:model",
            prompt="hi",
            output_schema=SCHEMA,
        )
        await run_turn(
            turn,
            step_store=step_store,
            product_store=store,
            run_id="run-journal",
            model=build_mock_model(mock),
        )
        snapshot = await step_store.latest_snapshot(run_id="run-journal")
        assert snapshot is not None
        assert snapshot.conversation_id == thread.id
        events = await step_store.list_events(run_id="run-journal")
        assert {event.kind for event in events} >= {"run_started", "run_completed"}
    finally:
        await engine.dispose()


async def test_load_history_falls_back_to_messages_json_without_a_snapshot(database_url: str) -> None:
    """F9: the ``messages_json`` fallback is reachable when a snapshot is gone."""
    engine = create_engine(database_url)
    try:
        store = Store(engine)
        step_store = PostgresStepStore(engine)
        thread = await store.create_thread(workspace=f"/repo/fallback-{uuid4().hex}", name="fallback")
        run_id = f"run-fallback-{uuid4().hex}"
        mock = MockOpenAI(content=json.dumps({"title": "x", "rating": 1}))
        turn = Turn(
            workspace=thread.workspace,
            thread_id=thread.id,
            agent_spec={"instructions": "x"},
            agent_name="fallback",
            model="mock:model",
            prompt="hi",
            output_schema=SCHEMA,
        )
        await run_turn(
            turn,
            step_store=step_store,
            product_store=store,
            run_id=run_id,
            model=build_mock_model(mock),
        )
        assert await step_store.latest_snapshot(run_id=run_id) is not None

        # Delete the snapshot. The run record and ``messages_json`` stay.
        async with engine.begin() as connection:
            await connection.execute(delete(snapshots).where(snapshots.c.run_id == run_id))
        assert await step_store.latest_snapshot(run_id=run_id) is None

        history = await load_history(step_store, store, thread.id)
        assert history, "the fallback must reconstruct the history from messages_json"
        prompts = [
            part.content
            for message in history
            if isinstance(message, ModelRequest)
            for part in message.parts
            if isinstance(part, UserPromptPart)
        ]
        assert "hi" in prompts
        run = await store.get_run(run_id)
        assert run is not None and run.messages_json
    finally:
        await engine.dispose()
