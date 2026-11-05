"""The harness ``StepStore`` adapter over Postgres.

This module implements the protocol exactly and invents no lifecycle rules.
Recovery policy and product queries live elsewhere (``inspect.py``). Semantics
are copied from the SQLite and Mongo reference stores, because they are
correctness rather than style:

* ``register_run`` inserts and never upserts; a duplicate run id raises.
* Events suppress duplicates on ``(run_id, idempotency_key)``.
* Snapshot identity is a storage ``seq``, never ``step_index``.
* The snapshot idempotency ledger survives pruning.
* Tool effects upsert on ``(run_id, tool_call_id)``.

Messages are content-addressed. A snapshot row holds an ordered array of
content hashes, not the message payload; each distinct message is stored once
per run however many snapshots cite it. A retry can rewrite part of a history,
so a positional prefix-append scheme would silently corrupt; a changed message
has a changed hash and a fork is visible in the data.

**One transaction per store method. Never one transaction per run.** A
transaction spanning a run would hold a connection for the whole turn and
serialise concurrent turns.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from pydantic_ai_harness.step_persistence import (
    ContinuableSnapshot,
    RunRecord,
    StepEvent,
    ToolEffectRecord,
)
from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from agentman.persistence.tables import (
    events,
    messages,
    runs,
    snapshot_idempotency_keys,
    snapshots,
    tool_effects,
)


def message_documents(model_messages: list[ModelMessage]) -> list[tuple[str, dict[str, Any]]]:
    """Return ``(content_hash, document)`` for each message, in order."""
    documents: list[tuple[str, dict[str, Any]]] = []
    for message in model_messages:
        raw = ModelMessagesTypeAdapter.dump_json([message]).decode("utf-8")
        document = json.loads(raw)[0]
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        documents.append((digest, document))
    return documents


def messages_from_documents(documents: list[dict[str, Any]]) -> list[ModelMessage]:
    """Rebuild a ``list[ModelMessage]`` from stored documents."""
    return ModelMessagesTypeAdapter.validate_python(documents)


class PostgresStepStore:
    """``StepStore`` over the ``step_persistence`` schema."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # -- runs ------------------------------------------------------------------

    async def register_run(self, record: RunRecord) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                insert(runs).values(
                    run_id=record.run_id,
                    conversation_id=record.conversation_id,
                    parent_run_id=record.parent_run_id,
                    agent_name=record.agent_name,
                    metadata=dict(record.metadata),
                    started_at=record.started_at,
                    registration_id=record.registration_id,
                )
            )

    async def get_run(self, *, run_id: str) -> RunRecord | None:
        async with self._engine.connect() as connection:
            row = (await connection.execute(select(runs).where(runs.c.run_id == run_id))).mappings().first()
        return _run_from_row(row) if row is not None else None

    async def list_runs(
        self,
        *,
        parent_run_id: str | None = None,
        conversation_id: str | None = None,
    ) -> list[RunRecord]:
        statement = select(runs)
        if parent_run_id is not None:
            statement = statement.where(runs.c.parent_run_id == parent_run_id)
        if conversation_id is not None:
            statement = statement.where(runs.c.conversation_id == conversation_id)
        statement = statement.order_by(runs.c.started_at.asc())
        async with self._engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return [_run_from_row(row) for row in rows]

    # -- events ----------------------------------------------------------------

    async def append_event(self, event: StepEvent) -> None:
        values = {
            "run_id": event.run_id,
            "kind": event.kind,
            "step_index": event.step_index,
            "timestamp": event.timestamp,
            "conversation_id": event.conversation_id,
            "parent_run_id": event.parent_run_id,
            "agent_name": event.agent_name,
            "tool_call_id": event.tool_call_id,
            "tool_name": event.tool_name,
            "error": event.error,
            "metadata": dict(event.metadata),
            "idempotency_key": event.idempotency_key,
        }
        statement = pg_insert(events).values(**values)
        if event.idempotency_key is not None:
            statement = statement.on_conflict_do_nothing(
                index_elements=[events.c.run_id, events.c.idempotency_key],
                index_where=events.c.idempotency_key.is_not(None),
            )
        async with self._engine.begin() as connection:
            await connection.execute(statement)

    async def list_events(self, *, run_id: str) -> list[StepEvent]:
        async with self._engine.connect() as connection:
            rows = (
                (await connection.execute(select(events).where(events.c.run_id == run_id).order_by(events.c.seq.asc())))
                .mappings()
                .all()
            )
        return [event_from_row(row) for row in rows]

    # -- snapshots -------------------------------------------------------------

    async def save_snapshot(self, snapshot: ContinuableSnapshot) -> None:
        documents = message_documents(snapshot.messages)
        hashes = [digest for digest, _ in documents]
        async with self._engine.begin() as connection:
            if snapshot.idempotency_key is not None:
                claim = await connection.execute(
                    pg_insert(snapshot_idempotency_keys)
                    .values(run_id=snapshot.run_id, idempotency_key=snapshot.idempotency_key)
                    .on_conflict_do_nothing(
                        index_elements=[
                            snapshot_idempotency_keys.c.run_id,
                            snapshot_idempotency_keys.c.idempotency_key,
                        ]
                    )
                )
                if claim.rowcount == 0:
                    return
            if documents:
                await connection.execute(
                    pg_insert(messages)
                    .values(
                        [
                            {"run_id": snapshot.run_id, "content_hash": digest, "message": document}
                            for digest, document in documents
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=[messages.c.run_id, messages.c.content_hash])
                )
            await connection.execute(
                insert(snapshots).values(
                    run_id=snapshot.run_id,
                    step_index=snapshot.step_index,
                    conversation_id=snapshot.conversation_id,
                    parent_run_id=snapshot.parent_run_id,
                    agent_name=snapshot.agent_name,
                    timestamp=snapshot.timestamp,
                    state=snapshot.state,
                    idempotency_key=snapshot.idempotency_key,
                    message_hashes=hashes,
                )
            )

    async def latest_snapshot(self, *, run_id: str, include_interrupted: bool = False) -> ContinuableSnapshot | None:
        statement = select(snapshots).where(snapshots.c.run_id == run_id)
        if not include_interrupted:
            statement = statement.where(snapshots.c.state == "complete")
        statement = statement.order_by(snapshots.c.seq.desc()).limit(1)
        async with self._engine.connect() as connection:
            row = (await connection.execute(statement)).mappings().first()
            if row is None:
                return None
            return await snapshot_from_row(connection, row)

    # -- tool effects ----------------------------------------------------------

    async def record_tool_effect(self, record: ToolEffectRecord) -> None:
        values = {
            "run_id": record.run_id,
            "tool_call_id": record.tool_call_id,
            "tool_name": record.tool_name,
            "status": record.status,
            "started_at": record.started_at,
            "ended_at": record.ended_at,
            "idempotency_key": record.idempotency_key,
            "effect_summary": record.effect_summary,
        }
        statement = pg_insert(tool_effects).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=[tool_effects.c.run_id, tool_effects.c.tool_call_id],
            set_={
                "tool_name": statement.excluded.tool_name,
                "status": statement.excluded.status,
                "started_at": statement.excluded.started_at,
                "ended_at": statement.excluded.ended_at,
                "idempotency_key": statement.excluded.idempotency_key,
                "effect_summary": statement.excluded.effect_summary,
            },
        )
        async with self._engine.begin() as connection:
            await connection.execute(statement)

    async def get_tool_effect(self, *, run_id: str, tool_call_id: str) -> ToolEffectRecord | None:
        async with self._engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(tool_effects).where(
                            tool_effects.c.run_id == run_id, tool_effects.c.tool_call_id == tool_call_id
                        )
                    )
                )
                .mappings()
                .first()
            )
        return tool_effect_from_row(row) if row is not None else None

    async def list_unresolved_tool_effects(self, *, run_id: str) -> list[ToolEffectRecord]:
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(tool_effects).where(tool_effects.c.run_id == run_id, tool_effects.c.status == "started")
                    )
                )
                .mappings()
                .all()
            )
        return [tool_effect_from_row(row) for row in rows]


async def snapshot_from_row(connection: Any, row: Any) -> ContinuableSnapshot:
    hashes: list[str] = list(row["message_hashes"])
    documents: list[dict[str, Any]] = []
    if hashes:
        found = (
            (
                await connection.execute(
                    select(messages.c.content_hash, messages.c.message).where(
                        messages.c.run_id == row["run_id"], messages.c.content_hash.in_(hashes)
                    )
                )
            )
            .mappings()
            .all()
        )
        by_hash = {record["content_hash"]: record["message"] for record in found}
        missing = [digest for digest in hashes if digest not in by_hash]
        if missing:  # pragma: no cover - messages are never pruned
            raise RuntimeError(f"snapshot references {len(missing)} missing message(s) for run {row['run_id']}")
        documents = [by_hash[digest] for digest in hashes]
    return ContinuableSnapshot(
        run_id=row["run_id"],
        step_index=row["step_index"],
        messages=messages_from_documents(documents),
        conversation_id=row["conversation_id"],
        parent_run_id=row["parent_run_id"],
        agent_name=row["agent_name"],
        timestamp=row["timestamp"],
        state=row["state"],
        idempotency_key=row["idempotency_key"],
    )


def _run_from_row(row: Any) -> RunRecord:
    return RunRecord(
        run_id=row["run_id"],
        conversation_id=row["conversation_id"],
        parent_run_id=row["parent_run_id"],
        agent_name=row["agent_name"],
        metadata=dict(row["metadata"]),
        started_at=row["started_at"],
        registration_id=row["registration_id"],
    )


def event_from_row(row: Any) -> StepEvent:
    return StepEvent(
        run_id=row["run_id"],
        kind=row["kind"],
        step_index=row["step_index"],
        timestamp=row["timestamp"],
        conversation_id=row["conversation_id"],
        parent_run_id=row["parent_run_id"],
        agent_name=row["agent_name"],
        tool_call_id=row["tool_call_id"],
        tool_name=row["tool_name"],
        error=row["error"],
        metadata=dict(row["metadata"]),
        idempotency_key=row["idempotency_key"],
    )


def tool_effect_from_row(row: Any) -> ToolEffectRecord:
    return ToolEffectRecord(
        run_id=row["run_id"],
        tool_call_id=row["tool_call_id"],
        tool_name=row["tool_name"],
        status=row["status"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        idempotency_key=row["idempotency_key"],
        effect_summary=row["effect_summary"],
    )


__all__ = [
    "PostgresStepStore",
    "event_from_row",
    "message_documents",
    "messages_from_documents",
    "snapshot_from_row",
    "tool_effect_from_row",
]
