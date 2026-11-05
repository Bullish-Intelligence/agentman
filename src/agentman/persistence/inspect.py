"""agentman's read model over the harness journal.

Product queries live here, not in ``step_store.py``, so an upstream protocol
change touches the adapter alone. ``list_snapshots`` is not a ``StepStore``
method; it lives here.
"""

from __future__ import annotations

from collections.abc import Sequence

from pydantic_ai_harness.step_persistence import ContinuableSnapshot, StepEvent, ToolEffectRecord
from pydantic_ai_harness.step_persistence.recovery import RecoveryInspection, inspect_recovery
from sqlalchemy import Text, cast, func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from agentman.persistence.step_store import PostgresStepStore, event_from_row, snapshot_from_row
from agentman.persistence.tables import events, messages, snapshots


class Journal:
    """Read-only queries over ``step_persistence.*``."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._step_store = PostgresStepStore(engine)

    async def events(self, *, run_id: str) -> list[StepEvent]:
        async with self._engine.connect() as connection:
            rows = (
                (await connection.execute(select(events).where(events.c.run_id == run_id).order_by(events.c.seq.asc())))
                .mappings()
                .all()
            )
        return [event_from_row(row) for row in rows]

    async def snapshots(self, *, run_id: str, include_interrupted: bool = False) -> list[ContinuableSnapshot]:
        """Return retained snapshots for ``run_id`` in write order."""
        statement = select(snapshots).where(snapshots.c.run_id == run_id)
        if not include_interrupted:
            statement = statement.where(snapshots.c.state == "complete")
        statement = statement.order_by(snapshots.c.seq.asc())
        async with self._engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
            return [await snapshot_from_row(connection, row) for row in rows]

    async def unresolved_tool_effects(self, *, run_id: str) -> list[ToolEffectRecord]:
        """Return harness recovery facts for unresolved tool calls.

        Harness owns this inspection. The stored status stays ``started``;
        agentman does not invent a fourth state and never silently replays.
        """
        return list((await self.recovery(run_id=run_id)).unresolved)

    async def recovery(self, *, run_id: str) -> RecoveryInspection:
        """Return harness-owned recovery facts for one run."""
        return await inspect_recovery(store=self._step_store, run_id=run_id)

    async def message_stats(self, *, run_ids: Sequence[str]) -> tuple[int, int]:
        """Return ``(row_count, byte_count)`` for message rows in these runs."""
        if not run_ids:
            return 0, 0
        statement = select(
            func.count(messages.c.content_hash),
            func.coalesce(func.sum(func.octet_length(cast(messages.c.message, Text))), 0),
        ).where(messages.c.run_id.in_(run_ids))
        async with self._engine.connect() as connection:
            row = (await connection.execute(statement)).one()
        return int(row[0]), int(row[1])


__all__ = ["Journal"]
