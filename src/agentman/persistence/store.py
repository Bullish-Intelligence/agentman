"""Thread and run CRUD. Daemon-private.

The daemon is the only Postgres client, so ``ls``, ``show`` and ``status`` are
socket requests, not queries. Thread names are labels, not identities: the
``id`` is the identity and there is no unique constraint on ``name``. ``use
<name>`` resolves to the most recently updated thread with that name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from agentman.persistence.tables import run as run_table
from agentman.persistence.tables import thread as thread_table

RUN_RUNNING = "running"
RUN_COMPLETED = "completed"
RUN_FAILED = "failed"
RUN_CANCELLED = "cancelled"

RUN_TERMINAL = frozenset({RUN_COMPLETED, RUN_FAILED, RUN_CANCELLED})
"""A run in one of these states never runs again."""


@dataclass(frozen=True)
class Thread:
    id: str
    workspace: str
    name: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Run:
    id: str
    thread_id: str
    workspace: str
    agent: str
    model: str
    status: str
    prompt: str
    output_json: Any | None
    output_type: str | None
    messages_json: Any | None
    usage: Any | None
    created_at: datetime
    updated_at: datetime
    run_metadata: dict[str, Any] = field(default_factory=dict)
    error: dict[str, Any] | None = None


def short_id(identifier: str) -> str:
    """Return the short form shown when thread names collide."""
    return identifier[:8]


class Store:
    """Product-facing thread and run persistence."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # -- threads ---------------------------------------------------------------

    async def create_thread(self, *, workspace: str, name: str | None = None) -> Thread:
        now = datetime.now(UTC)
        identifier = uuid4().hex
        label = name or ""
        async with self._engine.begin() as connection:
            await connection.execute(
                insert(thread_table).values(
                    id=identifier,
                    workspace=workspace,
                    name=label,
                    created_at=now,
                    updated_at=now,
                )
            )
        return Thread(id=identifier, workspace=workspace, name=label, created_at=now, updated_at=now)

    async def get_thread(self, thread_id: str) -> Thread | None:
        async with self._engine.connect() as connection:
            row = (
                (await connection.execute(select(thread_table).where(thread_table.c.id == thread_id)))
                .mappings()
                .first()
            )
        return _thread_from_row(row) if row is not None else None

    async def list_threads(self, *, workspace: str) -> list[Thread]:
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(thread_table)
                        .where(thread_table.c.workspace == workspace)
                        .order_by(thread_table.c.updated_at.desc())
                    )
                )
                .mappings()
                .all()
            )
        return [_thread_from_row(row) for row in rows]

    async def resolve_thread_name(self, *, workspace: str, name: str) -> Thread | None:
        """Return the most recently updated thread with ``name`` in ``workspace``."""
        async with self._engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(thread_table)
                        .where(thread_table.c.workspace == workspace, thread_table.c.name == name)
                        .order_by(thread_table.c.updated_at.desc())
                        .limit(1)
                    )
                )
                .mappings()
                .first()
            )
        return _thread_from_row(row) if row is not None else None

    async def touch_thread(self, thread_id: str) -> None:
        async with self._engine.begin() as connection:
            await connection.execute(
                update(thread_table).where(thread_table.c.id == thread_id).values(updated_at=datetime.now(UTC))
            )

    # -- runs ------------------------------------------------------------------

    async def create_run(
        self,
        *,
        run_id: str,
        thread_id: str,
        workspace: str,
        agent: str,
        model: str,
        prompt: str,
        run_metadata: dict[str, Any] | None = None,
        status: str = RUN_RUNNING,
    ) -> Run:
        now = datetime.now(UTC)
        async with self._engine.begin() as connection:
            await connection.execute(
                insert(run_table).values(
                    id=run_id,
                    thread_id=thread_id,
                    workspace=workspace,
                    agent=agent,
                    model=model,
                    status=status,
                    prompt=prompt,
                    run_metadata=run_metadata or {},
                    created_at=now,
                    updated_at=now,
                )
            )
        return Run(
            id=run_id,
            thread_id=thread_id,
            workspace=workspace,
            agent=agent,
            model=model,
            status=status,
            prompt=prompt,
            output_json=None,
            output_type=None,
            messages_json=None,
            usage=None,
            created_at=now,
            updated_at=now,
            run_metadata=run_metadata or {},
        )

    async def update_run(self, run_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = datetime.now(UTC)
        async with self._engine.begin() as connection:
            await connection.execute(update(run_table).where(run_table.c.id == run_id).values(**fields))

    async def finish_run(self, run_id: str, **fields: Any) -> bool:
        """Write the final status of a run only while it is ``running``.

        The first writer of a final status wins. The check and the write are one
        statement, so two writers cannot both succeed. Return ``True`` when this
        call changed the row, and ``False`` when the run was already final or
        does not exist.
        """
        if fields.get("status") not in RUN_TERMINAL:
            raise ValueError("finish_run needs a final status")
        fields["updated_at"] = datetime.now(UTC)
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(run_table).where(run_table.c.id == run_id, run_table.c.status == RUN_RUNNING).values(**fields)
            )
        return result.rowcount > 0

    async def get_run(self, run_id: str) -> Run | None:
        async with self._engine.connect() as connection:
            row = (await connection.execute(select(run_table).where(run_table.c.id == run_id))).mappings().first()
        return _run_from_row(row) if row is not None else None

    async def list_runs(self, *, thread_id: str) -> list[Run]:
        async with self._engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(run_table)
                        .where(run_table.c.thread_id == thread_id)
                        .order_by(run_table.c.created_at.asc())
                    )
                )
                .mappings()
                .all()
            )
        return [_run_from_row(row) for row in rows]


def _thread_from_row(row: Any) -> Thread:
    return Thread(
        id=row["id"],
        workspace=row["workspace"],
        name=row["name"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _run_from_row(row: Any) -> Run:
    return Run(
        id=row["id"],
        thread_id=row["thread_id"],
        workspace=row["workspace"],
        agent=row["agent"],
        model=row["model"],
        status=row["status"],
        prompt=row["prompt"],
        output_json=row["output_json"],
        output_type=row["output_type"],
        messages_json=row["messages_json"],
        usage=row["usage"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        run_metadata=row.get("run_metadata") or {},
        error=row.get("error"),
    )


__all__ = [
    "RUN_CANCELLED",
    "RUN_COMPLETED",
    "RUN_FAILED",
    "RUN_RUNNING",
    "RUN_TERMINAL",
    "Run",
    "Store",
    "Thread",
    "short_id",
]
