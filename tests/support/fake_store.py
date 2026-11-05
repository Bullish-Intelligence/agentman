"""An in-memory product store for hermetic runtime tests.

It mimics the ``Store`` surface ``run_turn`` and ``load_history`` call. It is
not the real store: the real one is daemon-private and needs Postgres.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from agentman.persistence.store import RUN_RUNNING, Run, Thread


class FakeProductStore:
    def __init__(self) -> None:
        self.threads: dict[str, Thread] = {}
        self.runs: dict[str, Run] = {}

    async def create_thread(self, *, workspace: str, name: str | None = None) -> Thread:
        now = datetime.now(UTC)
        thread = Thread(id=uuid4().hex, workspace=workspace, name=name or "", created_at=now, updated_at=now)
        self.threads[thread.id] = thread
        return thread

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
        run = Run(
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
        self.runs[run_id] = run
        return run

    async def update_run(self, run_id: str, **fields: Any) -> None:
        current = self.runs.get(run_id)
        if current is None:
            return
        self.runs[run_id] = dataclasses.replace(current, updated_at=datetime.now(UTC), **fields)

    async def get_run(self, run_id: str) -> Run | None:
        return self.runs.get(run_id)

    async def list_runs(self, *, thread_id: str) -> list[Run]:
        return sorted(
            (run for run in self.runs.values() if run.thread_id == thread_id),
            key=lambda run: run.created_at,
        )

    async def touch_thread(self, thread_id: str) -> None:
        thread = self.threads.get(thread_id)
        if thread is not None:
            self.threads[thread_id] = dataclasses.replace(thread, updated_at=datetime.now(UTC))


__all__ = ["FakeProductStore"]
