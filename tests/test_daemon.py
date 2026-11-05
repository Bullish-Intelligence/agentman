"""Hermetic daemon route policy tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from agentman.config import Config, Value
from agentman.daemon import AgentmanApp, _HttpError
from agentman.persistence.store import Run


class _RunStore:
    def __init__(self, run: Run) -> None:
        self.run = run

    async def get_run(self, run_id: str) -> Run | None:
        return self.run if run_id == self.run.id else None


def _run() -> Run:
    now = datetime.now(UTC)
    return Run(
        id="run-1",
        thread_id="thread-1",
        workspace="/workspace/owned",
        agent="reviewer",
        model="mock:model",
        status="running",
        prompt="review",
        output_json=None,
        output_type=None,
        messages_json=None,
        usage=None,
        created_at=now,
        updated_at=now,
    )


def test_resume_rejects_a_request_from_another_workspace() -> None:
    app = AgentmanApp(
        Config(
            agent=Value(None, "default"),
            model=Value(None, "default"),
            database_url=Value("postgresql://unused", "test"),
            socket=Value("/tmp/agentman-test.sock", "test"),
            router_url=Value(None, "default"),
        )
    )
    cast(Any, app).store = _RunStore(_run())

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("the route must reject before sending a response")

        with pytest.raises(_HttpError, match="different workspace") as error:
            await app._handle_resume(
                b'{"workspace":"/workspace/requested","run_id":"run-1"}',
                send,
            )
        assert error.value.status == 409
        assert "/workspace/owned" not in error.value.message
        assert "/workspace/requested" not in error.value.message

    asyncio.run(exercise())
