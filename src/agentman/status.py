"""Compose agentman's product state with the journal's public state.

``status`` surfaces what is unresolved, without inventing a fourth state. The
stored harness status stays ``started``; agentman reports that as
``unknown_after_crash`` and never silently replays.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai.messages import ModelMessagesTypeAdapter

from agentman.persistence.inspect import Journal
from agentman.persistence.store import Run, Store, Thread
from agentman.protocol import RunError, RunInfo, RunMetadata, StatusView, ThreadInfo, UnresolvedEffect


async def thread_info(store: Store, thread: Thread) -> ThreadInfo:
    runs = await store.list_runs(thread_id=thread.id)
    return ThreadInfo(
        id=thread.id,
        workspace=thread.workspace,
        name=thread.name,
        created_at=thread.created_at,
        updated_at=thread.updated_at,
        short_id=thread.id[:8],
        runs=len(runs),
    )


def run_info(run: Run) -> RunInfo:
    return RunInfo(
        id=run.id,
        thread_id=run.thread_id,
        workspace=run.workspace,
        agent=run.agent,
        model=run.model,
        status=run.status,
        prompt=run.prompt,
        output_json=run.output_json,
        output_type=run.output_type,
        run_metadata=RunMetadata.model_validate(run.run_metadata or {}),
        usage=run.usage,
        error=RunError.model_validate(run.error) if run.error is not None else None,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


async def thread_messages(step_store: Any, runs: list[Run]) -> list[dict[str, Any]]:
    """Return the newest thread history as ``ModelMessage`` JSON.

    The snapshot is preferred, because it is the record harness maintains.
    """
    if not runs:
        return []
    latest = runs[-1]
    snapshot = await step_store.latest_snapshot(run_id=latest.id)
    if snapshot is None:
        return list(latest.messages_json or [])
    return ModelMessagesTypeAdapter.dump_python(snapshot.messages, mode="json")


async def compose_status(
    *,
    store: Store,
    journal: Journal,
    workspace: str,
    thread_id: str | None = None,
    name: str | None = None,
) -> StatusView:
    """Compose thread and run state plus unresolved tool effects."""
    thread: Thread | None = None
    if thread_id is not None:
        thread = await store.get_thread(thread_id)
    elif name is not None:
        thread = await store.resolve_thread_name(workspace=workspace, name=name)
    if thread is None:
        return StatusView()

    runs = await store.list_runs(thread_id=thread.id)
    latest = runs[-1] if runs else None
    unresolved: list[UnresolvedEffect] = []
    events = 0
    snapshots = 0
    message_rows = 0
    message_bytes = 0
    if runs:
        message_rows, message_bytes = await journal.message_stats(run_ids=[run.id for run in runs])
        for run in runs:
            unresolved.extend(
                UnresolvedEffect(
                    run_id=run.id,
                    tool_call_id=effect.tool_call_id,
                    tool_name=effect.tool_name,
                    status=effect.status,
                    started_at=effect.started_at,
                )
                for effect in await journal.unresolved_tool_effects(run_id=run.id)
            )
    if latest is not None:
        events = len(await journal.events(run_id=latest.id))
        snapshots = len(await journal.snapshots(run_id=latest.id, include_interrupted=True))
    return StatusView(
        thread=await thread_info(store, thread),
        latest_run=run_info(latest) if latest is not None else None,
        unresolved_effects=unresolved,
        events=events,
        snapshots=snapshots,
        message_rows=message_rows,
        message_bytes=message_bytes,
    )


__all__ = ["compose_status", "run_info", "thread_info", "thread_messages"]
