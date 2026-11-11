"""Resume authorization from harness recovery facts.

This module decides whether DBOS may resume a run; it never replays a tool.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from pydantic_ai_harness.step_persistence import ToolEffectRecord
from pydantic_ai_harness.step_persistence.recovery import RecoveryInspection

from agentman.effects import ABSENT_FILE, UNKNOWN_IMAGE
from agentman.persistence.store import RUN_TERMINAL


class ResumeRefused(RuntimeError):
    """The recorded facts do not authorize resuming a DBOS workflow."""


class _RunReader(Protocol):
    async def get_run(self, run_id: str) -> Any | None: ...


class _RecoveryReader(Protocol):
    async def recovery(self, *, run_id: str) -> RecoveryInspection: ...


async def authorize_resume(*, store: _RunReader, journal: _RecoveryReader, run_id: str) -> None:
    """Raise ``ResumeRefused`` unless the run is safe to hand to DBOS."""
    run = await store.get_run(run_id)
    if run is None:
        raise ResumeRefused(f"cannot resume {run_id}: not an agentman run")
    if run.status in RUN_TERMINAL:
        raise ResumeRefused(f"cannot resume {run_id}: run is already terminal ({run.status})")
    workspace = Path(run.workspace)
    if not workspace.is_dir():
        raise ResumeRefused(f"cannot resume {run_id}: workspace does not exist: {run.workspace}")
    inspection = await journal.recovery(run_id=run_id)
    for effect in inspection.unresolved:
        if not _reconciled(effect, workspace):
            raise ResumeRefused(
                f"cannot resume {run_id}: {effect.tool_name} {effect.tool_call_id} is unknown_after_crash"
            )


def _reconciled(effect: ToolEffectRecord, workspace: Path) -> bool:
    if not effect.effect_summary:
        return False
    try:
        summary: Any = json.loads(effect.effect_summary)
    except json.JSONDecodeError:
        return False
    if not isinstance(summary, dict):
        return False
    path = summary.get("path")
    pre_image = summary.get("pre_image")
    post_image = summary.get("post_image")
    if not all(isinstance(value, str) for value in (path, pre_image)):
        return False
    target = Path(path).resolve()
    try:
        target.relative_to(workspace.resolve())
    except ValueError:
        return False
    current = _image(target)
    if not isinstance(post_image, str):
        return current == pre_image
    return current in {pre_image, post_image}


def _image(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return ABSENT_FILE
    except OSError:
        return UNKNOWN_IMAGE


__all__ = ["ResumeRefused", "authorize_resume"]
