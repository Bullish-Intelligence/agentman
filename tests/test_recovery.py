"""Resume policy tests: facts authorize, but never perform, recovery."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic_ai_harness.step_persistence import ToolEffectRecord
from pydantic_ai_harness.step_persistence.recovery import RecoveryInspection

from agentman.recovery import ResumeRefused, authorize_resume


class _Journal:
    def __init__(self, effects: tuple[ToolEffectRecord, ...] = ()) -> None:
        self.effects = effects

    async def recovery(self, *, run_id: str) -> RecoveryInspection:
        return RecoveryInspection(
            latest=None,
            settled=None,
            unresolved=self.effects,
            completed_tools=(),
            failed_tools=(),
        )


class _Store:
    def __init__(self, run: Any | None) -> None:
        self.run = run

    async def get_run(self, run_id: str) -> Any | None:
        return self.run


def _run(workspace: Path, status: str = "running") -> Any:
    return SimpleNamespace(workspace=str(workspace), status=status)


def _effect(summary: str | None) -> ToolEffectRecord:
    return ToolEffectRecord(
        run_id="run-1",
        tool_call_id="call-1",
        tool_name="write_file",
        status="started",
        effect_summary=summary,
    )


async def test_resume_allows_file_that_still_matches_pre_image(tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("before")
    summary = json.dumps(
        {
            "path": str(target),
            "pre_image": hashlib.sha256(b"before").hexdigest(),
            "post_image": hashlib.sha256(b"after").hexdigest(),
        }
    )
    await authorize_resume(store=_Store(_run(tmp_path)), journal=_Journal((_effect(summary),)), run_id="run-1")


async def test_resume_allows_pre_image_without_post_image(tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("before")
    summary = json.dumps({"path": str(target), "pre_image": hashlib.sha256(b"before").hexdigest()})
    await authorize_resume(store=_Store(_run(tmp_path)), journal=_Journal((_effect(summary),)), run_id="run-1")


async def test_resume_allows_file_that_matches_post_image(tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("after")
    summary = json.dumps(
        {
            "path": str(target),
            "pre_image": hashlib.sha256(b"before").hexdigest(),
            "post_image": hashlib.sha256(b"after").hexdigest(),
        }
    )
    await authorize_resume(store=_Store(_run(tmp_path)), journal=_Journal((_effect(summary),)), run_id="run-1")


@pytest.mark.parametrize(
    ("run", "effects", "expected"),
    [
        (None, (), "not an agentman run"),
        (SimpleNamespace(workspace="/unused", status="completed"), (), "already terminal"),
        (SimpleNamespace(workspace="/unused", status="failed"), (), "already terminal"),
        (SimpleNamespace(workspace="/unused", status="cancelled"), (), r"already terminal \(cancelled\)"),
    ],
)
async def test_resume_refuses_invalid_run_facts(
    tmp_path: Path,
    run: Any | None,
    effects: tuple[ToolEffectRecord, ...],
    expected: str,
) -> None:
    if run is not None:
        run.workspace = str(tmp_path)
    with pytest.raises(ResumeRefused, match=expected):
        await authorize_resume(store=_Store(run), journal=_Journal(effects), run_id="run-1")


async def test_resume_refuses_missing_workspace(tmp_path: Path) -> None:
    with pytest.raises(ResumeRefused, match="workspace does not exist"):
        await authorize_resume(store=_Store(_run(tmp_path / "gone")), journal=_Journal(), run_id="run-1")


@pytest.mark.parametrize("summary", [None, "not-json", json.dumps({"path": "x", "pre_image": "a"})])
async def test_resume_refuses_absent_or_inconclusive_evidence(tmp_path: Path, summary: str | None) -> None:
    with pytest.raises(ResumeRefused, match="unknown_after_crash"):
        await authorize_resume(store=_Store(_run(tmp_path)), journal=_Journal((_effect(summary),)), run_id="run-1")


async def test_resume_refuses_when_file_matches_neither_image(tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("other")
    summary = json.dumps({"path": str(target), "pre_image": "before", "post_image": "after"})
    with pytest.raises(ResumeRefused, match="unknown_after_crash"):
        await authorize_resume(store=_Store(_run(tmp_path)), journal=_Journal((_effect(summary),)), run_id="run-1")
