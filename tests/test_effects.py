"""The file-effect evidence capability is narrow and durable."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from pydantic_ai_harness.step_persistence import InMemoryStepStore, ToolEffectRecord
from pydantic_ai_harness.step_persistence._context import current_run_id

from agentman.effects import ABSENT_FILE, FileEffectEvidence


def test_file_effect_evidence_is_scoped_to_two_tools(tmp_path: Path) -> None:
    capability = FileEffectEvidence(store=InMemoryStepStore(), workspace=str(tmp_path))

    assert capability._target("write_file", {"path": "new.txt"}) == tmp_path / "new.txt"
    assert capability._target("edit_file", {"path": "new.txt"}) == tmp_path / "new.txt"
    assert capability._target("shell", {"path": "new.txt"}) is None


def test_missing_file_uses_an_explicit_absent_sentinel(tmp_path: Path) -> None:
    capability = FileEffectEvidence(store=InMemoryStepStore(), workspace=str(tmp_path))
    target = capability._target("write_file", {"path": "new.txt"})
    assert target is not None

    from agentman.effects import _image, _parse_summary, _summary

    summary = _parse_summary(_summary(path=target, pre_image=_image(target)))
    assert summary is not None
    assert summary["pre_image"] == ABSENT_FILE
    assert "post_image" not in summary


async def test_completed_write_persists_both_images(tmp_path: Path) -> None:
    store = InMemoryStepStore()
    await store.record_tool_effect(
        ToolEffectRecord(
            run_id="run-1",
            tool_call_id="call-1",
            tool_name="write_file",
            status="started",
        )
    )
    capability = FileEffectEvidence(store=store, workspace=str(tmp_path))
    ctx = cast(Any, SimpleNamespace(run_id="run-1", tool_call_id="call-1", tool_name="write_file"))
    tool_def = cast(Any, SimpleNamespace(name="write_file"))
    call = cast(Any, SimpleNamespace())
    token = current_run_id.set("run-1")
    try:
        args = {"path": "new.txt", "content": "hello"}
        await capability.before_tool_execute(ctx, call=call, tool_def=tool_def, args=args)
        (tmp_path / "new.txt").write_text("hello")
        await capability.after_tool_execute(ctx, call=call, tool_def=tool_def, args=args, result="ok")
    finally:
        current_run_id.reset(token)

    effect = await store.get_tool_effect(run_id="run-1", tool_call_id="call-1")
    assert effect is not None
    assert effect.effect_summary is not None
    assert effect.status == "started"
    await store.record_tool_effect(
        ToolEffectRecord(
            run_id=effect.run_id,
            tool_call_id=effect.tool_call_id,
            tool_name=effect.tool_name,
            status="completed",
            started_at=effect.started_at,
            effect_summary=effect.effect_summary,
        )
    )
    effect = await store.get_tool_effect(run_id="run-1", tool_call_id="call-1")
    assert effect is not None and effect.status == "completed"
    assert effect.effect_summary is not None
    summary = json.loads(effect.effect_summary)
    assert summary["pre_image"] == ABSENT_FILE
    assert summary["post_image"] == hashlib.sha256(b"hello").hexdigest()
    assert (tmp_path / "new.txt").read_text() == "hello"
