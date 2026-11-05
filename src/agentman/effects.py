"""Durable evidence for the file operations agentman understands."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic_ai.capabilities import AbstractCapability, CapabilityOrdering
from pydantic_ai.messages import ToolCallPart
from pydantic_ai.tools import RunContext, ToolDefinition
from pydantic_ai_harness.step_persistence import StepPersistence, StepStore, annotate_tool_effect

ABSENT_FILE = "absent"
UNKNOWN_IMAGE = "unknown"
FILE_TOOLS = frozenset({"write_file", "edit_file"})


@dataclass
class FileEffectEvidence(AbstractCapability[Any]):
    """Attach pre- and post-image evidence to known filesystem tool effects."""

    store: StepStore
    workspace: str
    root: Path = field(init=False)

    def __post_init__(self) -> None:
        self.id = "agentman_file_effects"
        self.root = Path(self.workspace).resolve()

    def get_ordering(self) -> CapabilityOrdering:
        """Run after StepPersistence so its started row already exists."""
        return CapabilityOrdering(wrapped_by=(StepPersistence,))

    async def before_tool_execute(
        self,
        ctx: RunContext[Any],
        *,
        call: ToolCallPart,
        tool_def: ToolDefinition,
        args: Any,
    ) -> Any:
        target = self._target(tool_def.name, args)
        if target is not None:
            await annotate_tool_effect(
                self.store,
                ctx,
                effect_summary=_summary(path=target, pre_image=_image(target)),
            )
        return args

    async def after_tool_execute(
        self,
        ctx: RunContext[Any],
        *,
        call: ToolCallPart,
        tool_def: ToolDefinition,
        args: Any,
        result: Any,
    ) -> Any:
        target = self._target(tool_def.name, args)
        if target is None:
            return result
        pre_image = UNKNOWN_IMAGE
        if ctx.run_id is not None and ctx.tool_call_id is not None:
            record = await self.store.get_tool_effect(run_id=ctx.run_id, tool_call_id=ctx.tool_call_id)
            if record is not None:
                prior = _parse_summary(record.effect_summary)
                if prior is not None:
                    pre_image = prior.get("pre_image", UNKNOWN_IMAGE)
        await annotate_tool_effect(
            self.store,
            ctx,
            effect_summary=_summary(path=target, pre_image=pre_image, post_image=_image(target)),
        )
        return result

    def _target(self, tool_name: str, args: Any) -> Path | None:
        if tool_name not in FILE_TOOLS or not isinstance(args, Mapping):
            return None
        raw_path = args.get("path")
        if not isinstance(raw_path, str):
            return None
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = self.root / candidate
        target = candidate.resolve()
        try:
            target.relative_to(self.root)
        except ValueError:
            return None
        return target


def _image(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return ABSENT_FILE
    except OSError:
        return UNKNOWN_IMAGE


def _summary(*, path: Path, pre_image: str, post_image: str | None = None) -> str:
    value: dict[str, str] = {"path": str(path), "pre_image": pre_image}
    if post_image is not None:
        value["post_image"] = post_image
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _parse_summary(value: str | None) -> dict[str, str] | None:
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in parsed.items()
    ):
        return None
    return parsed


__all__ = ["ABSENT_FILE", "FILE_TOOLS", "FileEffectEvidence", "UNKNOWN_IMAGE"]
