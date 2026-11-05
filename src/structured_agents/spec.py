"""Spec discovery and loading for agentman agents.

An agent is a YAML file in an agentman directory. The file's stem is the agent
name. The repository directory shadows the global directory by filename, so a
repo agent and a global agent can share a name and the repo one wins.

A spec is a pydantic-ai ``AgentSpec`` plus one agentman extension: the
``output:`` key. The loader pops that key before it hands the rest of the
mapping to pydantic-ai. ``output: models:Summary`` names an attribute in a
module beside the spec. The reference is an *instruction*: this module records
the directory and the ``module:Attribute`` target, and the caller resolves it in
the repository's own interpreter. Resolution never goes through ``sys.path``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic_ai.agent.spec import AgentSpec

CONFIG_FILENAME = "agentman.yaml"
"""The config file. It is never an agent, even if its stem names one."""

SPEC_SUFFIXES = (".yaml", ".yml")
"""File suffixes a spec may carry."""

DEFAULT_OUTPUT_TARGET = "str"
"""The output type of a spec that names none. Every agent is typed."""


class ToolSpec(StrEnum):
    """The closed set of toolsets an agent spec may request."""

    CODER = "coder"


class SpecError(Exception):
    """A spec file is missing, unreadable, or not a valid agent spec."""


@dataclass(frozen=True)
class OutputRef:
    """An instruction to resolve a pydantic output type in its own directory.

    ``directory`` is the directory that provided the spec. ``target`` is
    ``module:Attribute`` and resolves against that directory alone.
    """

    directory: Path
    target: str


@dataclass(frozen=True)
class LoadedSpec:
    """A validated agent spec and where it came from."""

    name: str
    path: Path
    directory: Path
    agent: AgentSpec
    output: OutputRef | None
    """``None`` means the built-in ``str`` output. There is no untyped branch."""
    tools: ToolSpec | None
    """The optional agentman toolset extension."""


def discover_specs(directories: Sequence[Path]) -> dict[str, Path]:
    """Return ``name -> path`` for every agent in ``directories``.

    ``directories`` is in precedence order. The first directory that provides a
    name wins, so the repository directory shadows the global one by filename.
    """
    found: dict[str, Path] = {}
    for directory in directories:
        if not directory.is_dir():
            continue
        for path in sorted(directory.iterdir()):
            if path.suffix.lower() not in SPEC_SUFFIXES:
                continue
            if path.name == CONFIG_FILENAME:
                continue
            found.setdefault(path.stem, path)
    return found


def load_spec(path: Path) -> LoadedSpec:
    """Load and validate one spec file.

    The agentman ``output:`` and ``tools:`` keys are popped before pydantic-ai
    sees the mapping.
    """
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise SpecError(f"cannot read agent spec {path}: {error}") from error
    except yaml.YAMLError as error:
        raise SpecError(f"cannot parse agent spec {path}: {error}") from error

    if not isinstance(raw, dict):
        raise SpecError(f"agent spec {path} must be a mapping, got {type(raw).__name__}")

    mapping = dict(raw)
    output_value = mapping.pop("output", None)
    output = _parse_output(output_value, directory=path.parent, path=path)
    tools = _parse_tools(mapping.pop("tools", None), path=path)

    try:
        agent = AgentSpec.from_dict(mapping)
    except Exception as error:  # pydantic raises several unrelated types
        raise SpecError(f"invalid agent spec {path}: {error}") from error

    return LoadedSpec(
        name=agent.name or path.stem,
        path=path,
        directory=path.parent,
        agent=agent,
        output=output,
        tools=tools,
    )


def _parse_output(value: Any, *, directory: Path, path: Path) -> OutputRef | None:
    if value is None or value == DEFAULT_OUTPUT_TARGET:
        return None
    if not isinstance(value, str):
        raise SpecError(f"agent spec {path}: `output:` must be a string, got {type(value).__name__}")
    module, separator, attribute = value.partition(":")
    if not separator or not module or not attribute:
        raise SpecError(
            f"agent spec {path}: `output: {value}` is not `module:Attribute` (or the literal `{DEFAULT_OUTPUT_TARGET}`)"
        )
    if "." in attribute or not attribute.isidentifier():
        raise SpecError(f"agent spec {path}: `output: {value}` names an invalid attribute")
    return OutputRef(directory=directory, target=value)


def _parse_tools(value: Any, *, path: Path) -> ToolSpec | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise SpecError(f"agent spec {path}: `tools:` must be a string, got {type(value).__name__}")
    try:
        return ToolSpec(value)
    except ValueError as error:
        raise SpecError(f"agent spec {path}: unsupported `tools: {value}`; expected `coder`") from error


def as_mapping(spec: LoadedSpec) -> dict[str, Any]:
    """Return the resolved spec as a plain mapping, for the socket.

    The daemon never imports repository code, so the request carries the spec
    as data. ``AgentSpec`` serialises with its aliases.
    """
    return spec.agent.model_dump(mode="json", by_alias=True, exclude_defaults=False)


__all__ = [
    "CONFIG_FILENAME",
    "DEFAULT_OUTPUT_TARGET",
    "SPEC_SUFFIXES",
    "ToolSpec",
    "LoadedSpec",
    "OutputRef",
    "SpecError",
    "as_mapping",
    "discover_specs",
    "load_spec",
]
