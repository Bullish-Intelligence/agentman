"""The provider-neutral agent contract and spec loading.

This package is an internal boundary, not a user-facing noun. It loads an
agentman ``AgentSpec`` and names the pydantic output type a spec points at. It
never imports ``agentman``, ``dbos``, ``sqlalchemy`` or ``httpx``: Import Linter
enforces that direction from the first commit.
"""

from structured_agents.spec import (
    CONFIG_FILENAME,
    LoadedSpec,
    OutputRef,
    SpecError,
    ToolSpec,
    as_mapping,
    discover_specs,
    load_spec,
)

__all__ = [
    "CONFIG_FILENAME",
    "LoadedSpec",
    "OutputRef",
    "SpecError",
    "ToolSpec",
    "as_mapping",
    "discover_specs",
    "load_spec",
]
