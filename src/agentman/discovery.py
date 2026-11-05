"""Resolve the workspace and locate the agentman directories.

A workspace is the resolved repository root. Two git worktrees of one
repository are two workspaces, because they are two working directories with
different content. The current-thread pointer and every stored thread are
workspace-scoped, so this function is the root of all identity.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

GLOBAL_DIRECTORY_NAME = "agentman"
"""The global directory's name under ``$XDG_CONFIG_HOME``."""

REPO_DIRECTORY_NAME = ".agentman"
"""The repository directory's name under the workspace root."""


class DiscoveryError(Exception):
    """The workspace or the interpreter cannot be resolved."""


@dataclass(frozen=True)
class Directories:
    """The workspace and the agentman directories that exist under it."""

    workspace: Path
    repo: Path | None
    global_: Path | None

    def precedence(self) -> tuple[Path, ...]:
        """Return existing directories in shadowing order, repository first."""
        return tuple(directory for directory in (self.repo, self.global_) if directory is not None)


def resolve_workspace(cwd: Path | None = None) -> Path:
    """Return the resolved repository root, or the resolved cwd outside a repo.

    ``git rev-parse --show-toplevel`` gives the root of the working tree, so two
    worktrees of one repository resolve to two different workspaces. When the
    directory is not in a repository, the resolved cwd is the workspace.
    """
    start = Path(cwd) if cwd is not None else Path.cwd()
    start = start.expanduser().resolve()
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=start,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return start
    if result.returncode == 0:
        top = result.stdout.strip()
        if top:
            return Path(top).resolve()
    return start


def global_directory(xdg_config_home: Path | str | None = None) -> Path:
    """Return ``$XDG_CONFIG_HOME/agentman`` or ``~/.config/agentman``."""
    if xdg_config_home is not None:
        base = Path(xdg_config_home).expanduser()
    elif os.environ.get("XDG_CONFIG_HOME"):
        base = Path(os.environ["XDG_CONFIG_HOME"]).expanduser()
    else:
        base = Path.home() / ".config"
    return (base / GLOBAL_DIRECTORY_NAME).resolve()


def resolve_directories(
    workspace: Path | None = None,
    *,
    xdg_config_home: Path | str | None = None,
) -> Directories:
    """Return the workspace and the existing agentman directories.

    ``repo`` and ``global_`` are ``None`` when that directory does not exist.
    """
    resolved = resolve_workspace(workspace) if workspace is not None else resolve_workspace()
    repo = resolved / REPO_DIRECTORY_NAME
    global_ = global_directory(xdg_config_home)
    return Directories(
        workspace=resolved,
        repo=repo if repo.is_dir() else None,
        global_=global_ if global_.is_dir() else None,
    )


__all__ = [
    "GLOBAL_DIRECTORY_NAME",
    "REPO_DIRECTORY_NAME",
    "Directories",
    "DiscoveryError",
    "global_directory",
    "resolve_directories",
    "resolve_workspace",
]
