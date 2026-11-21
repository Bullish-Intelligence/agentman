"""Phase A: workspace and directory resolution."""

from __future__ import annotations

import subprocess
from pathlib import Path

from agentman.discovery import global_directory, resolve_directories, resolve_workspace


def test_directory_without_git_resolves_to_itself(tmp_path: Path) -> None:
    resolved = resolve_workspace(tmp_path)
    assert resolved == tmp_path.resolve()


def test_symlinked_path_resolves_to_its_real_path(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    assert resolve_workspace(link) == real.resolve()


def test_repository_root_is_the_workspace(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    assert resolve_workspace(nested) == tmp_path.resolve()


def test_two_worktrees_are_two_workspaces(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    (repo / "file.txt").write_text("one")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=a@b.c", "-c", "user.name=t", "commit", "-m", "init"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    second = tmp_path / "second"
    subprocess.run(
        ["git", "worktree", "add", str(second)],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    first_workspace = resolve_workspace(repo)
    second_workspace = resolve_workspace(second)
    assert first_workspace != second_workspace
    assert first_workspace == repo.resolve()
    assert second_workspace == second.resolve()


def test_directories_report_which_exist(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    (workspace / ".agentman").mkdir()

    directories = resolve_directories(workspace, xdg_config_home=config_home)
    assert directories.workspace == workspace.resolve()
    assert directories.repo == (workspace / ".agentman").resolve()
    assert directories.global_ == (config_home / "agentman").resolve()
    assert directories.precedence() == (directories.repo, directories.global_)


def test_missing_directories_are_none(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    directories = resolve_directories(workspace, xdg_config_home=tmp_path / "config")
    assert directories.repo is None
    assert directories.global_ is None
    assert directories.precedence() == ()


def test_global_directory_follows_xdg_config_home(tmp_path: Path) -> None:
    assert global_directory(tmp_path) == (tmp_path / "agentman").resolve()


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True, capture_output=True)
