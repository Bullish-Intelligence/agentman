"""The import-linter contracts hold, and the client contract catches a bad import."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
RUNNER = ROOT / "tests" / "support" / "lint_imports.py"
SOURCE_TREES = (ROOT / "src", ROOT / "packages" / "agentman-client" / "src")
CLIENT_CONTRACT = "the client distribution has no server imports"


def _lint(config: Path, source: Path | None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    command = [sys.executable, str(RUNNER), "--config", str(config)]
    if source is not None:
        env["PYTHONPATH"] = str(source)
        command.append("--merged")
    return subprocess.run(
        command,
        cwd=config.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_all_contracts_hold() -> None:
    result = _lint(ROOT / "pyproject.toml", None)

    assert result.returncode == 0, result.stdout + result.stderr
    assert CLIENT_CONTRACT in result.stdout


@pytest.mark.parametrize(
    "bad_import",
    [
        "import agentman.daemon",
        "import agentman.runtime",
        "import agentman.persistence",
        "import sqlalchemy",
        "import dbos",
    ],
)
def test_client_contract_fails_when_a_client_module_imports_a_server_module(tmp_path: Path, bad_import: str) -> None:
    # Join both source trees into one copy, as a wheel install does. Then add
    # the bad import to the client module in the copy only.
    merged = tmp_path / "src"
    for tree in SOURCE_TREES:
        shutil.copytree(tree, merged, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy(ROOT / "pyproject.toml", tmp_path / "pyproject.toml")
    client = merged / "agentman" / "client.py"
    client.write_text(client.read_text() + f"\n{bad_import}  # noqa: E402,F401\n")

    result = _lint(tmp_path / "pyproject.toml", merged)

    assert result.returncode == 1, result.stdout + result.stderr
    assert f"{CLIENT_CONTRACT} BROKEN" in result.stdout
