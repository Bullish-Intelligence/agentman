"""Build the wheels and install them into clean virtual environments.

This test builds and installs packages, so it is slow and may need the network.
Run it with ``AGENTMAN_RUN_PACKAGING=1``. Set ``AGENTMAN_PACKAGING_OFFLINE=1``
to install from the uv cache only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CLIENT_TESTS = ROOT / "packages" / "agentman-client" / "tests"
SERVER_ONLY = {"dbos", "sqlalchemy", "asyncpg", "psycopg", "alembic", "uvicorn", "inferference", "pydantic-ai-harness"}

pytestmark = pytest.mark.skipif(
    os.environ.get("AGENTMAN_RUN_PACKAGING") != "1",
    reason="set AGENTMAN_RUN_PACKAGING=1 to build and install the wheels",
)


def _offline() -> list[str]:
    return ["--offline"] if os.environ.get("AGENTMAN_PACKAGING_OFFLINE") == "1" else []


def _run(command: list[str | Path], **kwargs: object) -> subprocess.CompletedProcess[str]:
    result = subprocess.run([str(part) for part in command], capture_output=True, text=True, check=False, **kwargs)  # type: ignore[call-overload]
    assert result.returncode == 0, f"{command}\n{result.stdout}\n{result.stderr}"
    return result


def _venv(path: Path) -> Path:
    _run(["uv", "venv", "--python", sys.executable, *_offline(), path])
    return path / "bin" / "python"


def _installed(python: Path) -> set[str]:
    listing = _run(["uv", "pip", "list", "--format", "json", "--python", python])
    return {entry["name"].lower().replace("_", "-") for entry in json.loads(listing.stdout)}


@pytest.fixture(scope="module")
def wheels(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    out = tmp_path_factory.mktemp("wheels")
    # Build each distribution on its own.
    for package in ("agentman-client", "agentman"):
        _run(["uv", "build", "--package", package, "--wheel", "--out-dir", out, *_offline()], cwd=ROOT)
    return {
        "client": next(out.glob("agentman_client-*.whl")),
        "server": next(out.glob("agentman-*.whl")),
    }


def test_client_wheel_installs_no_server_package(wheels: dict[str, Path], tmp_path: Path) -> None:
    python = _venv(tmp_path / "client-venv")
    _run(["uv", "pip", "install", *_offline(), "--python", python, wheels["client"], "pytest"])

    installed = _installed(python)
    print("installed in the client venv:", ", ".join(sorted(installed)))  # noqa: T201
    assert "agentman-client" in installed
    assert not installed & SERVER_ONLY
    assert "agentman" not in installed

    check = (
        "from agentman.client import Client\n"
        "from agentman.protocol import TurnRequest\n"
        "from structured_agents import load_spec\n"
        "import agentman, sys\n"
        "print(agentman.__version__, agentman.__file__)\n"
    )
    imported = _run([python, "-c", check], cwd=tmp_path)
    assert "site-packages" in imported.stdout

    # Run the client tests against the installed wheel only.
    _run(
        [python, "-m", "pytest", "-p", "no:cacheprovider", "--rootdir", tmp_path, CLIENT_TESTS],
        cwd=tmp_path,
    )


def test_both_wheels_install_one_agentman_package_with_the_cli(wheels: dict[str, Path], tmp_path: Path) -> None:
    python = _venv(tmp_path / "full-venv")
    # The root wheel depends on the inferference git source. A wheel records no
    # git source, so an index install cannot resolve it. Install the locked
    # third-party packages from `uv.lock`, then the two wheels without
    # dependencies.
    requirements = tmp_path / "requirements.txt"
    exported = _run(
        [
            "uv",
            "export",
            "--frozen",
            "--no-dev",
            "--no-hashes",
            "--no-emit-workspace",
            "--no-emit-project",
            *_offline(),
        ],
        cwd=ROOT,
    )
    requirements.write_text(exported.stdout)
    _run(["uv", "pip", "install", *_offline(), "--python", python, "-r", requirements])
    _run(["uv", "pip", "install", "--no-deps", *_offline(), "--python", python, wheels["client"], wheels["server"]])

    installed = _installed(python)
    assert {"agentman", "agentman-client", "dbos", "sqlalchemy"} <= installed

    from_wheels = _run(
        [python, "-c", "import agentman.daemon, agentman.cli, agentman, sys; print(agentman.__version__)"],
        cwd=tmp_path,
    )
    assert from_wheels.stdout.strip() == _read_version()
    version = _run([tmp_path / "full-venv" / "bin" / "agentman", "version"], cwd=tmp_path)
    assert version.stdout.startswith(f"agentman {_read_version()}")


def _read_version() -> str:
    import tomllib

    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
