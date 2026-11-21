"""Both distributions carry one version."""

from __future__ import annotations

import tomllib
from pathlib import Path

import agentman

ROOT = Path(__file__).resolve().parent.parent


def _pyproject(path: Path) -> dict:
    return tomllib.loads((path / "pyproject.toml").read_text())


def test_both_distributions_and_the_module_state_one_version() -> None:
    root = _pyproject(ROOT)
    client = _pyproject(ROOT / "packages" / "agentman-client")

    assert root["project"]["name"] == "agentman"
    assert client["project"]["name"] == "agentman-client"
    assert client["project"]["version"] == root["project"]["version"]
    assert agentman.__version__ == root["project"]["version"]


def test_root_distribution_pins_the_client_to_the_same_version() -> None:
    root = _pyproject(ROOT)
    version = root["project"]["version"]

    assert f"agentman-client=={version}" in root["project"]["dependencies"]
    assert root["tool"]["uv"]["sources"]["agentman-client"] == {"workspace": True}
