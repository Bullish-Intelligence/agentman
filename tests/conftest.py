"""Shared fixtures.

The default test run is hermetic. Postgres- and rig-dependent tests carry the
``integration`` marker and are skipped unless ``AGENTMAN_RUN_INTEGRATION=1``,
following lodestar's ``LODESTAR_RUN_INTEGRATION`` pattern.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

INTEGRATION_ENV = "AGENTMAN_RUN_INTEGRATION"
DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://agentman@127.0.0.1:5433/agentman"
REPO_ROOT = Path(__file__).resolve().parent.parent


def integration_enabled() -> bool:
    return os.environ.get(INTEGRATION_ENV) == "1"


def test_database_url() -> str:
    configured = os.environ.get("AGENTMAN_TEST_DATABASE_URL")
    if integration_enabled() and configured is None:
        raise RuntimeError(
            "AGENTMAN_TEST_DATABASE_URL is required for integration tests; point it at a fresh or known test database"
        )
    return configured or DEFAULT_TEST_DATABASE_URL


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if integration_enabled():
        return
    skip = pytest.mark.skip(reason=f"set {INTEGRATION_ENV}=1 to run integration tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def database_url() -> str:
    return test_database_url()


@pytest.fixture(scope="session", autouse=True)
def _migrated(database_url: str) -> None:
    """Bring the test database to head once per session."""
    if not integration_enabled():
        return
    sync_url = database_url.replace("+asyncpg", "")
    result = subprocess.run(
        ["alembic", "upgrade", "head"],
        cwd=REPO_ROOT,
        env={**os.environ, "AGENTMAN_DATABASE_URL": sync_url},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        combined = f"{result.stdout}\n{result.stderr}"
        # This fixture is autouse, so a database that is down fails every test in
        # the session with the same traceback. Say which it is. Finding G20.
        if "could not connect" in combined or "Connection refused" in combined:
            raise RuntimeError(
                f"the test database at {sync_url} is not reachable.\n"
                "Start the project cluster with `devenv up -d`, or unset "
                f"{INTEGRATION_ENV} to run the hermetic suite only."
            )
        raise RuntimeError(f"alembic upgrade head failed:\n{combined}")
