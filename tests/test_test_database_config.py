"""Integration tests require an explicit database target."""

from __future__ import annotations

import pytest
from conftest import test_database_url as configured_test_database_url


def test_integration_database_url_requires_an_explicit_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMAN_RUN_INTEGRATION", "1")
    monkeypatch.delenv("AGENTMAN_TEST_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="fresh or known test database"):
        configured_test_database_url()


def test_integration_database_url_uses_the_explicit_target(monkeypatch: pytest.MonkeyPatch) -> None:
    target = "postgresql+asyncpg://agentman@127.0.0.1:55432/known-test"
    monkeypatch.setenv("AGENTMAN_RUN_INTEGRATION", "1")
    monkeypatch.setenv("AGENTMAN_TEST_DATABASE_URL", target)

    assert configured_test_database_url() == target
