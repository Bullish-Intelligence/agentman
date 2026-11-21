"""Upgrade a database from revision 0003 to 0004 and keep its runs.

The test makes its own database, so it never touches the session database
that the ``_migrated`` fixture brings to head. The database role needs the
``CREATEDB`` privilege.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from uuid import uuid4

import pytest
from conftest import REPO_ROOT
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, make_url

pytestmark = pytest.mark.integration


def _sync_url(database_url: str) -> URL:
    return make_url(database_url.replace("+asyncpg", "+psycopg"))


def _alembic(url: URL, *args: str) -> None:
    result = subprocess.run(
        ["alembic", *args],
        cwd=REPO_ROOT,
        env={**os.environ, "AGENTMAN_DATABASE_URL": url.render_as_string(hide_password=False)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"alembic {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"


@pytest.fixture()
def fresh_database(database_url: str) -> Iterator[URL]:
    admin_url = _sync_url(database_url)
    name = f"agentman_upgrade_{uuid4().hex[:12]}"
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield admin_url.set(database=name)
    finally:
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def test_upgrade_to_0004_keeps_existing_runs(fresh_database: URL) -> None:
    _alembic(fresh_database, "upgrade", "0003")
    engine = create_engine(fresh_database)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO agentman.run (id, thread_id, workspace, agent, model, status, prompt) "
                    "VALUES ('run-old', 'thread-old', '/repo', 'reviewer', 'mock:model', 'completed', 'hello')"
                )
            )
            columns = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'agentman' AND table_name = 'run'"
                )
            ).scalars()
            assert "caller_metadata" not in set(columns)

        _alembic(fresh_database, "upgrade", "0004")

        with engine.connect() as connection:
            row = connection.execute(
                text("SELECT prompt, run_metadata, caller_metadata FROM agentman.run WHERE id = 'run-old'")
            ).one()
        assert row.prompt == "hello"
        assert row.run_metadata == {}
        assert row.caller_metadata == {}

        _alembic(fresh_database, "downgrade", "0003")

        with engine.connect() as connection:
            survivors = connection.execute(text("SELECT id FROM agentman.run")).scalars().all()
            columns = connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'agentman' AND table_name = 'run'"
                )
            ).scalars()
            assert "caller_metadata" not in set(columns)
        assert survivors == ["run-old"]
    finally:
        engine.dispose()
