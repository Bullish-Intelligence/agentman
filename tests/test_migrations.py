"""The frozen migration must keep matching the live metadata.

Migration ``0001`` is frozen: it uses explicit ``op.create_table`` calls and
imports no model metadata. That freeze is what makes a recorded run
reproducible, and it is also what lets the two drift apart in silence. This
check closes that gap.

The comparison runs offline. ``alembic upgrade head --sql`` renders the
migrations without a connection, and the metadata renders through the same
SQLAlchemy compiler, so the two outputs are comparable text. No database is
needed, so this check is not gated behind ``AGENTMAN_RUN_INTEGRATION``.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from agentman.persistence.tables import metadata

REPO_ROOT = Path(__file__).resolve().parent.parent
OFFLINE_URL = "postgresql://offline@localhost/offline"


def _normalise(statement: str) -> str:
    """Collapse whitespace so formatting differences do not register as drift."""
    return re.sub(r"\s+", " ", statement).strip().rstrip(";")


def _migration_statements() -> tuple[set[str], set[str]]:
    result = subprocess.run(
        ["alembic", "upgrade", "head", "--sql"],
        cwd=REPO_ROOT,
        env={**os.environ, "AGENTMAN_DATABASE_URL": OFFLINE_URL},
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"alembic upgrade head --sql failed:\n{result.stdout}\n{result.stderr}")
    statements = set()
    alterations = set()
    for raw in result.stdout.split(";"):
        without_comments = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--"))
        text = _normalise(without_comments)
        if not text.upper().startswith(("CREATE TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX")):
            if text.upper().startswith("ALTER TABLE"):
                alterations.add(text)
            continue
        # Alembic owns `alembic_version`, so agentman's metadata never declares it.
        if "alembic_version" in text:
            continue
        statements.add(text)
    return statements, alterations


def _metadata_statements() -> set[str]:
    dialect = postgresql.dialect()
    statements = set()
    for table in metadata.sorted_tables:
        statements.add(_normalise(str(CreateTable(table).compile(dialect=dialect))))
        for index in table.indexes:
            statements.add(_normalise(str(CreateIndex(index).compile(dialect=dialect))))
    return statements


def test_the_migrations_still_match_the_live_metadata() -> None:
    migration, alterations = _migration_statements()
    live = _metadata_statements()
    assert migration, "the offline render produced no DDL; the check would pass vacuously"

    migration_run = next(statement for statement in migration if statement.startswith("CREATE TABLE agentman.run ("))
    live_run = next(statement for statement in live if statement.startswith("CREATE TABLE agentman.run ("))
    assert "run_metadata JSONB" not in migration_run
    assert "run_metadata JSONB" in live_run
    assert "ALTER TABLE agentman.run ADD COLUMN run_metadata JSONB DEFAULT '{}'::jsonb NOT NULL" in alterations
    assert "error JSONB" not in migration_run
    assert "error JSONB" in live_run
    assert "ALTER TABLE agentman.run ADD COLUMN error JSONB" in alterations
    migration.remove(migration_run)
    live.remove(live_run)

    missing = live - migration
    extra = migration - live
    assert not missing, "the metadata declares DDL no migration creates; add a revision:\n" + "\n".join(sorted(missing))
    assert not extra, "the migrations create DDL the metadata does not declare:\n" + "\n".join(sorted(extra))


def test_the_alembic_version_table_is_not_in_the_metadata() -> None:
    """Alembic owns its own bookkeeping table; agentman never declares it."""
    assert "alembic_version" not in {table.name for table in metadata.sorted_tables}


def test_migrations_accept_the_daemon_async_url() -> None:
    result = subprocess.run(
        ["alembic", "upgrade", "head", "--sql"],
        cwd=REPO_ROOT,
        env={**os.environ, "AGENTMAN_DATABASE_URL": "postgresql+asyncpg://agentman@/agentman?host=/run/postgresql"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
