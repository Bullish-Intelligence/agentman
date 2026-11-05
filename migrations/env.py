"""Alembic environment. Sync, against the async application's metadata.

The URL comes from ``AGENTMAN_DATABASE_URL`` (or ``sqlalchemy.url``). Alembic
uses the synchronous ``psycopg`` driver, so async or plain PostgreSQL URLs are
rewritten to ``postgresql+psycopg://``.
"""

from __future__ import annotations

import os
from typing import cast

from alembic import context
from sqlalchemy import engine_from_config, pool

from agentman.persistence.tables import metadata

config = context.config

database_url = os.environ.get("AGENTMAN_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
if not database_url:
    raise RuntimeError("Alembic requires AGENTMAN_DATABASE_URL or sqlalchemy.url")
if database_url.startswith("postgresql+asyncpg://"):
    database_url = database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
elif database_url.startswith("postgresql://"):
    database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)
config.set_main_option("sqlalchemy.url", database_url)

if context.is_offline_mode():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    section = cast(dict[str, object], config.get_section(config.config_ini_section) or {})
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata)
        with context.begin_transaction():
            context.run_migrations()
