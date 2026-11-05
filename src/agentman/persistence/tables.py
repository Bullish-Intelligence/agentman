"""SQLAlchemy metadata for the two schemas agentman owns.

One Postgres database holds three ownership domains: DBOS's private tables,
``step_persistence.*`` (the harness journal), and ``agentman.*`` (the
product-facing index). DBOS owns the third schema and agentman never declares
it. Alembic owns the two declared here; the store never runs ``CREATE TABLE``.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Identity,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

AGENTMAN_SCHEMA = "agentman"
STEP_PERSISTENCE_SCHEMA = "step_persistence"

metadata = MetaData()

thread = Table(
    "thread",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("workspace", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_thread_workspace_updated", "workspace", "updated_at"),
    schema=AGENTMAN_SCHEMA,
)

run = Table(
    "run",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("thread_id", String(64), nullable=False),
    Column("workspace", Text, nullable=False),
    Column("agent", Text, nullable=False),
    Column("model", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("prompt", Text, nullable=False),
    Column("output_json", JSONB, nullable=True),
    Column("output_type", Text, nullable=True),
    Column("messages_json", JSONB, nullable=True),
    Column("usage", JSONB, nullable=True),
    Column("run_metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_run_thread_created", "thread_id", "created_at"),
    schema=AGENTMAN_SCHEMA,
)

runs = Table(
    "runs",
    metadata,
    Column("run_id", Text, primary_key=True),
    Column("conversation_id", Text, nullable=True),
    Column("parent_run_id", Text, nullable=True),
    Column("agent_name", Text, nullable=True),
    Column("metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("registration_id", Text, nullable=True),
    Index("ix_step_runs_conversation", "conversation_id"),
    schema=STEP_PERSISTENCE_SCHEMA,
)

events = Table(
    "events",
    metadata,
    Column("seq", BigInteger, Identity(always=True), primary_key=True),
    Column("run_id", Text, nullable=False),
    Column("kind", Text, nullable=False),
    Column("step_index", Integer, nullable=False),
    Column("timestamp", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("conversation_id", Text, nullable=True),
    Column("parent_run_id", Text, nullable=True),
    Column("agent_name", Text, nullable=True),
    Column("tool_call_id", Text, nullable=True),
    Column("tool_name", Text, nullable=True),
    Column("error", Text, nullable=True),
    Column("metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("idempotency_key", Text, nullable=True),
    Index(
        "uq_step_events_idempotency",
        "run_id",
        "idempotency_key",
        unique=True,
        postgresql_where=text("idempotency_key IS NOT NULL"),
    ),
    Index("ix_step_events_run_seq", "run_id", "seq"),
    schema=STEP_PERSISTENCE_SCHEMA,
)

messages = Table(
    "messages",
    metadata,
    Column("run_id", Text, primary_key=True),
    Column("content_hash", Text, primary_key=True),
    Column("message", JSONB, nullable=False),
    schema=STEP_PERSISTENCE_SCHEMA,
)

snapshots = Table(
    "snapshots",
    metadata,
    Column("seq", BigInteger, Identity(always=True), primary_key=True),
    Column("run_id", Text, nullable=False),
    Column("step_index", Integer, nullable=False),
    Column("conversation_id", Text, nullable=True),
    Column("parent_run_id", Text, nullable=True),
    Column("agent_name", Text, nullable=True),
    Column("timestamp", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("state", Text, nullable=False, server_default=text("'complete'")),
    Column("idempotency_key", Text, nullable=True),
    Column("message_hashes", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
    Index("ix_step_snapshots_run_seq", "run_id", "seq"),
    Index(
        "uq_step_snapshots_idempotency",
        "run_id",
        "idempotency_key",
        unique=True,
        postgresql_where=text("idempotency_key IS NOT NULL"),
    ),
    schema=STEP_PERSISTENCE_SCHEMA,
)

tool_effects = Table(
    "tool_effects",
    metadata,
    Column("run_id", Text, primary_key=True),
    Column("tool_call_id", Text, primary_key=True),
    Column("tool_name", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("ended_at", DateTime(timezone=True), nullable=True),
    Column("idempotency_key", Text, nullable=True),
    Column("effect_summary", Text, nullable=True),
    schema=STEP_PERSISTENCE_SCHEMA,
)

snapshot_idempotency_keys = Table(
    "snapshot_idempotency_keys",
    metadata,
    Column("run_id", Text, primary_key=True),
    Column("idempotency_key", Text, primary_key=True),
    schema=STEP_PERSISTENCE_SCHEMA,
)


__all__ = [
    "AGENTMAN_SCHEMA",
    "STEP_PERSISTENCE_SCHEMA",
    "events",
    "messages",
    "metadata",
    "run",
    "runs",
    "snapshot_idempotency_keys",
    "snapshots",
    "thread",
    "tool_effects",
]
