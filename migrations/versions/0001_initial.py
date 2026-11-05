"""Create the agentman and step_persistence schemas and every table.

Revision ID: 0001
Revises:
Create Date: 2026-09-19

This revision is frozen. It uses explicit ``op.create_table`` calls for the
schema as it stood when the revision was written. It never imports the live
model metadata, so an empty database and an upgraded database get the same
shape, and the revision graph describes the schema.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

AGENTMAN = "agentman"
STEP_PERSISTENCE = "step_persistence"

_NOW = sa.text("now()")
_EMPTY_JSONB = sa.text("'{}'::jsonb")
_EMPTY_TEXT_ARRAY = sa.text("'{}'::text[]")


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {AGENTMAN}")
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {STEP_PERSISTENCE}")
    _create_agentman_tables()
    _create_step_persistence_tables()


def downgrade() -> None:
    _drop_step_persistence_tables()
    _drop_agentman_tables()
    op.execute(f"DROP SCHEMA IF EXISTS {STEP_PERSISTENCE} CASCADE")
    op.execute(f"DROP SCHEMA IF EXISTS {AGENTMAN} CASCADE")


def _create_agentman_tables() -> None:
    op.create_table(
        "thread",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("workspace", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=AGENTMAN,
    )
    op.create_index("ix_thread_workspace_updated", "thread", ["workspace", "updated_at"], schema=AGENTMAN)

    op.create_table(
        "run",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("thread_id", sa.String(length=64), nullable=False),
        sa.Column("workspace", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("output_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("output_type", sa.Text(), nullable=True),
        sa.Column("messages_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("usage", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema=AGENTMAN,
    )
    op.create_index("ix_run_thread_created", "run", ["thread_id", "created_at"], schema=AGENTMAN)


def _create_step_persistence_tables() -> None:
    op.create_table(
        "runs",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=True),
        sa.Column("parent_run_id", sa.Text(), nullable=True),
        sa.Column("agent_name", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), server_default=_EMPTY_JSONB, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("registration_id", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
        schema=STEP_PERSISTENCE,
    )
    op.create_index("ix_step_runs_conversation", "runs", ["conversation_id"], schema=STEP_PERSISTENCE)

    op.create_table(
        "events",
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=True),
        sa.Column("parent_run_id", sa.Text(), nullable=True),
        sa.Column("agent_name", sa.Text(), nullable=True),
        sa.Column("tool_call_id", sa.Text(), nullable=True),
        sa.Column("tool_name", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), server_default=_EMPTY_JSONB, nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("seq"),
        schema=STEP_PERSISTENCE,
    )
    op.create_index(
        "uq_step_events_idempotency",
        "events",
        ["run_id", "idempotency_key"],
        unique=True,
        schema=STEP_PERSISTENCE,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )
    op.create_index("ix_step_events_run_seq", "events", ["run_id", "seq"], schema=STEP_PERSISTENCE)

    op.create_table(
        "messages",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("message", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("run_id", "content_hash"),
        schema=STEP_PERSISTENCE,
    )

    op.create_table(
        "snapshots",
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("conversation_id", sa.Text(), nullable=True),
        sa.Column("parent_run_id", sa.Text(), nullable=True),
        sa.Column("agent_name", sa.Text(), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("state", sa.Text(), server_default=sa.text("'complete'"), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("message_hashes", postgresql.ARRAY(sa.Text()), server_default=_EMPTY_TEXT_ARRAY, nullable=False),
        sa.PrimaryKeyConstraint("seq"),
        schema=STEP_PERSISTENCE,
    )
    op.create_index("ix_step_snapshots_run_seq", "snapshots", ["run_id", "seq"], schema=STEP_PERSISTENCE)
    op.create_index(
        "uq_step_snapshots_idempotency",
        "snapshots",
        ["run_id", "idempotency_key"],
        unique=True,
        schema=STEP_PERSISTENCE,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )

    op.create_table(
        "tool_effects",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("tool_call_id", sa.Text(), nullable=False),
        sa.Column("tool_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=_NOW, nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("effect_summary", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("run_id", "tool_call_id"),
        schema=STEP_PERSISTENCE,
    )

    op.create_table(
        "snapshot_idempotency_keys",
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("run_id", "idempotency_key"),
        schema=STEP_PERSISTENCE,
    )


def _drop_step_persistence_tables() -> None:
    op.drop_table("snapshot_idempotency_keys", schema=STEP_PERSISTENCE)
    op.drop_table("tool_effects", schema=STEP_PERSISTENCE)
    op.drop_table("snapshots", schema=STEP_PERSISTENCE)
    op.drop_table("messages", schema=STEP_PERSISTENCE)
    op.drop_table("events", schema=STEP_PERSISTENCE)
    op.drop_table("runs", schema=STEP_PERSISTENCE)


def _drop_agentman_tables() -> None:
    op.drop_table("run", schema=AGENTMAN)
    op.drop_table("thread", schema=AGENTMAN)
