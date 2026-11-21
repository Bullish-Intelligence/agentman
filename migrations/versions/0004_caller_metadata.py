"""Add the metadata a caller attaches to a run.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "run",
        sa.Column(
            "caller_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        schema="agentman",
    )


def downgrade() -> None:
    op.drop_column("run", "caller_metadata", schema="agentman")
