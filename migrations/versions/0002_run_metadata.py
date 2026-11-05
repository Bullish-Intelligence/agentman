"""Add retry metadata to product runs.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-02
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "run",
        sa.Column(
            "run_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        schema="agentman",
    )


def downgrade() -> None:
    op.drop_column("run", "run_metadata", schema="agentman")
