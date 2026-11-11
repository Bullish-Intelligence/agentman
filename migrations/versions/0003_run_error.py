"""Add the end reason of a failed or cancelled run.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "run",
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema="agentman",
    )


def downgrade() -> None:
    op.drop_column("run", "error", schema="agentman")
