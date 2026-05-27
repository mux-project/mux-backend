"""add_alert_breaches_table

Add alert_breaches table for persisting in-progress breach windows
across worker restarts (crash recovery for duration-based alerts).

Revision ID: 22485a76743e
Revises: 141af26b8cde
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "22485a76743e"
down_revision: Union[str, Sequence[str], None] = "141af26b8cde"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "alert_breaches",
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("node_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("rule_id", "node_id"),
    )


def downgrade() -> None:
    op.drop_table("alert_breaches")
