"""create_alert_history_table

Revision ID: 93d67cd965ee
Revises: f446e6bfc84f
Create Date: 2026-05-24 16:59:54.392347

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "93d67cd965ee"
down_revision: Union[str, Sequence[str], None] = "f446e6bfc84f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "alert_history",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("rule_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("alert_rules.id"), nullable=False),
        sa.Column("node_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("nodes.id"), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("metric_value", sa.Float(), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("notified_channels", postgresql.JSONB, nullable=True),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id"), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_alert_history_triggered_at", "alert_history",
                    ["triggered_at"], postgresql_using="brin")
    op.create_index("ix_alert_history_rule_id", "alert_history", ["rule_id"])
    op.create_index("ix_alert_history_firing", "alert_history", ["triggered_at"],
                    postgresql_where=sa.text("status = 'firing'"))


def downgrade() -> None:
    op.drop_index("ix_alert_history_firing", table_name="alert_history",
                  postgresql_where=sa.text("status = 'firing'"))
    op.drop_index("ix_alert_history_rule_id", table_name="alert_history")
    op.drop_index("ix_alert_history_triggered_at", table_name="alert_history")
    op.drop_table("alert_history")
