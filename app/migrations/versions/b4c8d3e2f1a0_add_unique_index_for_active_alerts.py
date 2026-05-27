"""add_unique_index_for_active_alerts

Add partial unique index on alert_history (tenant_id, rule_id, node_id)
WHERE status = 'firing'. This is the DB-side fence that prevents
concurrent fire_alert calls from inserting duplicate active alerts
for the same (tenant, rule, node) — the core race condition fix (F1).

Revision ID: b4c8d3e2f1a0
Revises: a3f7b2c9d0e1
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b4c8d3e2f1a0"
down_revision: Union[str, Sequence[str], None] = "a3f7b2c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_alert_history_active",
        "alert_history",
        ["tenant_id", "rule_id", "node_id"],
        unique=True,
        postgresql_where=sa.text("status = 'firing'"),
    )


def downgrade() -> None:
    op.drop_index("ix_alert_history_active", table_name="alert_history")
