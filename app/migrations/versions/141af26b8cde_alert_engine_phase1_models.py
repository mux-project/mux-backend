"""alert_engine_phase1_models

Add version, renotify_interval_seconds, cooldown_seconds to alert_rules.
Add last_notified_at, rule_version, notified_count to alert_history.
Change alert_history.rule_id FK → ON DELETE SET NULL.
Add partial unique index (rule_id, node_id) WHERE status='firing'.

Revision ID: 141af26b8cde
Revises: 1e3158762827
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "141af26b8cde"
down_revision: Union[str, Sequence[str], None] = "1e3158762827"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- alert_rules: new columns ---
    op.add_column(
        "alert_rules",
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.add_column(
        "alert_rules",
        sa.Column(
            "renotify_interval_seconds",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("900"),
        ),
    )
    op.add_column(
        "alert_rules",
        sa.Column(
            "cooldown_seconds",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("60"),
        ),
    )

    # --- alert_history: new columns ---
    op.add_column(
        "alert_history",
        sa.Column("last_notified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alert_history",
        sa.Column("rule_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "alert_history",
        sa.Column(
            "notified_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )

    # --- alert_history: change FK rule_id → ON DELETE SET NULL ---
    op.drop_constraint("alert_history_rule_id_fkey", "alert_history", type_="foreignkey")
    op.create_foreign_key(
        "alert_history_rule_id_fkey",
        "alert_history",
        "alert_rules",
        ["rule_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # --- alert_history: partial unique index (rule_id, node_id) WHERE status='firing' ---
    op.create_index(
        "uq_alert_history_active",
        "alert_history",
        ["rule_id", "node_id"],
        postgresql_where=sa.text("status = 'firing'"),
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_alert_history_active", table_name="alert_history",
                  postgresql_where=sa.text("status = 'firing'"))

    op.drop_constraint("alert_history_rule_id_fkey", "alert_history", type_="foreignkey")
    op.create_foreign_key(
        "alert_history_rule_id_fkey",
        "alert_history",
        "alert_rules",
        ["rule_id"],
        ["id"],
    )

    op.drop_column("alert_history", "notified_count")
    op.drop_column("alert_history", "rule_version")
    op.drop_column("alert_history", "last_notified_at")

    op.drop_column("alert_rules", "cooldown_seconds")
    op.drop_column("alert_rules", "renotify_interval_seconds")
    op.drop_column("alert_rules", "version")
