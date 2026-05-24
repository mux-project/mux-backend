"""create_system_metrics_table

Revision ID: b6125dc9820e
Revises: c5dabc129aec
Create Date: 2026-05-24 16:59:53.293479

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "b6125dc9820e"
down_revision: Union[str, Sequence[str], None] = "c5dabc129aec"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "system_metrics",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("node_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("nodes.id"), nullable=False),
        sa.Column("cpu_percent", sa.Float(), nullable=False),
        sa.Column("memory_percent", sa.Float(), nullable=False),
        sa.Column("memory_used_mb", sa.Integer(), nullable=True),
        sa.Column("memory_total_mb", sa.Integer(), nullable=True),
        sa.Column("disk_percent", sa.Float(), nullable=False),
        sa.Column("disk_used_gb", sa.Float(), nullable=True),
        sa.Column("disk_total_gb", sa.Float(), nullable=True),
        sa.Column("load_avg_1m", sa.Float(), nullable=True),
        sa.Column("load_avg_5m", sa.Float(), nullable=True),
        sa.Column("load_avg_15m", sa.Float(), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    op.create_index("ix_system_metrics_collected_at", "system_metrics",
                    ["collected_at"], postgresql_using="brin")
    op.create_index("ix_system_metrics_node_collected", "system_metrics",
                    ["node_id", sa.text("collected_at DESC")])


def downgrade() -> None:
    op.drop_index("ix_system_metrics_node_collected", table_name="system_metrics")
    op.drop_index("ix_system_metrics_collected_at", table_name="system_metrics")
    op.drop_table("system_metrics")
