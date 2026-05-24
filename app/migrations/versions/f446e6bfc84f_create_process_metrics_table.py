"""create_process_metrics_table

Revision ID: f446e6bfc84f
Revises: 8377e0eb7508
Create Date: 2026-05-24 16:59:54.010306

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "f446e6bfc84f"
down_revision: Union[str, Sequence[str], None] = "8377e0eb7508"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "process_metrics",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("node_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("nodes.id"), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("cpu_percent", sa.Float(), nullable=False),
        sa.Column("memory_percent", sa.Float(), nullable=False),
        sa.Column("memory_rss_mb", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(32), nullable=True),
        sa.Column("username", sa.String(128), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    op.create_index("ix_process_metrics_collected_at", "process_metrics",
                    ["collected_at"], postgresql_using="brin")
    op.create_index("ix_process_metrics_node_name", "process_metrics",
                    ["node_id", "name"])


def downgrade() -> None:
    op.drop_index("ix_process_metrics_node_name", table_name="process_metrics")
    op.drop_index("ix_process_metrics_collected_at", table_name="process_metrics")
    op.drop_table("process_metrics")
