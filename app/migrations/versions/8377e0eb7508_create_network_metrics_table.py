"""create_network_metrics_table

Revision ID: 8377e0eb7508
Revises: b6125dc9820e
Create Date: 2026-05-24 16:59:53.639743

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "8377e0eb7508"
down_revision: Union[str, Sequence[str], None] = "b6125dc9820e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "network_metrics",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("node_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("nodes.id"), nullable=False),
        sa.Column("interface", sa.String(64), nullable=False),
        sa.Column("bytes_sent", sa.BigInteger(), nullable=False,
                  server_default=sa.text("0")),
        sa.Column("bytes_recv", sa.BigInteger(), nullable=False,
                  server_default=sa.text("0")),
        sa.Column("packets_sent", sa.BigInteger(), nullable=True),
        sa.Column("packets_recv", sa.BigInteger(), nullable=True),
        sa.Column("errors_in", sa.Integer(), nullable=True,
                  server_default=sa.text("0")),
        sa.Column("errors_out", sa.Integer(), nullable=True,
                  server_default=sa.text("0")),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    op.create_index("ix_network_metrics_collected_at", "network_metrics",
                    ["collected_at"], postgresql_using="brin")


def downgrade() -> None:
    op.drop_index("ix_network_metrics_collected_at", table_name="network_metrics")
    op.drop_table("network_metrics")
