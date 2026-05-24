"""create_nodes_table

Revision ID: 96fee90db037
Revises: becea799d9c8
Create Date: 2026-05-24 16:59:46.519278

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "96fee90db037"
down_revision: Union[str, Sequence[str], None] = "becea799d9c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "nodes",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("ip_address", postgresql.INET, nullable=False),
        sa.Column("os_version", sa.String(255), nullable=True),
        sa.Column("agent_version", sa.String(50), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False,
                  server_default=sa.text("true")),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("api_key_hash", sa.String(255), nullable=False, unique=True),
    )


def downgrade() -> None:
    op.drop_table("nodes")
