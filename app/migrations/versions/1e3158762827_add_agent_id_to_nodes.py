"""add_agent_id_to_nodes

Revision ID: 1e3158762827
Revises: 8d3ae358e2c5
Create Date: 2026-05-25 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1e3158762827"
down_revision: Union[str, Sequence[str], None] = "8d3ae358e2c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("nodes", sa.Column("agent_id", sa.String(255), nullable=True))
    op.execute("UPDATE nodes SET agent_id = gen_random_uuid()::text WHERE agent_id IS NULL")
    op.alter_column("nodes", "agent_id", nullable=False)
    op.create_unique_constraint("uq_nodes_agent_id", "nodes", ["agent_id"])


def downgrade() -> None:
    op.drop_constraint("uq_nodes_agent_id", "nodes", type_="unique")
    op.drop_column("nodes", "agent_id")
