"""add_node_uuid_to_nodes

Revision ID: 8d3ae358e2c5
Revises: 93d67cd965ee
Create Date: 2026-05-24 21:11:53.400995

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8d3ae358e2c5"
down_revision: Union[str, Sequence[str], None] = "93d67cd965ee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("nodes", sa.Column("node_uuid", sa.UUID(), nullable=True))
    op.execute("UPDATE nodes SET node_uuid = gen_random_uuid() WHERE node_uuid IS NULL")
    op.alter_column("nodes", "node_uuid", nullable=False)
    op.create_unique_constraint("uq_nodes_node_uuid", "nodes", ["node_uuid"])


def downgrade() -> None:
    op.drop_constraint("uq_nodes_node_uuid", "nodes", type_="unique")
    op.drop_column("nodes", "node_uuid")
