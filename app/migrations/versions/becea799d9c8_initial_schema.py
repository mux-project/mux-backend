"""head_setup

Revision ID: becea799d9c8
Revises:
Create Date: 2026-05-24 16:45:29.140189

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "becea799d9c8"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
