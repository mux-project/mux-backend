"""add_tenant_id_to_alert_history

Add tenant_id column to alert_history so recovery can restore
tenant-scoped Redis keys instead of creating no-tenant ghosts (F1).

Revision ID: a3f7b2c9d0e1
Revises: 1e94c6e2bf0a
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a3f7b2c9d0e1"
down_revision: Union[str, Sequence[str], None] = "1e94c6e2bf0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("alert_history", sa.Column("tenant_id", sa.String(64), nullable=True))
    op.execute("UPDATE alert_history SET tenant_id = '' WHERE tenant_id IS NULL")
    op.alter_column("alert_history", "tenant_id", nullable=False)


def downgrade() -> None:
    op.drop_column("alert_history", "tenant_id")
