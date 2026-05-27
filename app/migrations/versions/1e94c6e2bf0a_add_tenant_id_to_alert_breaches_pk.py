"""add_tenant_id_to_alert_breaches_pk

Add tenant_id column to alert_breaches and extend the primary key to
(rule_id, node_id, tenant_id) so each tenant's breach windows are
isolated during crash recovery (H2).

Revision ID: 1e94c6e2bf0a
Revises: 22485a76743e
Create Date: 2026-05-27

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "1e94c6e2bf0a"
down_revision: Union[str, Sequence[str], None] = "22485a76743e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add column (nullable so we can backfill existing rows)
    op.add_column("alert_breaches", sa.Column("tenant_id", sa.String(64), nullable=True))

    # 2. Backfill existing rows with empty string (global / legacy)
    op.execute("UPDATE alert_breaches SET tenant_id = '' WHERE tenant_id IS NULL")

    # 3. Make NOT NULL
    op.alter_column("alert_breaches", "tenant_id", nullable=False)

    # 4. Drop the old PK
    op.drop_constraint("alert_breaches_pkey", "alert_breaches", type_="primary")

    # 5. Create new PK with tenant_id
    op.create_primary_key(
        "alert_breaches_pkey",
        "alert_breaches",
        ["rule_id", "node_id", "tenant_id"],
    )


def downgrade() -> None:
    op.drop_constraint("alert_breaches_pkey", "alert_breaches", type_="primary")
    op.create_primary_key(
        "alert_breaches_pkey",
        "alert_breaches",
        ["rule_id", "node_id"],
    )
    op.drop_column("alert_breaches", "tenant_id")
