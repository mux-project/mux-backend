import math
import uuid
from datetime import datetime

from sqlalchemy import func as sa_func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert_rule import AlertRule
from app.models.alert_history import AlertHistory
from app.models.node import Node
from app.schemas.alert_history import AlertHistoryResponse
from app.schemas.alert_rule import (
    ALERT_STATUSES,
    VALID_OPERATORS,
    AlertRuleCreate,
    AlertRuleResponse,
    AlertRuleUpdate,
)
from app.services.nodes import resolve_node_uuid


async def list_alert_rules(
    db: AsyncSession,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    count_query = select(sa_func.count()).select_from(AlertRule)
    total = await db.scalar(count_query)

    query = (
        select(AlertRule)
        .order_by(AlertRule.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    items = [
        AlertRuleResponse.model_validate(r)
        for r in result.scalars().all()
    ]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
    }


async def get_alert_rule(
    db: AsyncSession,
    rule_id: uuid.UUID,
) -> AlertRule | None:
    return await db.get(AlertRule, rule_id)


async def create_alert_rule(
    db: AsyncSession,
    payload: AlertRuleCreate,
) -> AlertRule:
    if payload.operator not in VALID_OPERATORS:
        raise ValueError(
            f"Invalid operator '{payload.operator}'. Must be one of: {', '.join(sorted(VALID_OPERATORS))}"
        )

    rule = AlertRule(**payload.model_dump())
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return rule


async def update_alert_rule(
    db: AsyncSession,
    rule_id: uuid.UUID,
    payload: AlertRuleUpdate,
) -> AlertRule | None:
    rule = await db.get(AlertRule, rule_id)
    if not rule:
        return None

    if payload.operator is not None and payload.operator not in VALID_OPERATORS:
        raise ValueError(
            f"Invalid operator '{payload.operator}'. Must be one of: {', '.join(sorted(VALID_OPERATORS))}"
        )

    update_data = payload.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(rule, key, value)

    await db.commit()
    await db.refresh(rule)
    return rule


async def delete_alert_rule(
    db: AsyncSession,
    rule_id: uuid.UUID,
) -> bool:
    rule = await db.get(AlertRule, rule_id)
    if not rule:
        return False
    await db.delete(rule)
    await db.commit()
    return True


async def list_alert_history(
    db: AsyncSession,
    status: str | None = None,
    node_uuid: uuid.UUID | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    page: int = 1,
    page_size: int = 20,
    rule_id: uuid.UUID | None = None,
) -> dict:
    if status is not None and status not in ALERT_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. Must be one of: {', '.join(sorted(ALERT_STATUSES))}"
        )

    filters = []
    if status:
        filters.append(AlertHistory.status == status)
    if node_uuid:
        node_id = await resolve_node_uuid(db, node_uuid)
        filters.append(AlertHistory.node_id == node_id)
    if start:
        filters.append(AlertHistory.triggered_at >= start)
    if end:
        filters.append(AlertHistory.triggered_at <= end)
    if rule_id:
        filters.append(AlertHistory.rule_id == rule_id)

    count_query = select(sa_func.count()).select_from(AlertHistory)
    if filters:
        count_query = count_query.where(*filters)
    total = await db.scalar(count_query)

    query = (
        select(
            AlertHistory,
            AlertRule.name.label("_rule_name"),
            Node.node_uuid.label("_node_uuid"),
        )
        .join(AlertRule, AlertHistory.rule_id == AlertRule.id)
        .join(Node, AlertHistory.node_id == Node.id)
        .where(*filters)
        .order_by(AlertHistory.triggered_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    rows = result.all()

    items = []
    for row in rows:
        history = row.AlertHistory
        rule_name = row._rule_name
        node_uuid_val = row._node_uuid
        items.append(
            AlertHistoryResponse(
                id=history.id,
                rule_id=history.rule_id,
                rule_name=rule_name,
                node_uuid=str(node_uuid_val) if node_uuid_val else None,
                status=history.status,
                metric_value=history.metric_value,
                message=history.message,
                triggered_at=history.triggered_at,
                resolved_at=history.resolved_at,
                acknowledged_at=history.acknowledged_at,
            )
        )

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
    }


async def acknowledge_alert(
    db: AsyncSession,
    history_id: uuid.UUID,
) -> AlertHistory | None:
    history = await db.get(AlertHistory, history_id)
    if not history:
        return None
    history.acknowledged_at = datetime.now()
    history.status = "acknowledged"
    await db.commit()
    await db.refresh(history)
    return history
