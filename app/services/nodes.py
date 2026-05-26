import secrets
import uuid
from datetime import datetime, timezone

import bcrypt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.node import Node
from app.models.system_metric import SystemMetric
from app.schemas.node import NodeCreate


async def get_nodes(db: AsyncSession) -> list[Node]:
    result = await db.execute(select(Node).order_by(Node.hostname))
    return list(result.scalars().all())


async def get_node(db: AsyncSession, node_id: uuid.UUID) -> Node | None:
    return await db.get(Node, node_id)


async def get_node_with_metrics(
    db: AsyncSession, node_id: uuid.UUID
) -> tuple[Node | None, SystemMetric | None]:
    node = await db.get(Node, node_id)
    if not node:
        return None, None
    result = await db.execute(
        select(SystemMetric)
        .where(SystemMetric.node_id == node_id)
        .order_by(SystemMetric.collected_at.desc())
        .limit(1)
    )
    latest_metric = result.scalar_one_or_none()
    return node, latest_metric


async def register_node(
    db: AsyncSession, payload: NodeCreate
) -> tuple[Node, str]:
    api_key = secrets.token_urlsafe(32)
    api_key_hash = bcrypt.hashpw(api_key.encode(), bcrypt.gensalt()).decode()
    node_uuid = payload.node_uuid

    existing = (
        await db.execute(select(Node).where(Node.node_uuid == node_uuid))
    ).scalar_one_or_none()

    if existing:
        existing.hostname = payload.hostname
        existing.ip_address = payload.ip_address
        existing.os_version = payload.os_version
        existing.agent_version = payload.agent_version
        existing.api_key_hash = api_key_hash
        node = existing
    else:
        data = payload.model_dump()
        data["api_key_hash"] = api_key_hash
        data["node_uuid"] = node_uuid
        node = Node(**data)
        db.add(node)

    await db.commit()
    await db.refresh(node)
    return node, api_key


async def deregister_node(db: AsyncSession, node_id: uuid.UUID) -> bool:
    node = await db.get(Node, node_id)
    if not node:
        return False
    node.is_active = False
    await db.commit()
    return True


async def heartbeat(db: AsyncSession, node_id: uuid.UUID) -> bool:
    node = await db.get(Node, node_id)
    if not node:
        return False
    node.last_heartbeat_at = datetime.now(timezone.utc)
    await db.commit()
    return True
