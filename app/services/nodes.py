import secrets
import uuid
from datetime import datetime, timezone

from fastapi import HTTPException
import bcrypt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.node import Node
from app.models.system_metric import SystemMetric
from app.schemas.node import NodeCreate


async def get_nodes(db: AsyncSession) -> list[Node]:
    result = await db.execute(select(Node).order_by(Node.hostname))
    return list(result.scalars().all())


async def get_node_by_node_uuid(
    db: AsyncSession, node_uuid: uuid.UUID
) -> Node | None:
    result = await db.execute(
        select(Node).where(Node.node_uuid == node_uuid)
    )
    return result.scalar_one_or_none()


async def resolve_node_uuid(
    db: AsyncSession, node_uuid: uuid.UUID
) -> uuid.UUID:
    result = await db.execute(
        select(Node.id).where(Node.node_uuid == node_uuid)
    )
    node_id = result.scalar_one_or_none()
    if node_id is None:
        raise HTTPException(status_code=404, detail="Node not found")
    return node_id


async def get_node_with_metrics(
    db: AsyncSession, node_uuid: uuid.UUID
) -> tuple[Node | None, SystemMetric | None]:
    node = await get_node_by_node_uuid(db, node_uuid)
    if not node:
        return None, None
    result = await db.execute(
        select(SystemMetric)
        .where(SystemMetric.node_id == node.id)
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

    existing = (
        await db.execute(select(Node).where(Node.agent_id == payload.agent_id))
    ).scalar_one_or_none()

    if existing:
        existing.hostname = payload.hostname
        existing.ip_address = payload.ip_address
        existing.os_version = payload.os_version
        existing.agent_version = payload.agent_version
        existing.api_key_hash = api_key_hash
        node = existing
    else:
        node = Node(
            node_uuid=uuid.uuid4(),
            agent_id=payload.agent_id,
            hostname=payload.hostname,
            ip_address=payload.ip_address,
            os_version=payload.os_version,
            agent_version=payload.agent_version,
            api_key_hash=api_key_hash,
        )
        db.add(node)

    await db.commit()
    await db.refresh(node)
    return node, api_key


async def deregister_node(db: AsyncSession, node_uuid: uuid.UUID) -> bool:
    node = await get_node_by_node_uuid(db, node_uuid)
    if not node:
        return False
    node.is_active = False
    await db.commit()
    return True


async def heartbeat(db: AsyncSession, node_uuid: uuid.UUID) -> bool:
    node = await get_node_by_node_uuid(db, node_uuid)
    if not node:
        return False
    node.last_heartbeat_at = datetime.now(timezone.utc)
    await db.commit()
    return True
