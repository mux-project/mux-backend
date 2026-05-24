import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.system_metric import SystemMetric
from app.schemas.system_metric import SystemMetricCreate


async def create_metric(
    db: AsyncSession,
    payload: SystemMetricCreate,
):
    data = payload.model_dump(exclude_none=True, exclude={"collected_at"})
    data["node_id"] = uuid.UUID(data["node_id"])
    metric = SystemMetric(**data)

    db.add(metric)
    await db.commit()
    await db.refresh(metric)

    return metric
