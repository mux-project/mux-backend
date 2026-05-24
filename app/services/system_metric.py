from sqlalchemy.ext.asyncio import AsyncSession

from app.models.system_metric import SystemMetric
from app.schemas.system_metric import SystemMetricCreate

async def create_metric(
    db: AsyncSession,
    payload: SystemMetricCreate,
):
    metric = SystemMetric(**payload.model_dump())

    db.add(metric)

    await db.commit()

    await db.refresh(metric)

    return metric