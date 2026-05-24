from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.dependencies import get_db
from app.schemas.system_metric import SystemMetricCreate, SystemMetricResponse
from app.services.system_metric import create_metric

router = APIRouter(tags=["metrics"])

@router.post("/metrics", response_model=SystemMetricResponse)
async def ingest_metric(
    payload: SystemMetricCreate,
    db: AsyncSession = Depends(get_db),
):
    return await create_metric(db, payload)
