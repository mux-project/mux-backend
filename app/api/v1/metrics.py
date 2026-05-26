import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.dependencies import get_db
from app.schemas.metric_payload import IngestResponse, MetricPayload
from app.schemas.network_metric import NetworkMetricResponse
from app.schemas.process_metric import ProcessMetricResponse
from app.services.metrics import (
    create_metrics,
    export_metrics,
    get_current_metrics,
    get_metric_history,
    get_network_metrics,
    get_top_processes,
)

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.post("/ingest", response_model=IngestResponse)
async def ingest_metric(
    payload: MetricPayload,
    db: AsyncSession = Depends(get_db),
):
    return await create_metrics(db, payload)


@router.get("/current")
async def current_metrics(
    node_uuid: uuid.UUID | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await get_current_metrics(db, node_uuid)


@router.get("/history")
async def metric_history(
    node_uuid: uuid.UUID,
    start: datetime,
    end: datetime,
    interval: str = Query("1 hour"),
    db: AsyncSession = Depends(get_db),
):
    return await get_metric_history(db, node_uuid, start, end, interval)


@router.get("/network", response_model=list[NetworkMetricResponse])
async def network_metrics(
    node_uuid: uuid.UUID,
    start: datetime,
    end: datetime,
    db: AsyncSession = Depends(get_db),
):
    return await get_network_metrics(db, node_uuid, start, end)


@router.get("/processes", response_model=list[ProcessMetricResponse])
async def top_processes(
    node_uuid: uuid.UUID,
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await get_top_processes(db, node_uuid, limit)


@router.get("/export")
async def export_metrics_endpoint(
    node_uuid: uuid.UUID,
    start: datetime,
    end: datetime,
    fmt: str = Query("json", alias="format"),
    db: AsyncSession = Depends(get_db),
):
    result = await export_metrics(db, node_uuid, start, end, fmt)
    if fmt == "csv":
        return PlainTextResponse(
            result, media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=metrics.csv"},
        )
    return result
