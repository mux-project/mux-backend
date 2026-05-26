import csv
import io
import uuid
from datetime import datetime, timezone

from sqlalchemy import func as sa_func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.network_metric import NetworkMetric
from app.models.process_metric import ProcessMetric
from app.models.system_metric import SystemMetric
from app.schemas.metric_payload import IngestResponse, MetricPayload


async def get_current_metrics(
    db: AsyncSession,
    node_id: uuid.UUID | None = None,
) -> list[SystemMetric]:
    subq = select(
        SystemMetric.node_id,
        sa_func.max(SystemMetric.collected_at).label("max_collected_at"),
    )
    if node_id:
        subq = subq.where(SystemMetric.node_id == node_id)
    subq = subq.group_by(SystemMetric.node_id).subquery()

    query = (
        select(SystemMetric)
        .join(
            subq,
            (SystemMetric.node_id == subq.c.node_id)
            & (SystemMetric.collected_at == subq.c.max_collected_at),
        )
        .order_by(SystemMetric.node_id)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_metric_history(
    db: AsyncSession,
    node_id: uuid.UUID,
    start: datetime,
    end: datetime,
    interval: str,
) -> list[dict]:
    bucket = sa_func.date_trunc(interval, SystemMetric.collected_at)
    query = (
        select(
            bucket.label("bucket"),
            sa_func.avg(SystemMetric.cpu_percent).label("avg_cpu"),
            sa_func.avg(SystemMetric.memory_percent).label("avg_memory"),
            sa_func.avg(SystemMetric.disk_percent).label("avg_disk"),
            sa_func.max(SystemMetric.cpu_percent).label("max_cpu"),
            sa_func.max(SystemMetric.memory_percent).label("max_memory"),
            sa_func.max(SystemMetric.disk_percent).label("max_disk"),
        )
        .where(
            SystemMetric.node_id == node_id,
            SystemMetric.collected_at >= start,
            SystemMetric.collected_at <= end,
        )
        .group_by(bucket)
        .order_by(bucket)
    )
    result = await db.execute(query)
    rows = result.all()
    return [
        {
            "bucket": row.bucket,
            "avg_cpu": round(row.avg_cpu, 2) if row.avg_cpu else None,
            "avg_memory": round(row.avg_memory, 2) if row.avg_memory else None,
            "avg_disk": round(row.avg_disk, 2) if row.avg_disk else None,
            "max_cpu": round(row.max_cpu, 2) if row.max_cpu else None,
            "max_memory": round(row.max_memory, 2) if row.max_memory else None,
            "max_disk": round(row.max_disk, 2) if row.max_disk else None,
        }
        for row in rows
    ]


async def get_network_metrics(
    db: AsyncSession,
    node_id: uuid.UUID,
    start: datetime,
    end: datetime,
) -> list[NetworkMetric]:
    query = (
        select(NetworkMetric)
        .where(
            NetworkMetric.node_id == node_id,
            NetworkMetric.collected_at >= start,
            NetworkMetric.collected_at <= end,
        )
        .order_by(NetworkMetric.collected_at.desc())
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def get_top_processes(
    db: AsyncSession,
    node_id: uuid.UUID,
    limit: int = 10,
) -> list[ProcessMetric]:
    latest = await db.scalar(
        select(sa_func.max(ProcessMetric.collected_at)).where(
            ProcessMetric.node_id == node_id
        )
    )
    if not latest:
        return []

    query = (
        select(ProcessMetric)
        .where(
            ProcessMetric.node_id == node_id,
            ProcessMetric.collected_at == latest,
        )
        .order_by(ProcessMetric.cpu_percent.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    return list(result.scalars().all())


async def export_metrics(
    db: AsyncSession,
    node_id: uuid.UUID,
    start: datetime,
    end: datetime,
    fmt: str = "json",
) -> str | list[dict]:
    query = (
        select(SystemMetric)
        .where(
            SystemMetric.node_id == node_id,
            SystemMetric.collected_at >= start,
            SystemMetric.collected_at <= end,
        )
        .order_by(SystemMetric.collected_at.asc())
    )
    result = await db.execute(query)
    rows = list(result.scalars().all())

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "node_id",
                "cpu_percent",
                "memory_percent",
                "memory_used_mb",
                "memory_total_mb",
                "disk_percent",
                "disk_used_gb",
                "disk_total_gb",
                "load_avg_1m",
                "load_avg_5m",
                "load_avg_15m",
                "collected_at",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.id,
                    r.node_id,
                    r.cpu_percent,
                    r.memory_percent,
                    r.memory_used_mb,
                    r.memory_total_mb,
                    r.disk_percent,
                    r.disk_used_gb,
                    r.disk_total_gb,
                    r.load_avg_1m,
                    r.load_avg_5m,
                    r.load_avg_15m,
                    r.collected_at.isoformat(),
                ]
            )
        return output.getvalue()

    return [
        {
            "id": r.id,
            "node_id": str(r.node_id),
            "cpu_percent": r.cpu_percent,
            "memory_percent": r.memory_percent,
            "memory_used_mb": r.memory_used_mb,
            "memory_total_mb": r.memory_total_mb,
            "disk_percent": r.disk_percent,
            "disk_used_gb": r.disk_used_gb,
            "disk_total_gb": r.disk_total_gb,
            "load_avg_1m": r.load_avg_1m,
            "load_avg_5m": r.load_avg_5m,
            "load_avg_15m": r.load_avg_15m,
            "collected_at": r.collected_at.isoformat(),
        }
        for r in rows
    ]


async def create_metrics(
    db: AsyncSession,
    payload: MetricPayload,
) -> IngestResponse:
    node_id = uuid.UUID(payload.node_id)
    collected_at = payload.collected_at or datetime.now(timezone.utc)
    counts = {"system_metric": 0, "network_metrics": 0, "process_metrics": 0}

    if payload.system is not None:
        data = payload.system.model_dump()
        data["node_id"] = node_id
        data["collected_at"] = collected_at
        db.add(SystemMetric(**data))
        counts["system_metric"] = 1

    if payload.network is not None:
        for iface in payload.network:
            data = iface.model_dump()
            data["node_id"] = node_id
            data["collected_at"] = collected_at
            db.add(NetworkMetric(**data))
            counts["network_metrics"] += 1

    if payload.processes is not None:
        for proc in payload.processes:
            data = proc.model_dump()
            data["node_id"] = node_id
            data["collected_at"] = collected_at
            db.add(ProcessMetric(**data))
            counts["process_metrics"] += 1

    await db.commit()
    return IngestResponse(**counts)
