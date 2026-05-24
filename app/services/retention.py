from datetime import datetime, timedelta, timezone

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert_history import AlertHistory
from app.models.network_metric import NetworkMetric
from app.models.process_metric import ProcessMetric
from app.models.system_metric import SystemMetric


async def run_retention_cleanup(db: AsyncSession) -> dict:
    now = datetime.now(timezone.utc)
    cutoff_30d = now - timedelta(days=30)
    cutoff_7d = now - timedelta(days=7)
    cutoff_90d = now - timedelta(days=90)

    result = await db.execute(
        delete(SystemMetric).where(SystemMetric.collected_at < cutoff_30d)
    )
    sm_deleted = result.rowcount

    result = await db.execute(
        delete(NetworkMetric).where(NetworkMetric.collected_at < cutoff_30d)
    )
    nm_deleted = result.rowcount

    result = await db.execute(
        delete(ProcessMetric).where(ProcessMetric.collected_at < cutoff_7d)
    )
    pm_deleted = result.rowcount

    result = await db.execute(
        delete(AlertHistory).where(AlertHistory.triggered_at < cutoff_90d)
    )
    ah_deleted = result.rowcount

    await db.commit()

    return {
        "system_metrics_deleted": sm_deleted,
        "network_metrics_deleted": nm_deleted,
        "process_metrics_deleted": pm_deleted,
        "alert_history_deleted": ah_deleted,
    }
