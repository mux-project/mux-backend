from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SystemMetricCreate(BaseModel):
    node_id: str
    cpu_percent: float
    memory_percent: float
    memory_used_mb: int | None = None
    memory_total_mb: int | None = None
    disk_percent: float
    disk_used_gb: float | None = None
    disk_total_gb: float | None = None
    load_avg_1m: float | None = None
    load_avg_5m: float | None = None
    load_avg_15m: float | None = None
    collected_at: datetime | None = None


class SystemMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    node_id: str
    cpu_percent: float
    memory_percent: float
    memory_used_mb: int | None = None
    memory_total_mb: int | None = None
    disk_percent: float
    disk_used_gb: float | None = None
    disk_total_gb: float | None = None
    load_avg_1m: float | None = None
    load_avg_5m: float | None = None
    load_avg_15m: float | None = None
    collected_at: datetime
