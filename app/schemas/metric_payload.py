from datetime import datetime

from pydantic import BaseModel


class SystemMetricData(BaseModel):
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


class NetworkInterfaceData(BaseModel):
    interface: str
    bytes_sent: int = 0
    bytes_recv: int = 0
    packets_sent: int | None = None
    packets_recv: int | None = None
    errors_in: int | None = 0
    errors_out: int | None = 0


class ProcessData(BaseModel):
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float
    memory_rss_mb: int | None = None
    status: str | None = None
    username: str | None = None


class MetricPayload(BaseModel):
    node_id: str
    collected_at: datetime | None = None
    system: SystemMetricData | None = None
    network: list[NetworkInterfaceData] | None = None
    processes: list[ProcessData] | None = None


class IngestResponse(BaseModel):
    status: str = "ok"
    system_metric: int = 0
    network_metrics: int = 0
    process_metrics: int = 0
