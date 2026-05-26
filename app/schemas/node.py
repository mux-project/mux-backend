import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, model_validator


class NodeCreate(BaseModel):
    node_uuid: uuid.UUID
    hostname: str
    ip_address: str
    os_version: str | None = None
    agent_version: str | None = None


class NodeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    node_uuid: uuid.UUID
    hostname: str
    ip_address: str
    os_version: str | None = None
    agent_version: str | None = None
    is_active: bool
    last_heartbeat_at: datetime | None = None
    registered_at: datetime
    status: str = "unknown"

    @model_validator(mode="after")
    def compute_status(self):
        if not self.is_active:
            self.status = "inactive"
        elif self.last_heartbeat_at and (
            datetime.now(timezone.utc) - self.last_heartbeat_at
        ).total_seconds() < 300:
            self.status = "online"
        else:
            self.status = "offline"
        return self


class LatestMetricSnapshot(BaseModel):
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


class NodeDetailResponse(BaseModel):
    id: str
    node_uuid: uuid.UUID
    hostname: str
    ip_address: str
    os_version: str | None = None
    agent_version: str | None = None
    is_active: bool
    last_heartbeat_at: datetime | None = None
    registered_at: datetime
    latest_metrics: LatestMetricSnapshot | None = None


class NodeRegistrationResponse(BaseModel):
    node: NodeResponse
    api_key: str


class NodeHeartbeatResponse(BaseModel):
    status: str = "ok"
    message: str = "Heartbeat received"


class NodeDeleteResponse(BaseModel):
    status: str = "ok"
    message: str = "Node deregistered"
