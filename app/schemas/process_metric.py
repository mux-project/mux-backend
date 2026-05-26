from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ProcessMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    node_id: str
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float
    memory_rss_mb: int | None = None
    status: str | None = None
    username: str | None = None
    collected_at: datetime
