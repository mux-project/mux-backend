from datetime import datetime

from pydantic import BaseModel, ConfigDict


class NetworkMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    node_id: str
    interface: str
    bytes_sent: int
    bytes_recv: int
    packets_sent: int | None = None
    packets_recv: int | None = None
    errors_in: int | None = None
    errors_out: int | None = None
    collected_at: datetime
