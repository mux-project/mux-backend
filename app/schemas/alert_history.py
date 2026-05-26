from datetime import datetime

from pydantic import BaseModel

import uuid


class AlertHistoryResponse(BaseModel):
    id: uuid.UUID
    rule_id: uuid.UUID
    rule_name: str | None = None
    node_uuid: uuid.UUID | str | None = None
    status: str
    metric_value: float
    message: str | None = None
    triggered_at: datetime
    resolved_at: datetime | None = None
    acknowledged_at: datetime | None = None


class AlertAckResponse(BaseModel):
    status: str = "ok"
    message: str = "Alert acknowledged"
