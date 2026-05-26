from datetime import datetime

from pydantic import BaseModel, ConfigDict

import uuid


class AlertRuleCreate(BaseModel):
    name: str
    description: str | None = None
    metric_field: str
    operator: str
    threshold: float
    duration_seconds: int = 0
    channels: list[dict] = []
    node_ids: list[uuid.UUID] | None = None
    is_active: bool = True


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    metric_field: str | None = None
    operator: str | None = None
    threshold: float | None = None
    duration_seconds: int | None = None
    channels: list[dict] | None = None
    node_ids: list[uuid.UUID] | None = None
    is_active: bool | None = None


class AlertRuleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str | None
    metric_field: str
    operator: str
    threshold: float
    duration_seconds: int | None
    channels: list
    node_ids: list[uuid.UUID] | None
    is_active: bool
    created_at: datetime
    updated_at: datetime
