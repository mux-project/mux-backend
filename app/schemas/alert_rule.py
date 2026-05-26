from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

import uuid

OPERATOR_MAP = {
    ">": "gt",
    "<": "lt",
    ">=": "gte",
    "<=": "lte",
    "==": "eq",
}
VALID_OPERATORS = frozenset({"gt", "lt", "gte", "lte", "eq"})
REVERSE_OPERATOR_MAP = {v: k for k, v in OPERATOR_MAP.items()}
ALERT_STATUSES = frozenset({"firing", "resolved", "acknowledged"})


class AlertRuleCreate(BaseModel):
    name: str
    description: str | None = None
    metric_field: str
    operator: str
    threshold: float
    duration_seconds: int = Field(default=0, ge=0)
    channels: list[dict] = []
    node_ids: list[uuid.UUID] | None = None
    is_active: bool = True

    @field_validator("operator")
    @classmethod
    def normalize_operator(cls, v: str) -> str:
        normalized = OPERATOR_MAP.get(v, v)
        if normalized not in VALID_OPERATORS:
            raise ValueError(
                f"Invalid operator '{v}'. Must be one of: "
                + ", ".join(sorted(VALID_OPERATORS))
            )
        return normalized


class AlertRuleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    metric_field: str | None = None
    operator: str | None = None
    threshold: float | None = None
    duration_seconds: int | None = Field(default=None, ge=0)
    channels: list[dict] | None = None
    node_ids: list[uuid.UUID] | None = None
    is_active: bool | None = None

    @field_validator("operator")
    @classmethod
    def normalize_operator(cls, v: str) -> str:
        if v is None:
            return v
        normalized = OPERATOR_MAP.get(v, v)
        if normalized not in VALID_OPERATORS:
            raise ValueError(
                f"Invalid operator '{v}'. Must be one of: "
                + ", ".join(sorted(VALID_OPERATORS))
            )
        return normalized


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

    @field_serializer("operator")
    def serialize_operator(self, value: str) -> str:
        return REVERSE_OPERATOR_MAP.get(value, value)
