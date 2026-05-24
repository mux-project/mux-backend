from pydantic import BaseModel, ConfigDict


class SystemMetricCreate(BaseModel):
    cpu_percent: float
    memory_percent: float
    disk_percent: float


class SystemMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    cpu_percent: float
    memory_percent: float
    disk_percent: float
