from sqlalchemy import Float
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base, TimestampMixin


class SystemMetric(Base, TimestampMixin):
    __tablename__ = "system_metrics"

    id: Mapped[int] = mapped_column(primary_key=True)

    cpu_percent: Mapped[float] = mapped_column(Float)

    memory_percent: Mapped[float] = mapped_column(Float)

    disk_percent: Mapped[float] = mapped_column(Float)
