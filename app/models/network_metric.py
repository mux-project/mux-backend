from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database.base import Base

import uuid


class NetworkMetric(Base):
    __tablename__ = "network_metrics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    node_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("nodes.id"), nullable=False
    )
    interface: Mapped[str] = mapped_column(String(64), nullable=False)
    bytes_sent: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_recv: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    packets_sent: Mapped[int | None] = mapped_column(BigInteger)
    packets_recv: Mapped[int | None] = mapped_column(BigInteger)
    errors_in: Mapped[int | None] = mapped_column(Integer, default=0)
    errors_out: Mapped[int | None] = mapped_column(Integer, default=0)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
