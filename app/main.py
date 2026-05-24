import asyncio
import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI

from app.api import api_router
from app.database.session import async_session_factory, engine
from app.services.retention import run_retention_cleanup

logger = logging.getLogger("mux")


async def retention_scheduler() -> None:
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        await asyncio.sleep((next_run - now).total_seconds())

        async with async_session_factory() as db:
            try:
                result = await run_retention_cleanup(db)
                logger.info("retention_cleanup completed", extra=result)
            except Exception:
                logger.exception("retention_cleanup failed")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    task = asyncio.create_task(retention_scheduler())
    yield
    task.cancel()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="mux", lifespan=lifespan)
    app.include_router(api_router)
    return app
