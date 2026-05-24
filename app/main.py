from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI

from app.api import api_router
from app.database.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="mux", lifespan=lifespan)
    app.include_router(api_router)
    return app
