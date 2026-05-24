from fastapi import FastAPI

from app.api import api_router


def create_app() -> FastAPI:
    app = FastAPI(title="mux")
    app.include_router(api_router)
    return app

# Instantiate it here so Uvicorn can find it
app = create_app()