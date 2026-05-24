from fastapi import APIRouter

from .system_metric import router as system_metric_router

api_router = APIRouter(prefix="/api")
api_router.include_router(system_metric_router)