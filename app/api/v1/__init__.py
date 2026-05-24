from fastapi import APIRouter

from app.api.v1.system_metric import router as system_metric_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(system_metric_router)
