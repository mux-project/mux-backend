from fastapi import APIRouter

from app.api.v1.metrics import router as metrics_router
from app.api.v1.nodes import router as nodes_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(metrics_router)
v1_router.include_router(nodes_router)
