from fastapi import APIRouter

from .hello import router as hello_router

api_router = APIRouter(prefix="/api")
api_router.include_router(hello_router)
