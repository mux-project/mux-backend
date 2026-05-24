from fastapi import APIRouter

from app.schemas.hello import HelloResponse
from app.services.hello import HelloService

router = APIRouter(tags=["hello"])


@router.get("/hello", response_model=HelloResponse)
async def get_hello():
    message = HelloService.get_greeting()
    return HelloResponse(message=message)
