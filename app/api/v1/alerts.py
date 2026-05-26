import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.dependencies import get_db
from app.schemas.alert_history import AlertAckResponse, AlertHistoryResponse
from app.schemas.alert_rule import AlertRuleCreate, AlertRuleResponse, AlertRuleUpdate
from app.services.alerts import (
    acknowledge_alert,
    create_alert_rule,
    delete_alert_rule,
    get_alert_rule,
    list_alert_history,
    list_alert_rules,
    update_alert_rule,
)

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/rules")
async def get_rules(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await list_alert_rules(db, page, page_size)


@router.post("/rules", response_model=AlertRuleResponse, status_code=201)
async def create_rule(
    payload: AlertRuleCreate,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await create_alert_rule(db, payload)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.get("/rules/{rule_id}", response_model=AlertRuleResponse)
async def get_rule(
    rule_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    rule = await get_alert_rule(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return rule


@router.put("/rules/{rule_id}", response_model=AlertRuleResponse)
async def update_rule(
    rule_id: uuid.UUID,
    payload: AlertRuleUpdate,
    db: AsyncSession = Depends(get_db),
):
    try:
        rule = await update_alert_rule(db, rule_id, payload)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    if not rule:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return rule


@router.delete("/rules/{rule_id}")
async def delete_rule(
    rule_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    ok = await delete_alert_rule(db, rule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    return {"status": "deleted"}


@router.get("/history")
async def get_history(
    status: str | None = Query(None),
    node_uuid: uuid.UUID | None = Query(None),
    rule_id: uuid.UUID | None = Query(None),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await list_alert_history(
            db, status, node_uuid, start, end, page, page_size, rule_id
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.patch("/history/{history_id}/ack", response_model=AlertAckResponse)
async def acknowledge(
    history_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    history = await acknowledge_alert(db, history_id)
    if not history:
        raise HTTPException(status_code=404, detail="Alert history not found")
    return AlertAckResponse()
