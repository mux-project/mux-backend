import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.dependencies import get_db
from app.schemas.node import (
    NodeCreate,
    NodeDeleteResponse,
    NodeDetailResponse,
    NodeHeartbeatResponse,
    NodeRegistrationResponse,
    NodeResponse,
    LatestMetricSnapshot,
)
from app.services.nodes import (
    deregister_node,
    get_node_with_metrics,
    get_nodes,
    heartbeat,
    register_node,
)

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("", response_model=list[NodeResponse])
async def list_nodes(db: AsyncSession = Depends(get_db)):
    return await get_nodes(db)


@router.post("/register", response_model=NodeRegistrationResponse)
async def register(
    payload: NodeCreate,
    db: AsyncSession = Depends(get_db),
):
    node, api_key = await register_node(db, payload)
    return NodeRegistrationResponse(node=node, api_key=api_key)


@router.get("/{node_id}", response_model=NodeDetailResponse)
async def get_node_detail(
    node_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = uuid.UUID(node_id)
    node, latest_metric = await get_node_with_metrics(db, nid)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return NodeDetailResponse(
        id=str(node.id),
        node_uuid=str(node.node_uuid),
        hostname=node.hostname,
        ip_address=node.ip_address,
        os_version=node.os_version,
        agent_version=node.agent_version,
        is_active=node.is_active,
        last_heartbeat_at=node.last_heartbeat_at,
        registered_at=node.registered_at,
        latest_metrics=LatestMetricSnapshot.model_validate(latest_metric)
        if latest_metric
        else None,
    )


@router.delete("/{node_id}", response_model=NodeDeleteResponse)
async def deregister(
    node_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = uuid.UUID(node_id)
    ok = await deregister_node(db, nid)
    if not ok:
        raise HTTPException(status_code=404, detail="Node not found")
    return NodeDeleteResponse()


@router.post("/{node_id}/heartbeat", response_model=NodeHeartbeatResponse)
async def node_heartbeat(
    node_id: str,
    db: AsyncSession = Depends(get_db),
):
    nid = uuid.UUID(node_id)
    ok = await heartbeat(db, nid)
    if not ok:
        raise HTTPException(status_code=404, detail="Node not found")
    return NodeHeartbeatResponse()
