from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, WebSocket
from pydantic import BaseModel

from app.generator import SCENARIOS

router = APIRouter()


class ScenarioIn(BaseModel):
    name: str


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/api/metrics/current")
async def metrics_current(request: Request) -> list[dict]:
    return request.app.state.rt.current_metrics()


@router.get("/api/metrics/history")
async def metrics_history(request: Request, service: str, minutes: float = 5) -> list[dict]:
    rt = request.app.state.rt
    return rt.repo.history(service, rt.clock() - minutes * 60)


@router.get("/api/alerts")
async def alerts(request: Request, status: str | None = None, limit: int = 50) -> list[dict]:
    rt = request.app.state.rt
    if status is not None and status.upper() not in {"OPEN", "RESOLVED"}:
        raise HTTPException(422, "status must be OPEN or RESOLVED")
    return [rt.manager.alert_view(a) for a in rt.repo.list_alerts(min(max(limit, 1), 500), status and status.upper())]


@router.get("/api/alerts/{alert_id}")
async def alert_detail(alert_id: str, request: Request) -> dict:
    rt = request.app.state.rt
    alert = next((a for a in rt.machine.open.values() if a.id == alert_id), None) or rt.repo.get_alert(alert_id)
    if alert is None:
        raise HTTPException(404, "alert not found")
    return rt.manager.alert_view(alert)


@router.get("/api/system/status")
async def system_status(request: Request) -> dict:
    rt = request.app.state.rt
    return {
        **rt.health.report(),
        "profile": rt.settings.profile_name,
        "demo_mode": rt.settings.demo_mode,
        "log_path": rt.settings.log_path,
        "ws_clients": rt.hub.client_count,
    }


@router.post("/api/demo/scenario")
async def demo_scenario(body: ScenarioIn, request: Request) -> dict:
    rt = request.app.state.rt
    if not rt.settings.demo_mode:
        raise HTTPException(404, "demo mode is off")
    if body.name not in SCENARIOS:
        raise HTTPException(422, f"unknown scenario; expected one of {list(SCENARIOS)}")
    rt.scenario.set(body.name)
    return {"scenario": rt.scenario.name}


@router.post("/api/demo/test-alert")
async def demo_test_alert(request: Request) -> dict:
    """Send a labelled fake alert through the real dispatcher and every configured sink (verifies the wiring)."""
    rt = request.app.state.rt
    if not rt.settings.demo_mode:
        raise HTTPException(404, "demo mode is off")
    return await rt.send_test_alert()


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.app.state.rt.hub.handle(ws)
