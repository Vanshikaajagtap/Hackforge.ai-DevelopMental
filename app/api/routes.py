"""REST and WebSocket routes (the contract is in docs/PRD.md section 41)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, WebSocket
from pydantic import BaseModel

from app.generator import SCENARIOS
from app.replay import ReplayError

router = APIRouter()


class ScenarioIn(BaseModel):
    """Body of POST /api/demo/scenario."""
    name: str


class ReplayIn(BaseModel):
    """Body of POST /api/demo/replay."""
    action: str = "start"                 # start | stop
    preset: str | None = None
    speed: float | None = None
    aws: bool = False                     # ask for SNS/CloudWatch during this run (honoured only for replay.aws_preset)
    start: str | None = None              # custom window instead of a preset
    end: str | None = None
    loop: bool = False
    seed: int | None = None


@router.get("/health")
async def health() -> dict:
    """Liveness probe."""
    return {"status": "ok"}


@router.get("/api/metrics/current")
async def metrics_current(request: Request) -> list[dict]:
    """Latest snapshot per service."""
    return request.app.state.rt.current_metrics()


@router.get("/api/metrics/history")
async def metrics_history(request: Request, service: str, minutes: float = 5) -> list[dict]:
    """Stored snapshots of one service over the last `minutes`."""
    rt = request.app.state.rt
    return rt.repo.history(service, rt.clock() - minutes * 60)


@router.get("/api/alerts")
async def alerts(request: Request, status: str | None = None, limit: int = 50) -> list[dict]:
    """Recent alerts with their delivery badges, optionally filtered by status."""
    rt = request.app.state.rt
    if status is not None and status.upper() not in {"OPEN", "RESOLVED"}:
        raise HTTPException(422, "status must be OPEN or RESOLVED")
    return [rt.manager.alert_view(a) for a in rt.repo.list_alerts(min(max(limit, 1), 500), status and status.upper())]


@router.get("/api/alerts/{alert_id}")
async def alert_detail(alert_id: str, request: Request) -> dict:
    """One alert with its deliveries."""
    rt = request.app.state.rt
    alert = next((a for a in rt.machine.open.values() if a.id == alert_id), None) or rt.repo.get_alert(alert_id)
    if alert is None:
        raise HTTPException(404, "alert not found")
    return rt.manager.alert_view(alert)


@router.get("/api/system/status")
async def system_status(request: Request) -> dict:
    """Detector health report: queue, lag, sinks, AWS checks, replay."""
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
    """Switch the synthetic generator's scenario (demo mode only)."""
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


@router.get("/api/demo/replay")
async def replay_status(request: Request) -> dict:
    """Replay progress, AWS-guard status and the available presets (demo mode only)."""
    rt = request.app.state.rt
    if not rt.settings.demo_mode:
        raise HTTPException(404, "demo mode is off")
    return {**rt.replay.status(), "presets": rt.replay.presets()}


@router.post("/api/demo/replay")
async def replay_control(body: ReplayIn, request: Request) -> dict:
    """Start / stop replaying the real NASA log into the file the tailer watches (demo mode only)."""
    rt = request.app.state.rt
    if not rt.settings.demo_mode:
        raise HTTPException(404, "demo mode is off")
    if body.action == "stop":
        return await rt.replay.stop()
    if body.action != "start":
        raise HTTPException(422, "action must be start or stop")
    try:
        return await rt.replay.start(body.preset, body.speed, body.aws, body.start, body.end, body.loop, body.seed)
    except ReplayError as e:
        raise HTTPException(e.status, str(e)) from e


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    """WebSocket feed: `hello` on connect, then metric / alert / health updates."""
    await ws.app.state.rt.hub.handle(ws)
