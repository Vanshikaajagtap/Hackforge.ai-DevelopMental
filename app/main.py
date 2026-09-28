"""Wiring: tailer -> parser -> bounded queue -> detection engine -> state machine -> manager
(SQLite first, then dispatcher -> sinks) -> WebSocket hub -> dashboard."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import asdict
from typing import Callable

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.alerts import build_sinks
from app.alerts.aws_common import aws_startup_check
from app.alerts.base import AlertSink
from app.alerts.dispatcher import Dispatcher
from app.alerts.manager import AlertManager
from app.api.routes import router
from app.api.websocket import Hub
from app.config import ROOT, Settings, load_settings
from app.detection.detector import DetectionEngine
from app.detection.models import Snapshot
from app.detection.state import AlertStateMachine
from app.generator import LogGenerator, ScenarioController
from app.health import HealthMonitor
from app.ingestion.models import LogEvent
from app.ingestion.parser import IngestStats, ParseError, parse_line
from app.ingestion.tailer import Checkpoint, Tailer
from app.storage.db import Database
from app.storage.repository import Repository

if not logging.getLogger().handlers:   # uvicorn only configures its own loggers
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("logpulse")
FRONTEND = ROOT / "frontend"


class Runtime:
    def __init__(
        self,
        settings: Settings,
        clock: Callable[[], float] = time.time,
        sinks: dict[str, AlertSink] | None = None,
    ) -> None:
        self.settings, self.clock = settings, clock
        s = settings

        self.repo = Repository(Database(s.db_path))
        self.machine = AlertStateMachine(s.detector, s.profile)
        self.engine = DetectionEngine(s, clock, frozen_fn=self.machine.is_open)
        self.stats = IngestStats()
        self.queue: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=s.ingestion.queue_max)
        self.latest: dict[str, Snapshot] = {}
        self.scenario = ScenarioController(s.generator, clock)
        self.hub = Hub(self.hello)

        cp = self.repo.load_checkpoint(s.log_path)
        self.tailer = Tailer(s.log_path, s.ingestion.start_at, s.ingestion.poll_seconds,
                             resume=Checkpoint(*cp) if cp else None)

        if sinks is None:
            sinks, skipped = build_sinks(s)
        else:
            skipped = {}
        self.dispatcher = Dispatcher(
            sinks, self.repo, s.alerts.retry_attempts, s.alerts.retry_backoff_seconds, s.alerts.sink_timeout_seconds,
            on_update=self._publish_update, clock=clock)
        self.manager = AlertManager(
            self.machine, self.repo, self.dispatcher, self.hub.broadcast, s.profile.window_seconds,
            on_level_shift=lambda svc: self.engine.detector(svc).reset_baseline())
        self.health = HealthMonitor(s, clock, self.stats, self.queue, self.engine, self.tailer,
                                    self.dispatcher, self.machine, self.repo, skipped)
        self._tasks: list[asyncio.Task] = []
        self._gen_stop = asyncio.Event()
        self._restore()

    # ---- restart recovery ------------------------------------------------------------------------
    def _restore(self) -> None:
        s = self.settings
        since = self.clock() - s.storage.snapshot_retention_hours * 3600
        for service in self.repo.services():
            samples = self.repo.baseline_samples(
                service, s.profile.baseline_sample_every, s.profile.baseline_max_samples, since,
                s.profile.min_events, s.detector.warmup_ceiling)
            if samples:
                self.engine.detector(service).restore_samples(samples)
        self.manager.restore()
        log.info("restored: %d services, %d open alerts, checkpoint=%s",
                 len(self.engine.services), len(self.machine.open), self.tailer.resume)

    # ---- views -----------------------------------------------------------------------------------
    def current_metrics(self) -> list[dict]:
        return [asdict(x) for x in self.latest.values()]

    def hello(self) -> dict:
        s = self.settings
        since = self.clock() - s.storage.hello_history_minutes * 60
        services = sorted(set(self.latest) | set(self.repo.services()))
        return {
            "config": {
                "profile": s.profile_name,
                "window_seconds": s.profile.window_seconds,
                "tick_seconds": s.profile.tick_seconds,
                "min_events": s.profile.min_events,
                "demo_mode": s.demo_mode,
            },
            "scenario": self.scenario.name,
            "services": services,
            "snapshots": self.current_metrics(),
            "history": {svc: self.repo.history(svc, since) for svc in services},
            "alerts": [self.manager.alert_view(a) for a in self.repo.list_alerts(50)],
            "health": self.health.report(),
        }

    async def send_test_alert(self) -> dict:
        alert = await self.manager.send_test(self.clock())
        return {"queued": True, "alert_id": alert.id, "sinks": list(self.dispatcher.sinks)}

    async def _publish_update(self, alert_id: str) -> None:
        await self.manager.publish_update(alert_id)

    # ---- pipeline stages -------------------------------------------------------------------------
    async def ingest_line(self, line: str) -> None:
        self.stats.lines_read += 1
        try:
            ev = parse_line(line)
        except ParseError as e:
            self.stats.record_error(line, str(e))     # counted, sampled, skipped - never fatal
            return
        self.stats.events_parsed += 1
        self.stats.last_event_ts = ev.ts
        self.stats.last_event_seen_at = self.clock()
        if self.queue.full():                         # backpressure: drop the oldest, count it
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.stats.dropped_events += 1
        self.queue.put_nowait(ev)

    async def _consume(self) -> None:
        while True:
            ev = await self.queue.get()
            try:
                self.engine.on_event(ev)
                self.health.record_event(ev.ts)
            except Exception:  # noqa: BLE001
                log.exception("detection failed on event")

    async def _tick_once(self) -> None:
        t0 = time.perf_counter()
        snaps = self.engine.tick()
        for snap in snaps:
            self.latest[snap.service] = snap
        self.repo.save_snapshots(snaps)
        await self.manager.process(snaps)             # alerts persisted before any delivery is attempted
        self.health.record_tick(time.perf_counter() - t0)
        for snap in snaps:
            await self.hub.broadcast("metric.update", asdict(snap))
        await self.hub.broadcast("health.update", self.health.report())

    async def _tick_loop(self) -> None:
        s = self.settings
        last_prune = self.clock()
        while True:
            await asyncio.sleep(s.profile.tick_seconds)
            try:
                await self._tick_once()
                if self.clock() - last_prune >= s.storage.prune_every_seconds:
                    last_prune = self.clock()
                    self.repo.prune_snapshots(self.clock() - s.storage.snapshot_retention_hours * 3600)
            except Exception:  # noqa: BLE001 - the tick loop must survive anything
                log.exception("tick failed")

    def save_checkpoint(self) -> None:
        if self.tailer.inode is not None:
            self.repo.save_checkpoint(self.settings.log_path, self.tailer.inode, self.tailer.offset, self.clock())

    async def _checkpoint_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.ingestion.checkpoint_every_seconds)
            self.save_checkpoint()

    # ---- lifecycle -------------------------------------------------------------------------------
    def start(self) -> None:
        coros = [
            self.tailer.run(self.ingest_line), self._consume(), self._tick_loop(),
            self.dispatcher.run(), self._checkpoint_loop(),
        ]
        if any(n in self.dispatcher.sinks for n in ("sns", "cloudwatch")):
            # fail-soft: runs in the background, reports to the health panel, can never block or crash startup
            coros.append(aws_startup_check(self.dispatcher.sinks, self.health.set_aws))
        if self.settings.demo_mode:
            gen = LogGenerator(self.settings.generator, self.scenario)
            coros.append(gen.run(self.settings.log_path, stop=self._gen_stop))
        self._tasks = [asyncio.create_task(c) for c in coros]

    async def stop(self) -> None:
        self._gen_stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self.save_checkpoint()                        # shutdown checkpoint
        self.repo.db.close()


def create_app(settings: Settings | None = None, sinks: dict[str, AlertSink] | None = None) -> FastAPI:
    settings = settings or load_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        rt = Runtime(settings, sinks=sinks)
        app.state.rt = rt
        rt.start()
        log.info("LogPulse up: profile=%s demo=%s log=%s sinks=%s", settings.profile_name, settings.demo_mode,
                 settings.log_path, list(rt.dispatcher.sinks))
        try:
            yield
        finally:
            await rt.stop()

    app = FastAPI(title="LogPulse", lifespan=lifespan)
    app.include_router(router)
    app.mount("/static", StaticFiles(directory=FRONTEND / "static"), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(FRONTEND / "index.html")

    return app


app = create_app()
