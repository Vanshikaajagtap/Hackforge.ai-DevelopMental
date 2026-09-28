"""AlertManager: snapshots -> state machine -> persist FIRST -> enqueue deliveries -> push to the dashboard."""
from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, replace
from typing import Awaitable, Callable

from app.detection.models import Alert, Snapshot
from app.detection.state import AlertStateMachine, Transition, build_evidence
from app.storage.repository import Repository

from .dispatcher import Delivery, Dispatcher

log = logging.getLogger("logpulse.alerts")

Broadcast = Callable[[str, dict], Awaitable[None]]


class AlertManager:
    """Turns snapshots into alerts: state machine -> persist first -> enqueue deliveries -> dashboard push."""
    def __init__(
        self,
        machine: AlertStateMachine,
        repo: Repository,
        dispatcher: Dispatcher,
        broadcast: Broadcast,
        window_seconds: float,
        on_level_shift: Callable[[str], None] | None = None,
        channel_permit: Callable[[str], bool] | None = None,
    ) -> None:
        self._permit = channel_permit      # e.g. ReplayGuard.permit: False -> no delivery row / send for that channel
        self.machine = machine
        self.repo = repo
        self.dispatcher = dispatcher
        self._broadcast = broadcast
        self._window = window_seconds
        self._on_level_shift = on_level_shift
        self._evidence: dict[str, dict] = {}   # alert_id -> latest evidence (for the dashboard)

    # ---- startup ---------------------------------------------------------------------------------
    def restore(self) -> None:
        """Reload OPEN alerts (no duplicate 'created' after a restart) and re-enqueue undelivered notifications."""
        self.machine.restore(self.repo.open_alerts())
        for row in self.repo.pending_deliveries():
            alert = self.repo.get_alert(row["alert_id"])
            if alert is None:
                continue
            self.dispatcher.enqueue(Delivery(row["id"], alert, row["event"], row["channel"]))

    # ---- per tick --------------------------------------------------------------------------------
    async def process(self, snapshots: list[Snapshot]) -> None:
        """Evaluate this tick's snapshots and handle every alert transition they cause."""
        for snap in snapshots:
            t = self.machine.evaluate(snap)
            if t is not None:
                await self._handle(t, snap)

    async def send_test(self, now: float) -> Alert:
        """Demo 'Send test alert': a clearly-labelled fake alert that goes through the REAL persist -> dispatcher -> sinks
        path (so SNS / CloudWatch / ntfy / Telegram / JSONL wiring can be verified in seconds). It is stored as RESOLVED so
        it can never be reloaded as an open incident after a restart or counted as an active alert."""
        alert = Alert(
            id="test-" + uuid.uuid4().hex[:8], dedup_key="test-service:error_rate", service="test-service",
            severity="CRITICAL", peak_severity="CRITICAL", status="RESOLVED", created_at=now, resolved_at=None,
            current_rate=0.384, baseline_rate=0.051, z=5.18, ratio=7.5, sample_size=284,
            reason="TEST ALERT - sent from the dashboard, not a real incident",
        )
        await self._handle(Transition("created", alert, notify=True))
        return alert

    async def _handle(self, t: Transition, snap: Snapshot | None = None) -> None:
        alert = t.alert
        if snap is not None and (t.kind in {"created", "escalated"} or t.new_peak):
            self._evidence[alert.id] = build_evidence(snap, self._window)
        self.repo.upsert_alert(alert)                                    # 1) persist the alert
        if t.level_shift and self._on_level_shift is not None:
            self._on_level_shift(alert.service)
        if t.notify:
            for channel in self.dispatcher.sinks:                       # 2) persist PENDING delivery rows
                if self._permit is not None and not self._permit(channel):
                    continue          # e.g. AWS is paused while a dataset replay runs; other channels are unaffected
                delivery_id = self.repo.add_delivery(alert.id, t.kind, channel)
                self.dispatcher.enqueue(Delivery(delivery_id, replace(alert), t.kind, channel))   # 3) only then send
        msg = {"created": "alert.created", "resolved": "alert.resolved"}.get(t.kind, "alert.updated")
        await self._broadcast(msg, self.alert_view(alert))
        if t.kind == "resolved":
            self._evidence.pop(alert.id, None)

    # ---- views -----------------------------------------------------------------------------------
    def alert_view(self, alert: Alert) -> dict:
        """Alert + delivery badges (+ evidence while open) for WebSocket / REST."""
        d = asdict(alert)
        d["deliveries"] = self.repo.deliveries_for(alert.id)
        ev = self._evidence.get(alert.id)
        if ev:
            d["evidence"] = ev
        return d

    async def publish_update(self, alert_id: str) -> None:
        """Delivery status changed -> tell the dashboard so its badges move."""
        alert = next((a for a in self.machine.open.values() if a.id == alert_id), None) or self.repo.get_alert(alert_id)
        if alert is not None:
            await self._broadcast("alert.updated", self.alert_view(alert))
