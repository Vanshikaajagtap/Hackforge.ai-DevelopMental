"""Alert state machine: NORMAL -> OPEN (escalations) -> RESOLVED -> NORMAL, per dedup_key = service:error_rate.

Pure logic - no I/O, no clock of its own (snapshots carry the time), so it is deterministic under test.

Notify only on: created, escalated (severity above the alert's peak), resolved. Everything else is suppressed.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable, Literal

from app.config import DetectorCfg, ProfileCfg

from .models import Alert, Snapshot
from .severity import rank

Kind = Literal["created", "escalated", "updated", "resolved"]


@dataclass
class Transition:
    kind: Kind
    alert: Alert
    notify: bool
    level_shift: bool = False
    new_peak: bool = False      # the alert's evidence was refreshed because the rate climbed to a new high


def dedup_key(service: str) -> str:
    return f"{service}:error_rate"


def build_evidence(snap: Snapshot, window_seconds: float) -> dict:
    """Explainable evidence for an anomalous snapshot (what / how much / why)."""
    ratio = snap.ratio or 0.0
    z = snap.z or 0.0
    return {
        "is_anomaly": snap.severity != "NONE",
        "service": snap.service,
        "current_error_rate": round(snap.error_rate, 4),
        "baseline_error_rate": round(snap.baseline_mean or 0.0, 4),
        "baseline_std": round(snap.baseline_std or 0.0, 4),
        "relative_change": round(ratio, 2),
        "z_score": round(z, 2),
        "sample_size": snap.total,
        "errors": snap.errors,
        "window_seconds": window_seconds,
        "reason": f"Error rate {ratio:.1f}× baseline with z={z:.2f} over {snap.total} events",
    }


class AlertStateMachine:
    def __init__(
        self,
        detector_cfg: DetectorCfg,
        profile_cfg: ProfileCfg,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._cfg = detector_cfg
        self._profile = profile_cfg
        self._new_id = id_factory or (lambda: uuid.uuid4().hex[:10])
        self.open: dict[str, Alert] = {}
        self._pending: dict[str, int] = {}
        self._calm: dict[str, int] = {}

    # ---- queries / restore -----------------------------------------------------------------------
    def is_open(self, service: str) -> bool:
        return dedup_key(service) in self.open

    def open_alerts(self) -> list[Alert]:
        return list(self.open.values())

    def restore(self, alerts: list[Alert]) -> None:
        for a in alerts:
            if a.status == "OPEN":
                self.open[a.dedup_key] = a

    # ---- evaluation ------------------------------------------------------------------------------
    def evaluate(self, snap: Snapshot) -> Transition | None:
        key = dedup_key(snap.service)
        alert = self.open.get(key)
        return self._evaluate_closed(key, snap) if alert is None else self._evaluate_open(key, alert, snap)

    def _evaluate_closed(self, key: str, snap: Snapshot) -> Transition | None:
        if snap.severity == "NONE":
            self._pending[key] = 0            # LOW_DATA / WARMUP / NORMAL all break a confirmation streak
            return None
        self._pending[key] = self._pending.get(key, 0) + 1
        if snap.severity != "CRITICAL" and self._pending[key] < self._cfg.confirm_ticks:
            return None
        self._pending[key] = 0
        self._calm[key] = 0
        ev = build_evidence(snap, self._profile.window_seconds)
        alert = Alert(
            id=self._new_id(), dedup_key=key, service=snap.service,
            severity=snap.severity, peak_severity=snap.severity, status="OPEN",
            created_at=snap.ts, resolved_at=None,
            current_rate=snap.error_rate, baseline_rate=snap.baseline_mean or 0.0,
            z=snap.z or 0.0, ratio=snap.ratio or 0.0, sample_size=snap.total, reason=ev["reason"],
        )
        self.open[key] = alert
        return Transition("created", alert, notify=True)

    def _evaluate_open(self, key: str, alert: Alert, snap: Snapshot) -> Transition | None:
        # Sustained level shift: the "incident" has become the new normal -> resolve and let the baseline relearn.
        if snap.ts - alert.created_at > self._profile.level_shift_seconds:
            return self._resolve(key, alert, snap, level_shift=True)

        if snap.z is None:
            return None                       # WARMUP / LOW_DATA: no information, hold the current state

        if rank(snap.severity) > rank(alert.peak_severity):
            ev = build_evidence(snap, self._profile.window_seconds)
            alert.severity = alert.peak_severity = snap.severity
            self._apply_evidence(alert, snap, ev["reason"])
            self._calm[key] = 0
            return Transition("escalated", alert, notify=True)

        # Quiet updates (never a notification): the alert's severity follows the current level while its peak stays,
        # and its evidence (rate / z / ratio / sample) tracks the worst point of the incident, so the resolve message
        # and the alert history report the real peak rather than the rate at the last escalation.
        transition = None
        if snap.severity != "NONE" and snap.severity != alert.severity:
            alert.severity = snap.severity
            transition = Transition("updated", alert, notify=False)
        if snap.error_rate > alert.current_rate:
            self._apply_evidence(alert, snap, build_evidence(snap, self._profile.window_seconds)["reason"])
            transition = Transition("updated", alert, notify=False, new_peak=True)

        # hysteresis: a stricter bar to resolve than to trigger, held for resolve_ticks in a row
        if snap.z < self._cfg.resolve_z and (snap.ratio or 0.0) < self._cfg.resolve_ratio:
            self._calm[key] = self._calm.get(key, 0) + 1
            if self._calm[key] >= self._cfg.resolve_ticks:
                return self._resolve(key, alert, snap, level_shift=False)
        else:
            self._calm[key] = 0
        return transition

    @staticmethod
    def _apply_evidence(alert: Alert, snap: Snapshot, reason: str) -> None:
        alert.current_rate = snap.error_rate
        alert.baseline_rate = snap.baseline_mean or alert.baseline_rate
        alert.z = snap.z or 0.0
        alert.ratio = snap.ratio or 0.0
        alert.sample_size = snap.total
        alert.reason = reason

    def _resolve(self, key: str, alert: Alert, snap: Snapshot, level_shift: bool) -> Transition:
        alert.status = "RESOLVED"
        alert.resolved_at = snap.ts
        if level_shift:
            alert.reason += (
                f" | sustained level shift: open > {self._profile.level_shift_seconds:g}s, "
                "resolved and baseline re-learning from the current level"
            )
        self.open.pop(key, None)
        self._pending[key] = 0
        self._calm[key] = 0
        return Transition("resolved", alert, notify=True, level_shift=level_shift)
