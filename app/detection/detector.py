"""Per-service detection: sliding window -> baseline -> gates -> severity. Emits one Snapshot per tick.

The alert lifecycle (confirm ticks, hysteresis, dedup, level shift) lives in state.py and consumes these snapshots.
"""
from __future__ import annotations

from typing import Callable, Iterable

from app.config import Settings
from app.ingestion.models import LogEvent

from .baseline import Baseline
from .models import Snapshot
from .severity import classify
from .window import SlidingWindow

Clock = Callable[[], float]


class ServiceDetector:
    """Sliding window + baseline + severity gates for one service; emits one Snapshot per tick."""
    def __init__(self, service: str, settings: Settings, clock: Clock) -> None:
        self.service = service
        self._p = settings.profile
        self._d = settings.detector
        self._sev = settings.severity
        self._silence_seconds = settings.health.silence_windows * settings.profile.window_seconds
        self._clock = clock
        self.window = SlidingWindow(self._p.window_seconds)
        self.baseline = Baseline(
            self._p.baseline_max_samples, self._p.min_baseline_samples,
            self._d.std_floor, self._d.ratio_floor,
        )
        self.frozen = False           # True while an alert for this service is OPEN: no baseline admission
        self.late_events = 0
        self.last_event_at: float | None = None   # clock time of the last accepted event
        self._last_sample_at = float("-inf")
        self._waive_ceiling = False   # one-shot: level-shift adoption relearns even from a high rate

    # ---- input -----------------------------------------------------------------------------------
    def on_event(self, ev: LogEvent) -> bool:
        """Returns False (and counts it) if the event is already older than the window."""
        now = self._clock()
        if ev.ts < now - self._p.window_seconds:
            self.late_events += 1
            return False
        self.window.add(ev.ts, ev.is_error, now)
        self.last_event_at = now
        return True

    # ---- baseline management ---------------------------------------------------------------------
    def restore_samples(self, rates: Iterable[float]) -> None:
        """Seed the baseline from stored history after a restart."""
        self.baseline.load(rates)

    def reset_baseline(self, waive_ceiling: bool = True) -> None:
        """Level shift: forget the old normal and relearn from the current level."""
        self.baseline.clear()
        self._waive_ceiling = waive_ceiling
        self._last_sample_at = float("-inf")

    # ---- tick ------------------------------------------------------------------------------------
    def tick(self) -> Snapshot:
        """Evict by the clock, evaluate against the baseline, maybe sample the baseline, return a Snapshot."""
        now = self._clock()
        self.window.evict(now)  # a silent service must decay to LOW_DATA, not freeze on stale numbers
        total, errors, rate = self.window.total, self.window.errors, self.window.error_rate
        b = self.baseline

        z = ratio = None
        severity = "NONE"
        if not b.ready:
            state = "WARMUP"
        elif total < self._p.min_events:
            state = "LOW_DATA"
        else:
            z, ratio = b.z(rate), b.ratio(rate)
            severity = classify(z, rate, ratio, errors, self._sev)
            state = "ANOMALY" if severity != "NONE" else "NORMAL"

        self._maybe_sample(now, total, rate, state)
        return Snapshot(
            ts=now, service=self.service, total=total, errors=errors, error_rate=rate,
            baseline_mean=b.mean, baseline_std=b.std, z=z, ratio=ratio,
            state=state, severity=severity,
        )

    def _maybe_sample(self, now: float, total: int, rate: float, state: str) -> None:
        """Feed the baseline on a fixed cadence, only from healthy, well-populated windows."""
        # Contamination guard, part 1: while an alert is open the service is (or just was) in an incident, so its
        # windows must not become "normal"; and a window with too few events says nothing about the rate.
        if self.frozen or total < self._p.min_events:
            return
        # Fixed cadence, not per event: consecutive windows overlap almost completely, so per-event samples are
        # near-identical, sigma collapses toward zero and any wobble would look like a huge z-score.
        if now - self._last_sample_at < self._p.baseline_sample_every:
            return
        if state == "WARMUP":
            # an incident during startup must not become "normal" (unless we are explicitly adopting a level shift)
            admit = rate <= self._d.warmup_ceiling or self._waive_ceiling
        else:
            admit = state == "NORMAL"                # anomalous windows never enter the baseline
        if admit:
            self.baseline.add(rate)
            self._last_sample_at = now
            if self.baseline.ready:
                self._waive_ceiling = False

    # ---- silence ---------------------------------------------------------------------------------
    @property
    def silent(self) -> bool:
        """An established service with an empty window that hasn't logged for N windows (dashboard-only signal)."""
        if not self.baseline.ready or self.window.total > 0:
            return False
        return self.last_event_at is not None and self._clock() - self.last_event_at > self._silence_seconds


class DetectionEngine:
    """Routes events to per-service detectors and ticks them together."""
    def __init__(self, settings: Settings, clock: Clock, frozen_fn: Callable[[str], bool] | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self._frozen_fn = frozen_fn
        self.services: dict[str, ServiceDetector] = {}

    def detector(self, service: str) -> ServiceDetector:
        """The detector for `service`, created on first use."""
        det = self.services.get(service)
        if det is None:
            det = self.services[service] = ServiceDetector(service, self._settings, self._clock)
        return det

    def on_event(self, ev: LogEvent) -> bool:
        """Route an event; False (counted as late) if it was already older than the window."""
        return self.detector(ev.service).on_event(ev)

    def tick(self) -> list[Snapshot]:
        """Tick every service, first refreshing its baseline-freeze flag from open alerts."""
        snaps = []
        for det in self.services.values():
            if self._frozen_fn is not None:
                det.frozen = self._frozen_fn(det.service)
            snaps.append(det.tick())
        return snaps

    @property
    def late_events(self) -> int:
        """Total events dropped for being older than the window."""
        return sum(d.late_events for d in self.services.values())

    def silent_services(self) -> list[str]:
        """Established services that have logged nothing for several windows."""
        return sorted(d.service for d in self.services.values() if d.silent)
