"""Monitor the monitor: HEALTHY / DEGRADED / DOWN plus the numbers behind it (§49)."""
from __future__ import annotations

import time
from collections import deque
from typing import Callable

from app.alerts.dispatcher import Dispatcher
from app.config import Settings
from app.detection.detector import DetectionEngine
from app.detection.state import AlertStateMachine
from app.ingestion.parser import IngestStats
from app.ingestion.tailer import Tailer
from app.storage.repository import Repository


class HealthMonitor:
    """Monitors the monitor: HEALTHY / DEGRADED / DOWN plus the numbers behind it."""
    def __init__(
        self,
        settings: Settings,
        clock: Callable[[], float],
        stats: IngestStats,
        queue,
        engine: DetectionEngine,
        tailer: Tailer,
        dispatcher: Dispatcher,
        machine: AlertStateMachine,
        repo: Repository,
        skipped_sinks: dict[str, str],
    ) -> None:
        self._s, self._clock = settings, clock
        self._stats, self._queue, self._engine, self._tailer = stats, queue, engine, tailer
        self._dispatcher, self._machine, self._repo = dispatcher, machine, repo
        self._skipped = skipped_sinks
        self._aws_checks: dict[str, str] = {}            # filled by the fail-soft startup check
        self.replay_status: Callable[[], dict] | None = None   # wired by the Runtime (dataset replay panel)
        self.events_processed = 0
        self.tail_lag_ms = 0.0
        self.detection_latency_ms = 0.0
        self._buckets: deque[list[float]] = deque()      # [whole second, count]

    # ---- recorded by the pipeline ----------------------------------------------------------------
    def record_event(self, event_ts: float) -> None:
        """Note a processed event (tail lag and throughput)."""
        now = self._clock()
        self.events_processed += 1
        self.tail_lag_ms = max(0.0, (now - event_ts) * 1000)   # how stale the event was when we got to it
        sec = int(now)
        if self._buckets and self._buckets[-1][0] == sec:
            self._buckets[-1][1] += 1
        else:
            self._buckets.append([sec, 1])

    def set_aws(self, name: str, status: str) -> None:
        """Result of the AWS startup check: name in {aws_identity, sns, cloudwatch}, status 'ok' or 'error: ...'."""
        self._aws_checks[name] = status

    def aws_report(self) -> dict:
        """State of the AWS startup check, for the dashboard."""
        s = self._s
        sinks = self._dispatcher.sinks
        if not s.aws_enabled:
            return {"enabled": False, "aws_identity": "off", "sns": "off", "cloudwatch": "off"}
        any_aws = any(n in sinks for n in ("sns", "cloudwatch"))
        out: dict = {"enabled": True,
                     "aws_identity": self._aws_checks.get("aws_identity", "checking..." if any_aws else "not configured")}
        for n in ("sns", "cloudwatch"):
            out[n] = self._aws_checks.get(n, "checking..." if n in sinks else "not configured")
        return out

    def record_tick(self, seconds: float) -> None:
        # exponential moving average so one slow tick doesn't dominate
        """Track detection latency per tick as a moving average."""
        self.detection_latency_ms = 0.8 * self.detection_latency_ms + 0.2 * seconds * 1000

    def events_per_second(self) -> float:
        """Recent processing throughput."""
        span = self._s.health.events_per_second_span_seconds
        now = self._clock()
        while self._buckets and self._buckets[0][0] < now - span:
            self._buckets.popleft()
        return sum(c for _, c in self._buckets) / span

    # ---- report ----------------------------------------------------------------------------------
    def report(self) -> dict:
        """The full health payload for /api/system/status and `health.update`."""
        h, st = self._s.health, self._stats
        now = self._clock()
        self._repo.ping()          # active check: DB trouble is noticed even if no write happened to fail this tick
        depth, cap = self._queue.qsize(), self._queue.maxsize
        file_status = "ok" if self._tailer.file_ok else "missing"

        sinks: dict[str, dict] = {}
        for name, s in self._dispatcher.stats.items():
            status = "idle" if s.last_ok is None else ("ok" if s.last_ok else "failing")
            sinks[name] = {"status": status, "success": s.success, "failure": s.failure, "last_error": s.last_error,
                           "last_ok_at": s.last_ok_at}
        for name, why in self._skipped.items():
            sinks[name] = {"status": "disabled", "success": 0, "failure": 0, "last_error": why}

        reasons: list[str] = []
        status = "HEALTHY"
        if file_status != "ok":
            status = "DOWN"
            reasons.append(f"log file missing: {self._s.log_path}")
        if self._repo.status != "ok":
            status = "DOWN"
            reasons.append("database not writable")
        if status != "DOWN":
            if cap and depth >= h.degraded_queue_fraction * cap:
                reasons.append(f"queue at {depth}/{cap}")
            failing = [n for n, s in sinks.items() if s["status"] == "failing"]
            if failing:
                reasons.append("sink failing: " + ", ".join(failing))
            if self.tail_lag_ms > h.degraded_tail_lag_seconds * 1000:
                reasons.append(f"tail lag {self.tail_lag_ms / 1000:.1f}s")
            if reasons:
                status = "DEGRADED"
        silent = self._engine.silent_services()
        if silent:
            reasons.append("silent: " + ", ".join(silent))     # informational; does not degrade
        aws = self.aws_report()
        bad = [f"{k} {v}" for k, v in aws.items() if k != "enabled" and str(v).startswith(("error", "unchecked"))]
        if bad:
            reasons.append("aws check failed (fail-soft, other channels unaffected): " + "; ".join(bad))   # informational

        return {
            "status": status,
            "reasons": reasons,
            "events_processed": self.events_processed,
            "events_per_second": round(self.events_per_second(), 1),
            "lines_read": st.lines_read,
            "queue_depth": depth,
            "queue_capacity": cap,
            "parse_errors": st.parse_errors,
            "parse_error_samples": list(st.parse_error_samples),
            "late_events": self._engine.late_events,
            "dropped_events": st.dropped_events,
            "tail_lag_ms": round(self.tail_lag_ms, 1),
            "detection_latency_ms": round(self.detection_latency_ms, 3),
            "active_alerts": len(self._machine.open),
            "sinks": sinks,
            "aws": aws,
            "replay": self.replay_status() if self.replay_status else {"running": False},
            "sink_success": sum(s["success"] for s in sinks.values()),
            "sink_failure": sum(s["failure"] for s in sinks.values()),
            "delivery_backlog": self._dispatcher.backlog,
            "db_status": self._repo.status,
            "file_status": file_status,
            "tail_offset": self._tailer.offset,
            "rotations": self._tailer.rotations,
            "resumed_from_checkpoint": self._tailer.resumed_from_checkpoint,
            "last_event_age_seconds": None if st.last_event_seen_at is None else round(now - st.last_event_seen_at, 1),
            "silent_services": silent,
            "server_ts": time.time(),
        }
