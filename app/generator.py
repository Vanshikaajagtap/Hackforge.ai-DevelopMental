"""Demo log generator (§43). Appends NDJSON lines to the real log file; nothing is faked downstream.

Used two ways: in-process by the dashboard demo buttons, and standalone via scripts/generate_logs.py.
Error placement uses error diffusion (a running debt plus tiny noise), so "5 % errors" really is ~5 % in every
window - keeps the demo reproducible instead of at the mercy of dice. Seed the RNG for identical runs.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Callable

from app.config import GeneratorCfg

SCENARIOS = ("normal", "traffic_spike", "error_spike", "recover", "mixed", "malformed")

_OK_MSGS = ["request completed", "cache hit", "token issued", "payment authorised"]
_ERR_MSGS = ["upstream timeout", "db connection refused", "internal error", "payment gateway timeout"]
_BROKEN = [
    '{"timestamp":"2026-09-28T12:10:33.127Z","service":"payment-serv',       # truncated write
    "this is not json at all",
    '{"timestamp":"not-a-time","service":"auth-service","level":"INFO"}',      # bad timestamp
    '{"service":"auth-service","level":"ERROR","message":"no timestamp"}',     # missing field
    "{}",
]


class ScenarioController:
    """Holds the requested scenario and when it started; resolves `mixed` into its scripted timeline."""

    def __init__(self, cfg: GeneratorCfg, clock: Callable[[], float] = time.time) -> None:
        self._cfg = cfg
        self._clock = clock
        self.name = "normal"
        self.started_at = clock()
        self.paused = False              # True while a dataset replay owns the log file

    def set(self, name: str) -> None:
        """Select a scenario ('recover' is simply normal traffic again)."""
        if name not in SCENARIOS:
            raise ValueError(f"unknown scenario {name!r}; expected one of {SCENARIOS}")
        self.name = name
        self.started_at = self._clock()

    def elapsed(self) -> float:
        """Seconds since the current scenario started."""
        return self._clock() - self.started_at

    def effective(self) -> tuple[str, float]:
        """(concrete scenario, seconds since it began). `recover` is normal traffic - the detector does the recovering."""
        elapsed = self.elapsed()
        name = self.name
        if name == "mixed":
            start, name = 0.0, "normal"
            for at, scn in self._cfg.mixed_timeline:
                if elapsed >= at:
                    start, name = float(at), scn
            elapsed -= start
        return ("normal" if name == "recover" else name), elapsed

    @property
    def timeline_end(self) -> float:
        """When the scripted `mixed` timeline ends, in seconds."""
        return float(self._cfg.mixed_timeline[-1][0])


class LogGenerator:
    """Produces reproducible NDJSON traffic for the demo scenarios."""
    def __init__(self, cfg: GeneratorCfg, controller: ScenarioController, seed: int | None = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.cfg = cfg
        self.controller = controller
        self._clock = clock
        self._rng = random.Random(seed)
        self._debt = {s: self._rng.random() for s in cfg.services}     # error-diffusion state
        self._carry: dict[str, float] = {}                             # fractional events between slices
        self._phase = {s: self._rng.uniform(0, 6.28) for s in cfg.services}
        self._lines_emitted = 0

    def params(self, service: str, scenario: str, elapsed: float, now: float) -> tuple[float, float]:
        """(events per second, error rate) for a service in a concrete scenario."""
        c = self.cfg
        rate = c.services[service]
        err = c.normal_error_rate + c.normal_error_drift * math.sin(now / 9.0 + self._phase[service])
        if scenario == "traffic_spike":
            rate *= c.traffic_spike_multiplier                          # volume changes, error RATE does not
        elif scenario == "error_spike" and service == c.spike_service:
            if elapsed >= c.error_spike_escalate_after_seconds:
                err = c.critical_spike_rate
            elif elapsed < c.error_spike_ramp_seconds:
                f = elapsed / c.error_spike_ramp_seconds
                err = c.error_spike_start_rate + f * (c.error_spike_rate - c.error_spike_start_rate)
            else:
                err = c.error_spike_rate
        return rate, err

    def _make_line(self, service: str, err_rate: float, ts: float) -> str:
        self._debt[service] += err_rate + self._rng.uniform(-0.02, 0.02)
        is_error = self._debt[service] >= 1.0
        if is_error:
            self._debt[service] -= 1.0
            level, status, msg = "ERROR", self._rng.choice([500, 502, 503]), self._rng.choice(_ERR_MSGS)
        else:
            level, status, msg = "INFO", self._rng.choice([200, 200, 200, 201, 204]), self._rng.choice(_OK_MSGS)
        return json.dumps({
            "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "service": service, "level": level, "status": status,
            "message": msg, "request_id": "req-" + uuid.UUID(int=self._rng.getrandbits(128)).hex[:8],
        })

    def batch(self, ts: float, dt: float) -> list[str]:
        """Lines for a time slice of dt seconds; fractional events carry over to the next slice."""
        scenario, elapsed = self.controller.effective()
        lines: list[str] = []
        for service in self.cfg.services:
            rate, err = self.params(service, scenario, elapsed, ts)
            self._carry[service] = self._carry.get(service, 0.0) + rate * dt
            n = int(self._carry[service])
            self._carry[service] -= n
            lines.extend(self._make_line(service, err, ts) for _ in range(n))
        self._rng.shuffle(lines)
        if self.controller.name == "malformed":
            out: list[str] = []
            for line in lines:
                self._lines_emitted += 1
                out.append(line)
                if self._lines_emitted % self.cfg.malformed_every == 0:
                    out.append(self._rng.choice(_BROKEN))
            lines = out
        return lines

    async def run(self, path: str, step: float = 0.1, stop: asyncio.Event | None = None) -> None:
        """Append generated lines to the log file until stopped."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        last = self._clock()
        while stop is None or not stop.is_set():
            await asyncio.sleep(step)
            now = self._clock()
            if self.controller.paused:
                last = now
                continue
            lines = self.batch(now, now - last)
            last = now
            if lines:
                with open(path, "a", encoding="utf-8", newline="\n") as fh:   # open/close per batch = flushed
                    fh.write("\n".join(lines) + "\n")
