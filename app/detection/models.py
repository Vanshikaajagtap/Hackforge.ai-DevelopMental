"""Data contracts: Snapshot (one service, one tick) and Alert (an incident with its evidence)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class Snapshot:          # emitted every tick per service
    """One tick of one service: counts, rate, baseline stats, z, ratio, state and severity."""
    ts: float
    service: str
    total: int
    errors: int
    error_rate: float
    baseline_mean: float | None
    baseline_std: float | None
    z: float | None
    ratio: float | None
    state: Literal["WARMUP", "LOW_DATA", "NORMAL", "ANOMALY"]
    severity: Literal["NONE", "MEDIUM", "HIGH", "CRITICAL"]


@dataclass
class Alert:
    """An incident with its evidence; persisted, delivered and shown on the dashboard."""
    id: str
    dedup_key: str
    service: str
    severity: str                      # current severity
    peak_severity: str                 # highest severity reached while open
    status: Literal["OPEN", "RESOLVED"]
    created_at: float
    resolved_at: float | None
    current_rate: float
    baseline_rate: float
    z: float
    ratio: float
    sample_size: int
    reason: str
