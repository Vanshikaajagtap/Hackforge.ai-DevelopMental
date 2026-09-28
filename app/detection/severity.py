"""Severity policy: which row of the gate table an observation satisfies."""
from __future__ import annotations

from app.config import SeverityCfg, SeverityRow

RANK = {"NONE": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def rank(severity: str) -> int:
    """Numeric order of a severity name (NONE < MEDIUM < HIGH < CRITICAL)."""
    return RANK[severity]


def _meets(row: SeverityRow, z: float, rate: float, ratio: float, errors: int) -> bool:
    # z gate AND min-errors gate AND (absolute-rate floor OR ratio floor)
    return z >= row.z and errors >= row.errors and (rate >= row.rate or ratio >= row.ratio)


def classify(z: float, rate: float, ratio: float, errors: int, cfg: SeverityCfg) -> str:
    """Highest satisfied row wins."""
    if _meets(cfg.critical, z, rate, ratio, errors):
        return "CRITICAL"
    if _meets(cfg.high, z, rate, ratio, errors):
        return "HIGH"
    if _meets(cfg.medium, z, rate, ratio, errors):
        return "MEDIUM"
    return "NONE"
