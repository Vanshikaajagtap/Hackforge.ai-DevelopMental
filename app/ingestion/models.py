from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LogEvent:
    ts: float            # epoch seconds (parsed from timestamp)
    service: str
    level: str           # upper-cased
    status: int | None
    message: str
    request_id: str | None
    is_error: bool       # level in {ERROR,CRITICAL,FATAL} OR 500<=status<=599
