"""NDJSON line -> LogEvent. Bad input raises ParseError; the caller counts it and moves on."""
from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .models import LogEvent

ERROR_LEVELS = {"ERROR", "CRITICAL", "FATAL"}


class ParseError(ValueError):
    pass


def _parse_ts(value: object) -> float:
    if isinstance(value, bool):
        raise ParseError("bad timestamp")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as e:
            raise ParseError(f"bad timestamp: {value[:40]!r}") from e
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    raise ParseError("missing timestamp")


def parse_line(line: str) -> LogEvent:
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        raise ParseError(f"invalid json: {e.msg}") from e
    if not isinstance(obj, dict):
        raise ParseError("json is not an object")

    for key in ("timestamp", "service", "level"):
        if obj.get(key) in (None, ""):
            raise ParseError(f"missing field: {key}")

    status = obj.get("status")
    if status is not None:
        try:
            status = int(status)
        except (TypeError, ValueError) as e:
            raise ParseError(f"bad status: {str(status)[:20]!r}") from e

    level = str(obj["level"]).upper()
    is_error = level in ERROR_LEVELS or (status is not None and 500 <= status <= 599)
    request_id = obj.get("request_id")
    return LogEvent(
        ts=_parse_ts(obj["timestamp"]),
        service=str(obj["service"]),
        level=level,
        status=status,
        message=str(obj.get("message", "")),
        request_id=None if request_id is None else str(request_id),
        is_error=is_error,
    )


@dataclass
class IngestStats:
    """Counters surfaced on the health panel."""
    lines_read: int = 0
    events_parsed: int = 0
    parse_errors: int = 0
    dropped_events: int = 0                     # queue overflow (drop-oldest)
    parse_error_samples: deque = field(default_factory=lambda: deque(maxlen=20))
    last_event_ts: float | None = None          # timestamp inside the last parsed event
    last_event_seen_at: float | None = None     # clock time we parsed it

    def record_error(self, line: str, reason: str) -> None:
        self.parse_errors += 1
        self.parse_error_samples.append({"line": line[:200], "reason": reason})
