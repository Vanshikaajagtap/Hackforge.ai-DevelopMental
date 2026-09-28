"""AlertSink contract + shared message rendering. Detection never imports sinks; sinks never block detection."""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Literal, Protocol

from app.detection.models import Alert

Event = Literal["created", "escalated", "resolved"]


class AlertSink(Protocol):
    """The contract every delivery channel implements; detection never imports concrete sinks."""
    name: str

    async def send(self, alert: Alert, event: str) -> str | None:
        """Deliver the alert. Return an external id if the channel has one (SNS MessageId, CloudWatch group:stream) -
        it is stored on the delivery row and shown on the dashboard as proof of delivery.
        Raise on failure; the dispatcher handles retries and delivery status."""
        ...


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S UTC")


def headline(alert: Alert, event: str) -> str:
    """First line of an alert message: severity, service and error rate (or the recovery notice)."""
    if event == "resolved":
        return f"[RESOLVED] {alert.service} error rate recovered (peak {alert.peak_severity} {_pct(alert.current_rate)})"
    suffix = " (escalated)" if event == "escalated" else ""
    return f"[{alert.severity}] {alert.service} error rate {_pct(alert.current_rate)}{suffix}"


def render(alert: Alert, event: str, window_seconds: float | None = None) -> str:
    """The §35 message: what, where, when, current, normal, how much, evidence, sample size, severity."""
    delta_pp = (alert.current_rate - alert.baseline_rate) * 100
    events = f"{alert.sample_size} in last {window_seconds:g} s" if window_seconds else str(alert.sample_size)
    lines = [
        headline(alert, event),
        "",
        f"Baseline:   {_pct(alert.baseline_rate)}   ({delta_pp:+.1f} pp, {alert.ratio:.1f}×)",
        f"Z-score:    {alert.z:.2f}",
        f"Events:     {events}",
        f"Opened:     {_utc(alert.created_at)}",
    ]
    if alert.resolved_at is not None:
        lines.append(f"Resolved:   {_utc(alert.resolved_at)}")
    lines.append(f"Reason:     {alert.reason}")
    return "\n".join(lines)


def ascii_safe(text: str) -> str:
    """HTTP header values must be latin-1/ASCII; service names come from log data."""
    return text.encode("ascii", "replace").decode("ascii")


def alert_payload(alert: Alert, event: str) -> dict:
    """Structured record - the JSONL line and the CloudWatch PutLogEvents message share this schema."""
    return {
        "alert_id": alert.id,
        "event": event,
        "timestamp": datetime.fromtimestamp(time.time(), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "service": alert.service,
        "severity": alert.severity,
        "peak_severity": alert.peak_severity,
        "status": alert.status,
        "current_error_rate": round(alert.current_rate, 4),
        "baseline_error_rate": round(alert.baseline_rate, 4),
        "z_score": round(alert.z, 2),
        "relative_change": round(alert.ratio, 2),
        "sample_size": alert.sample_size,
        "reason": alert.reason,
    }
