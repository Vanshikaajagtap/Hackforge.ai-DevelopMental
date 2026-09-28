"""ntfy.sh push sink (free, no signup; the topic acts as the password)."""
from __future__ import annotations

import httpx

from app.detection.models import Alert

from .base import ascii_safe, render


class NtfySink:
    """Live push, no signup. The topic is effectively a password - use a long random one."""
    name = "ntfy"

    def __init__(self, topic: str, base_url: str = "https://ntfy.sh", window_seconds: float | None = None,
                 transport: httpx.AsyncBaseTransport | None = None, timeout: float = 5.0) -> None:
        self.url = f"{base_url.rstrip('/')}/{topic}"
        self.window_seconds = window_seconds
        self._transport = transport
        self._timeout = timeout

    async def send(self, alert: Alert, event: str) -> None:
        """POST the alert to the topic with title, priority and tags."""
        if event == "resolved":
            prio, tags, label = "default", "white_check_mark", "RESOLVED"
        else:
            prio = {"CRITICAL": "urgent", "HIGH": "high"}.get(alert.severity, "default")
            tags, label = "rotating_light", alert.severity
        headers = {
            "Title": ascii_safe(f"[{label}] {alert.service} error rate ({event})"),
            "Priority": prio,
            "Tags": tags,
        }
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as c:
            r = await c.post(self.url, content=render(alert, event, self.window_seconds).encode("utf-8"), headers=headers)
            r.raise_for_status()
