from __future__ import annotations

from app.detection.models import Alert

from .base import render


class ConsoleSink:
    name = "console"

    def __init__(self, window_seconds: float | None = None, out=print) -> None:
        self.window_seconds = window_seconds
        self._out = out

    async def send(self, alert: Alert, event: str) -> None:
        self._out(f"\n=== ALERT {event.upper()} ===\n{render(alert, event, self.window_seconds)}\n", flush=True)
