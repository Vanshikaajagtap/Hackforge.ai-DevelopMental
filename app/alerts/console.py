"""Console sink: prints alerts to stdout for local visibility."""
from __future__ import annotations

from app.detection.models import Alert

from .base import render


class ConsoleSink:
    """Always-on development channel that prints the rendered alert."""
    name = "console"

    def __init__(self, window_seconds: float | None = None, out=print) -> None:
        self.window_seconds = window_seconds
        self._out = out

    async def send(self, alert: Alert, event: str) -> None:
        """Print the rendered alert."""
        self._out(f"\n=== ALERT {event.upper()} ===\n{render(alert, event, self.window_seconds)}\n", flush=True)
