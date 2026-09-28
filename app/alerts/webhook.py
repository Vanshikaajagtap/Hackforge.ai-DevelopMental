from __future__ import annotations

import httpx

from app.detection.models import Alert

from .base import render


class WebhookSink:
    """Discord / Slack incoming webhook (Discord reads `content`, Slack reads `text`)."""
    name = "webhook"

    def __init__(self, url: str, window_seconds: float | None = None,
                 transport: httpx.AsyncBaseTransport | None = None, timeout: float = 5.0) -> None:
        self.url = url
        self.window_seconds = window_seconds
        self._transport = transport
        self._timeout = timeout

    async def send(self, alert: Alert, event: str) -> None:
        text = render(alert, event, self.window_seconds)
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as c:
            r = await c.post(self.url, json={"content": text, "text": text})
            r.raise_for_status()
