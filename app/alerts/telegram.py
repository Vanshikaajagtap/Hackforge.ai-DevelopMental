from __future__ import annotations

import httpx

from app.detection.models import Alert

from .base import render


class TelegramSink:
    """Bot API push. Setup: @BotFather /newbot -> token; message the bot once; read chat_id from getUpdates."""
    name = "telegram"

    def __init__(self, token: str, chat_id: str, window_seconds: float | None = None,
                 base_url: str = "https://api.telegram.org",
                 transport: httpx.AsyncBaseTransport | None = None, timeout: float = 5.0) -> None:
        self.url = f"{base_url.rstrip('/')}/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.window_seconds = window_seconds
        self._transport = transport
        self._timeout = timeout

    async def send(self, alert: Alert, event: str) -> None:
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as c:
            r = await c.post(self.url, json={"chat_id": self.chat_id, "text": render(alert, event, self.window_seconds)})
            r.raise_for_status()
