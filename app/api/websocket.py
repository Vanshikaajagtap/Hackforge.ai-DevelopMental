"""WebSocket hub. Messages are {"type": ..., "data": ...}: hello, metric.update, alert.created/updated/resolved,
health.update. The client sends {"type":"ping"} every 60 s (keeps Render's free tier awake)."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

from fastapi import WebSocket, WebSocketDisconnect

log = logging.getLogger("logpulse.ws")


class Hub:
    """Fan-out of `{type, data}` JSON messages to the connected dashboards."""
    def __init__(self, hello_factory: Callable[[], dict[str, Any]]) -> None:
        self._clients: set[WebSocket] = set()
        self._hello_factory = hello_factory

    @property
    def client_count(self) -> int:
        """Number of connected dashboards."""
        return len(self._clients)

    @staticmethod
    async def _send(ws: WebSocket, text: str) -> bool:
        try:
            await ws.send_text(text)
            return True
        except Exception:  # noqa: BLE001 - a dead client is just dropped
            return False

    async def broadcast(self, type_: str, data: Any) -> None:
        """Send one message to every client, silently dropping dead connections."""
        if not self._clients:
            return
        text = json.dumps({"type": type_, "data": data})
        clients = list(self._clients)
        results = await asyncio.gather(*(self._send(ws, text) for ws in clients))
        for ws, ok in zip(clients, results):
            if not ok:
                self._clients.discard(ws)

    async def handle(self, ws: WebSocket) -> None:
        """Serve one client: send `hello` with the current state, then hold the socket open for pings."""
        await ws.accept()
        try:
            await ws.send_text(json.dumps({"type": "hello", "data": self._hello_factory()}))
            self._clients.add(ws)     # only after hello, so a fresh client never sees an update before its state
            while True:
                await ws.receive_text()   # pings; receiving also detects the disconnect
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001
            log.debug("ws closed", exc_info=True)
        finally:
            self._clients.discard(ws)
