"""Async delivery with retry + backoff. Detection never awaits this; each delivery runs as its own task so a
slow or dead sink cannot delay the others."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from typing import Awaitable, Callable

from app.detection.models import Alert
from app.storage.repository import Repository

from .base import AlertSink

log = logging.getLogger("logpulse.dispatch")


@dataclass
class Delivery:
    """One (alert, channel, event) delivery to attempt; the alert is frozen as it was at event time."""
    delivery_id: int | None
    alert: Alert           # snapshot of the alert at event time (later mutation must not change retries)
    event: str
    channel: str


@dataclass
class SinkStats:
    """Per-channel delivery counters shown on the health panel."""
    success: int = 0
    failure: int = 0
    last_error: str | None = None
    last_ok_at: float | None = None
    last_ok: bool | None = None       # outcome of the most recent delivery attempt


class Dispatcher:
    """Delivers alerts asynchronously with retry, backoff and a per-send timeout; never blocks detection."""
    def __init__(
        self,
        sinks: dict[str, AlertSink],
        repo: Repository,
        retry_attempts: int = 3,
        backoff_seconds: float = 2.0,
        send_timeout: float = 15.0,
        on_update: Callable[[str], Awaitable[None]] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.sinks = sinks
        self.repo = repo
        self.retry_attempts = max(1, retry_attempts)
        self.backoff = backoff_seconds
        self.send_timeout = send_timeout
        self.on_update = on_update
        self._sleep = sleep
        self._clock = clock
        self._queue: asyncio.Queue[Delivery] = asyncio.Queue()
        self._tasks: set[asyncio.Task] = set()
        self.stats: dict[str, SinkStats] = {name: SinkStats() for name in sinks}

    def enqueue(self, delivery: Delivery) -> None:
        """Queue a delivery for the background worker."""
        self._queue.put_nowait(delivery)

    @property
    def backlog(self) -> int:
        """Deliveries queued or currently in flight."""
        return self._queue.qsize() + len(self._tasks)

    async def run(self) -> None:
        """Worker loop: each queued delivery becomes its own task so one slow sink cannot delay the others."""
        try:
            while True:
                d = await self._queue.get()
                task = asyncio.create_task(self._deliver(d))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        finally:
            for t in list(self._tasks):
                t.cancel()

    async def drain(self) -> None:
        """Wait until everything queued so far has finished (tests, graceful shutdown)."""
        while not self._queue.empty() or self._tasks:
            await asyncio.sleep(0)
            if self._tasks:
                await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def _deliver(self, d: Delivery) -> None:
        sink = self.sinks.get(d.channel)
        stats = self.stats.setdefault(d.channel, SinkStats())
        if sink is None:
            self.repo.update_delivery(d.delivery_id, "FAILED", 0, self._clock(), "sink not configured")
            await self._notify(d.alert.id)
            return
        error: str | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                ext = await asyncio.wait_for(sink.send(replace(d.alert), d.event), timeout=self.send_timeout)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - any sink failure is retried, never propagated
                error = f"{type(e).__name__}: {e}"[:300]
                stats.failure += 1
                stats.last_error, stats.last_ok = error, False
                final = attempt == self.retry_attempts
                # PENDING with attempt_count > 0 is what the dashboard shows as "retrying"
                self.repo.update_delivery(d.delivery_id, "FAILED" if final else "PENDING", attempt, self._clock(), error)
                log.warning("sink %s failed (attempt %d/%d): %s", d.channel, attempt, self.retry_attempts, error)
                await self._notify(d.alert.id)
                if not final:
                    await self._sleep(self.backoff * 2 ** (attempt - 1))
                continue
            stats.success += 1
            stats.last_ok, stats.last_ok_at = True, self._clock()
            external_id = ext if isinstance(ext, str) and ext else None     # SNS MessageId, CloudWatch group:stream
            self.repo.update_delivery(d.delivery_id, "DELIVERED", attempt, self._clock(), None, external_id)
            await self._notify(d.alert.id)
            return

    async def _notify(self, alert_id: str) -> None:
        if self.on_update is not None:
            try:
                await self.on_update(alert_id)
            except Exception:  # noqa: BLE001 - UI push must never break delivery bookkeeping
                log.exception("on_update failed")
