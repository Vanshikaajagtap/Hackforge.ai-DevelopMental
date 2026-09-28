from __future__ import annotations

from collections import deque


class SlidingWindow:
    """Time-based window of (ts, is_error) with running counts; each event is inserted and evicted once.

    An event is kept while ts >= now - window_seconds (an event exactly on the boundary stays in).
    """

    def __init__(self, window_seconds: float) -> None:
        self.window_seconds = window_seconds
        self._events: deque[tuple[float, bool]] = deque()
        self.total = 0
        self.errors = 0

    def add(self, ts: float, is_error: bool, now: float) -> None:
        self._events.append((ts, is_error))
        self.total += 1
        self.errors += int(is_error)
        self.evict(now)

    def evict(self, now: float) -> None:
        cutoff = now - self.window_seconds
        ev = self._events
        while ev and ev[0][0] < cutoff:
            _, was_error = ev.popleft()
            self.total -= 1
            self.errors -= int(was_error)

    @property
    def error_rate(self) -> float:
        return self.errors / self.total if self.total else 0.0
