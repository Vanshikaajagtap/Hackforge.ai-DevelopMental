from __future__ import annotations

import statistics
from collections import deque
from typing import Iterable


class Baseline:
    """Rolling error-rate baseline. Samples are taken on a fixed cadence by the caller, never per event
    (overlapping windows are near-identical, which would collapse sigma and inflate z)."""

    def __init__(self, max_samples: int, min_samples: int, std_floor: float, ratio_floor: float) -> None:
        self._samples: deque[float] = deque(maxlen=max_samples)
        self.min_samples = min_samples
        self.std_floor = std_floor
        self.ratio_floor = ratio_floor

    def add(self, rate: float) -> None:
        self._samples.append(rate)

    def load(self, rates: Iterable[float]) -> None:
        self._samples.clear()
        self._samples.extend(rates)

    def clear(self) -> None:
        self._samples.clear()

    @property
    def n(self) -> int:
        return len(self._samples)

    @property
    def ready(self) -> bool:
        return self.n >= self.min_samples

    @property
    def mean(self) -> float | None:
        return statistics.fmean(self._samples) if self._samples else None

    @property
    def std(self) -> float | None:
        return statistics.pstdev(self._samples) if self._samples else None

    def z(self, rate: float) -> float:
        return (rate - self.mean) / max(self.std, self.std_floor)

    def ratio(self, rate: float) -> float:
        return rate / max(self.mean, self.ratio_floor)
