"""Rolling baseline of the error rate: mean and sigma of samples taken on a fixed cadence."""
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
        """Admit one sample (the caller decides whether it is allowed in)."""
        self._samples.append(rate)

    def load(self, rates: Iterable[float]) -> None:
        """Replace the samples, e.g. when restoring after a restart."""
        self._samples.clear()
        self._samples.extend(rates)

    def clear(self) -> None:
        """Forget every sample (a level shift starts over)."""
        self._samples.clear()

    @property
    def n(self) -> int:
        """Number of samples held."""
        return len(self._samples)

    @property
    def ready(self) -> bool:
        """True once there are enough samples to judge deviations (otherwise the service is in WARMUP)."""
        return self.n >= self.min_samples

    @property
    def mean(self) -> float | None:
        """Mean of the samples, or None when there are none."""
        return statistics.fmean(self._samples) if self._samples else None

    @property
    def std(self) -> float | None:
        """Population standard deviation of the samples, or None when there are none."""
        return statistics.pstdev(self._samples) if self._samples else None

    def z(self, rate: float) -> float:
        """z-score of `rate`, with sigma floored at std_floor so a steady baseline cannot explode z."""
        return (rate - self.mean) / max(self.std, self.std_floor)

    def ratio(self, rate: float) -> float:
        """`rate` relative to the baseline mean, with the denominator floored at ratio_floor."""
        return rate / max(self.mean, self.ratio_floor)
