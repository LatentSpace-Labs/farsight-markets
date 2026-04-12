"""
Rolling window data structures for streaming aggregations.

Used by StateEngine to maintain per-market sliding windows of prices, volumes, trades.
O(1) add, amortized O(1) evict.

Memory: ~400KB per market (5 windows × 10K entries). 200 markets = ~80MB total.
"""

from collections import deque
from datetime import datetime, timedelta
from typing import Optional


class RollingWindow:
    """Time-bounded deque for streaming aggregations."""

    def __init__(self, duration: timedelta, maxlen: int = 10_000):
        self._data: deque[tuple[datetime, float]] = deque(maxlen=maxlen)
        self._duration = duration

    def add(self, timestamp: datetime, value: float):
        self._data.append((timestamp, value))
        self._evict()

    def _evict(self):
        cutoff = datetime.utcnow() - self._duration
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()

    @property
    def values(self) -> list[float]:
        self._evict()
        return [v for _, v in self._data]

    def mean(self) -> Optional[float]:
        vals = self.values
        return sum(vals) / len(vals) if vals else None

    def std(self) -> Optional[float]:
        vals = self.values
        if len(vals) < 2:
            return None
        m = sum(vals) / len(vals)
        variance = sum((x - m) ** 2 for x in vals) / (len(vals) - 1)
        return variance ** 0.5

    def sum(self) -> float:
        return sum(self.values)

    def count(self) -> int:
        self._evict()
        return len(self._data)

    def last(self) -> Optional[float]:
        self._evict()
        return self._data[-1][1] if self._data else None

    def first(self) -> Optional[float]:
        self._evict()
        return self._data[0][1] if self._data else None

    def delta(self) -> Optional[float]:
        """Change from first to last value in window."""
        self._evict()
        if len(self._data) < 2:
            return None
        return self._data[-1][1] - self._data[0][1]

    def min(self) -> Optional[float]:
        vals = self.values
        return min(vals) if vals else None

    def max(self) -> Optional[float]:
        vals = self.values
        return max(vals) if vals else None

    def is_empty(self) -> bool:
        self._evict()
        return len(self._data) == 0

    def last_timestamp(self) -> Optional[datetime]:
        self._evict()
        return self._data[-1][0] if self._data else None


class VolumeWeightedWindow:
    """Rolling window that tracks (timestamp, price, volume) for VWAP computation."""

    def __init__(self, duration: timedelta, maxlen: int = 10_000):
        self._data: deque[tuple[datetime, float, float]] = deque(maxlen=maxlen)
        self._duration = duration

    def add(self, timestamp: datetime, price: float, volume: float):
        self._data.append((timestamp, price, volume))
        self._evict()

    def _evict(self):
        cutoff = datetime.utcnow() - self._duration
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()

    def vwap(self) -> Optional[float]:
        """Volume-weighted average price."""
        self._evict()
        if not self._data:
            return None
        total_pv = sum(p * v for _, p, v in self._data)
        total_v = sum(v for _, _, v in self._data)
        return total_pv / total_v if total_v > 0 else None

    def total_volume(self) -> float:
        self._evict()
        return sum(v for _, _, v in self._data)

    def trade_count(self) -> int:
        self._evict()
        return len(self._data)

    def is_empty(self) -> bool:
        self._evict()
        return len(self._data) == 0
