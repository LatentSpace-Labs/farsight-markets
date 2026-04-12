"""
Checkpoint store for crash recovery.

Tracks the last-seen event timestamp per source so the ingestor knows
where to resume after a restart. Also used by gap-fill to determine
what period to backfill.

Two implementations:
- RedisCheckpointStore: production (persistent across restarts)
- MemoryCheckpointStore: testing / development
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class CheckpointStore(ABC):
    """Abstract checkpoint store interface."""

    @abstractmethod
    async def update(self, source: str, timestamp: datetime) -> None:
        """Record the last-seen event timestamp for a source."""

    @abstractmethod
    async def get_last(self, source: str) -> Optional[datetime]:
        """Get the last-seen event timestamp for a source. None if never seen."""

    @abstractmethod
    async def get_all(self) -> dict[str, datetime]:
        """Get all source checkpoints."""

    @abstractmethod
    async def clear(self, source: str) -> None:
        """Remove checkpoint for a source."""


class MemoryCheckpointStore(CheckpointStore):
    """In-memory checkpoint store for testing and development."""

    def __init__(self):
        self._checkpoints: dict[str, datetime] = {}

    async def update(self, source: str, timestamp: datetime) -> None:
        current = self._checkpoints.get(source)
        if current is None or timestamp > current:
            self._checkpoints[source] = timestamp

    async def get_last(self, source: str) -> Optional[datetime]:
        return self._checkpoints.get(source)

    async def get_all(self) -> dict[str, datetime]:
        return dict(self._checkpoints)

    async def clear(self, source: str) -> None:
        self._checkpoints.pop(source, None)


class RedisCheckpointStore(CheckpointStore):
    """Redis-backed checkpoint store for production persistence."""

    KEY_PREFIX = "pm:checkpoint:"

    def __init__(self, redis_client):
        self._redis = redis_client

    async def update(self, source: str, timestamp: datetime) -> None:
        key = f"{self.KEY_PREFIX}{source}"
        ts_str = timestamp.isoformat()
        # Only update if newer (SETNX-like logic with comparison)
        current = await self._redis.get(key)
        if current is None or ts_str > current.decode():
            await self._redis.set(key, ts_str)

    async def get_last(self, source: str) -> Optional[datetime]:
        key = f"{self.KEY_PREFIX}{source}"
        val = await self._redis.get(key)
        if val is None:
            return None
        return datetime.fromisoformat(val.decode())

    async def get_all(self) -> dict[str, datetime]:
        # Scan for all checkpoint keys
        result = {}
        async for key in self._redis.scan_iter(f"{self.KEY_PREFIX}*"):
            source = key.decode().replace(self.KEY_PREFIX, "")
            val = await self._redis.get(key)
            if val:
                result[source] = datetime.fromisoformat(val.decode())
        return result

    async def clear(self, source: str) -> None:
        key = f"{self.KEY_PREFIX}{source}"
        await self._redis.delete(key)
