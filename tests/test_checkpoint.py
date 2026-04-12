"""Tests for checkpoint store implementations."""

import pytest
from datetime import datetime
from farsight.markets.engine.checkpoint import MemoryCheckpointStore


@pytest.fixture
def store():
    return MemoryCheckpointStore()


class TestMemoryCheckpointStore:
    @pytest.mark.asyncio
    async def test_get_unset_returns_none(self, store):
        assert await store.get_last("unknown_source") is None

    @pytest.mark.asyncio
    async def test_update_and_get(self, store):
        ts = datetime(2026, 4, 10, 12, 0, 0)
        await store.update("polymarket_ws", ts)
        result = await store.get_last("polymarket_ws")
        assert result == ts

    @pytest.mark.asyncio
    async def test_update_only_advances(self, store):
        ts1 = datetime(2026, 4, 10, 12, 0, 0)
        ts2 = datetime(2026, 4, 10, 11, 0, 0)  # Earlier
        ts3 = datetime(2026, 4, 10, 13, 0, 0)  # Later

        await store.update("src", ts1)
        await store.update("src", ts2)  # Should not go backwards
        assert await store.get_last("src") == ts1

        await store.update("src", ts3)  # Should advance
        assert await store.get_last("src") == ts3

    @pytest.mark.asyncio
    async def test_multiple_sources_independent(self, store):
        ts_a = datetime(2026, 1, 1)
        ts_b = datetime(2026, 6, 1)

        await store.update("source_a", ts_a)
        await store.update("source_b", ts_b)

        assert await store.get_last("source_a") == ts_a
        assert await store.get_last("source_b") == ts_b

    @pytest.mark.asyncio
    async def test_get_all(self, store):
        await store.update("a", datetime(2026, 1, 1))
        await store.update("b", datetime(2026, 2, 1))

        all_cp = await store.get_all()
        assert len(all_cp) == 2
        assert "a" in all_cp
        assert "b" in all_cp

    @pytest.mark.asyncio
    async def test_clear(self, store):
        await store.update("src", datetime(2026, 1, 1))
        await store.clear("src")
        assert await store.get_last("src") is None
