"""Tests for Kalshi REST client."""

import pytest
import httpx
from unittest.mock import AsyncMock, patch
from farsight.markets.clients.kalshi.rest_client import KalshiClient


def _mock_response(status_code: int, json_data):
    request = httpx.Request("GET", "https://api.elections.kalshi.com/test")
    return httpx.Response(status_code, json=json_data, request=request)


class TestKalshiClientHTTP:
    @pytest.fixture
    def client(self):
        return KalshiClient()

    @pytest.mark.asyncio
    async def test_get_events_success(self, client):
        mock_resp = _mock_response(200, {"events": [{"event_ticker": "EVT1"}], "cursor": ""})
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            result = await client.get_events(limit=5)
            assert len(result["events"]) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_get_events_error_returns_empty(self, client):
        mock_resp = _mock_response(500, {"error": "internal"})
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            result = await client.get_events()
            assert result["events"] == []
        await client.close()

    @pytest.mark.asyncio
    async def test_get_markets_success(self, client):
        mock_resp = _mock_response(200, {"markets": [{"ticker": "MKT1"}], "cursor": ""})
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            result = await client.get_markets(limit=5)
            assert len(result["markets"]) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_get_market_not_found(self, client):
        mock_resp = _mock_response(404, {})
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            result = await client.get_market("NONEXISTENT")
            assert result is None
        await client.close()


class TestKalshiNormalization:
    def test_market_to_probability(self):
        market = {"yes_bid_dollars": "0.55", "yes_ask_dollars": "0.57", "last_price_dollars": "0.56"}
        prob = KalshiClient.market_to_probability(market)
        assert prob == pytest.approx(0.56)

    def test_market_to_probability_last_fallback(self):
        market = {"yes_bid_dollars": None, "yes_ask_dollars": None, "last_price_dollars": "0.42"}
        prob = KalshiClient.market_to_probability(market)
        assert prob == 0.42

    def test_market_to_dict(self):
        market = {
            "ticker": "KXHIGHNY-26MAR01-B45.5",
            "event_ticker": "KXHIGHNY",
            "yes_sub_title": "NYC high > 45.5F on Mar 1",
            "status": "open",
            "yes_bid_dollars": "0.60",
            "yes_ask_dollars": "0.62",
            "no_bid_dollars": "0.38",
            "no_ask_dollars": "0.40",
            "last_price_dollars": "0.61",
            "volume_24h_fp": "1500.0000",
            "liquidity_dollars": "25000.00",
        }
        d = KalshiClient.market_to_dict(market)
        assert d["source"] == "kalshi"
        assert d["ticker"] == "KXHIGHNY-26MAR01-B45.5"
        assert d["probability"] == pytest.approx(0.61)
        assert d["volume_24h"] == 1500.0
        assert d["liquidity"] == 25000.0

    def test_safe_float_handles_none(self):
        from farsight.markets.clients.kalshi.rest_client import _safe_float
        assert _safe_float(None) == 0.0
        assert _safe_float("bad") == 0.0
        assert _safe_float("3.14") == pytest.approx(3.14)
