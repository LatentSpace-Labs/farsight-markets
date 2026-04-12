"""Tests for Polymarket Gamma API client with mocked HTTP."""

import pytest
import httpx
from unittest.mock import AsyncMock, patch
from farsight.markets.clients.polymarket.gamma_client import GammaClient


@pytest.fixture
def client():
    return GammaClient(base_url="https://gamma-api.polymarket.com")


@pytest.fixture
def sample_market_response():
    return {
        "conditionId": "0xabc123def456",
        "question": "Will BTC hit 100k?",
        "slug": "btc-100k",
        "active": True,
        "closed": False,
        "resolved": False,
        "outcomePrices": '["0.35", "0.65"]',
        "clobTokenIds": '["token_yes_123", "token_no_456"]',
        "volume": "1500000",
        "liquidity": "250000",
        "minimumTickSize": "0.01",
        "minimumOrderSize": "5",
        "negRisk": False,
        "endDate": "2026-12-31T23:59:59Z",
    }


@pytest.fixture
def sample_event_response(sample_market_response):
    return {
        "slug": "btc-milestones-2026",
        "title": "BTC Price Milestones 2026",
        "description": "Bitcoin price target markets",
        "active": True,
        "tags": [{"label": "Crypto"}, {"label": "Bitcoin"}],
        "endDate": "2026-12-31T23:59:59Z",
        "markets": [sample_market_response],
    }


def _mock_response(status_code: int, json_data):
    """Create a properly formed httpx.Response with request set."""
    request = httpx.Request("GET", "https://gamma-api.polymarket.com/test")
    return httpx.Response(status_code, json=json_data, request=request)


class TestGammaClientHTTP:
    @pytest.mark.asyncio
    async def test_get_markets_success(self, client):
        mock_resp = _mock_response(200, [{"conditionId": "0x1"}])
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            markets = await client.get_markets(limit=10)
            assert len(markets) == 1
            assert markets[0]["conditionId"] == "0x1"
        await client.close()

    @pytest.mark.asyncio
    async def test_get_markets_http_error_returns_empty(self, client):
        mock_resp = _mock_response(500, {"error": "internal"})
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            markets = await client.get_markets()
            assert markets == []
        await client.close()

    @pytest.mark.asyncio
    async def test_get_events_success(self, client):
        mock_resp = _mock_response(200, [{"slug": "test-event"}])
        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=mock_resp):
            events = await client.get_events(limit=10)
            assert len(events) == 1
        await client.close()


class TestGammaNormalization:
    def test_normalize_market_full(self, sample_market_response):
        market = GammaClient.normalize_market(sample_market_response)

        assert market.condition_id == "0xabc123def456"
        assert market.question == "Will BTC hit 100k?"
        assert market.volume_total == 1500000.0
        assert market.liquidity == 250000.0
        assert market.neg_risk is False
        assert market.resolution_source == "uma_oracle"
        assert len(market.outcomes) == 2
        assert market.outcomes[0].token_id == "token_yes_123"
        assert market.outcomes[0].current_price == 0.35
        assert market.outcomes[1].token_id == "token_no_456"
        assert market.outcomes[1].current_price == 0.65
        assert market.end_date is not None

    def test_normalize_market_uses_volumeNum(self):
        """Should prefer volumeNum/liquidityNum (numbers) over volume/liquidity (strings)."""
        raw = {
            "conditionId": "0x1",
            "question": "Test?",
            "volume": "bad_string",
            "liquidity": "also_bad",
            "volumeNum": 42000.0,
            "liquidityNum": 8000.0,
            "outcomePrices": "[]",
            "clobTokenIds": "[]",
        }
        market = GammaClient.normalize_market(raw)
        assert market.volume_total == 42000.0
        assert market.liquidity == 8000.0

    def test_normalize_market_missing_optional_fields(self):
        raw = {
            "conditionId": "0xmin",
            "question": "Minimal?",
            "outcomePrices": "[]",
            "clobTokenIds": "[]",
        }
        market = GammaClient.normalize_market(raw)
        assert market.condition_id == "0xmin"
        assert len(market.outcomes) == 0
        assert market.volume_total == 0.0

    def test_normalize_market_json_string_parsing(self):
        """outcomePrices and clobTokenIds come as JSON strings from the API."""
        raw = {
            "conditionId": "0x1",
            "question": "Test?",
            "outcomePrices": '["0.80", "0.20"]',
            "clobTokenIds": '["tok_a", "tok_b"]',
            "volume": None,
            "liquidity": None,
        }
        market = GammaClient.normalize_market(raw)
        assert len(market.outcomes) == 2
        assert market.outcomes[0].current_price == 0.80

    def test_normalize_market_uses_outcome_labels_from_api(self):
        """Should use outcomes field for labels, not hardcode Yes/No."""
        raw = {
            "conditionId": "0x1",
            "question": "Who wins?",
            "outcomes": '["Trump", "Biden"]',
            "outcomePrices": '["0.60", "0.40"]',
            "clobTokenIds": '["tok_t", "tok_b"]',
        }
        market = GammaClient.normalize_market(raw)
        assert market.outcomes[0].label == "Trump"
        assert market.outcomes[1].label == "Biden"

    def test_normalize_market_uses_orderPriceMinTickSize(self):
        raw = {
            "conditionId": "0x1",
            "question": "Test?",
            "outcomePrices": "[]",
            "clobTokenIds": "[]",
            "orderPriceMinTickSize": 0.001,
            "orderMinSize": 10,
        }
        market = GammaClient.normalize_market(raw)
        assert market.min_tick_size == 0.001
        assert market.min_order_size == 10.0

    def test_normalize_event_uses_api_category_field(self):
        """Should use category from API when available, not infer."""
        raw = {
            "slug": "some-event",
            "title": "Some Event",
            "active": True,
            "category": "Sports",
            "tags": [],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "sports"  # Lowercased

    def test_normalize_event_infers_when_no_api_category(self, sample_event_response):
        event = GammaClient.normalize_event(sample_event_response)
        assert event.slug == "btc-milestones-2026"
        assert event.category == "crypto"  # Inferred from "Crypto" tag
        assert len(event.markets) == 1
        assert event.volume_total > 0

    def test_normalize_event_politics_category(self):
        raw = {
            "slug": "election",
            "title": "Election",
            "active": True,
            "tags": [{"label": "Elections"}, {"label": "Politics"}],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "politics"

    def test_infer_category_from_title_when_no_tags(self):
        """Category should be inferred from title/slug when tags are empty."""
        raw = {
            "slug": "nfl-will-the-falcons-beat-the-panthers",
            "title": "NFL: Will the Falcons beat the Panthers by more than 3.5 points?",
            "active": True,
            "tags": [],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "sports"

    def test_infer_category_crypto_from_question(self):
        raw = {
            "slug": "btc-100k",
            "title": "Will Bitcoin hit $100K?",
            "active": True,
            "tags": [],
            "markets": [{"question": "Will BTC close above $100,000?", "conditionId": "0x1",
                         "outcomePrices": "[]", "clobTokenIds": "[]"}],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "crypto"

    def test_infer_category_geopolitics(self):
        raw = {
            "slug": "russia-ukraine-ceasefire",
            "title": "Russia-Ukraine Ceasefire before GTA VI?",
            "active": True,
            "tags": [],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "geopolitics"

    def test_infer_category_economics_from_fed(self):
        raw = {
            "slug": "fed-rate-cut-june",
            "title": "Will the Fed cut rates in June 2026?",
            "active": True,
            "tags": [],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category == "economics"
