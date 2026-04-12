"""Tests for canonical schemas — normalization and dedup keys."""

import json
import pytest
from datetime import datetime
from farsight.markets.schemas.events import (
    EventSchema,
    MarketSchema,
    MarketSource,
    MarketStatus,
    MarketTier,
    OutcomeSchema,
)
from farsight.markets.schemas.ticks import (
    OrderbookLevel,
    OrderbookSnapshot,
    PriceTick,
    TradePrint,
    TradeSide,
)
from farsight.markets.schemas.signals import (
    Direction,
    SignalEvidence,
    SignalSchema,
    SignalStatus,
    SignalType,
)
from farsight.markets.clients.polymarket.gamma_client import GammaClient


class TestPriceTick:
    def test_dedup_key_format(self):
        tick = PriceTick(
            source="polymarket",
            token_id="abc123",
            timestamp=datetime(2026, 4, 10, 12, 0, 0),
            bid=0.45,
            ask=0.55,
            mid=0.50,
            spread=0.10,
        )
        key = tick.dedup_key
        assert key.startswith("polymarket:abc123:")
        assert len(key.split(":")) == 3

    def test_dedup_key_deterministic(self):
        ts = datetime(2026, 4, 10, 12, 0, 0)
        tick1 = PriceTick(source="polymarket", token_id="x", timestamp=ts, bid=0, ask=1, mid=0.5, spread=1)
        tick2 = PriceTick(source="polymarket", token_id="x", timestamp=ts, bid=0, ask=1, mid=0.5, spread=1)
        assert tick1.dedup_key == tick2.dedup_key

    def test_from_polymarket_ws_price_change(self):
        """price_change has nested price_changes[] with bid/ask per token."""
        msg = {
            "event_type": "price_change",
            "timestamp": "1757908892351",
            "price_changes": [
                {"asset_id": "token_abc", "best_bid": "0.58", "best_ask": "0.62"},
            ],
        }
        tick = PriceTick.from_polymarket_ws(msg)
        assert tick.source == "polymarket"
        assert tick.token_id == "token_abc"
        assert tick.bid == 0.58
        assert tick.ask == 0.62
        assert tick.mid == 0.60

    def test_from_polymarket_ws_best_bid_ask(self):
        """best_bid_ask events have top-level fields."""
        msg = {
            "event_type": "best_bid_ask",
            "asset_id": "token_1",
            "best_bid": "0.73",
            "best_ask": "0.77",
            "timestamp": "1766789469958",
        }
        tick = PriceTick.from_polymarket_ws(msg)
        assert tick.bid == 0.73
        assert tick.ask == 0.77
        assert tick.mid == 0.75

    def test_all_from_polymarket_ws(self):
        """Should extract multiple ticks from price_changes array."""
        msg = {
            "event_type": "price_change",
            "timestamp": "1757908892351",
            "price_changes": [
                {"asset_id": "t_yes", "best_bid": "0.48", "best_ask": "0.52"},
                {"asset_id": "t_no", "best_bid": "0.48", "best_ask": "0.52"},
            ],
        }
        ticks = PriceTick.all_from_polymarket_ws(msg)
        assert len(ticks) == 2
        assert ticks[0].token_id == "t_yes"
        assert ticks[1].token_id == "t_no"


class TestTradePrint:
    def test_from_polymarket_ws(self):
        msg = {
            "event_type": "last_trade_price",
            "asset_id": "token_xyz",
            "price": "0.456",
            "side": "BUY",
            "size": "219.22",
            "timestamp": "1750428146322",
        }
        trade = TradePrint.from_polymarket_ws(msg)
        assert trade.source == "polymarket"
        assert trade.token_id == "token_xyz"
        assert trade.price == 0.456
        assert trade.size_usd == pytest.approx(219.22)
        assert trade.side == TradeSide.BUY

    def test_from_polymarket_ws_sell_side(self):
        msg = {"asset_id": "t1", "price": "0.5", "side": "SELL", "size": "100", "timestamp": "1750428146322"}
        trade = TradePrint.from_polymarket_ws(msg)
        assert trade.side == TradeSide.SELL

    def test_dedup_key_includes_price_and_size(self):
        trade = TradePrint(
            source="polymarket",
            token_id="t1",
            timestamp=datetime(2026, 1, 1),
            price=0.50,
            size_usd=100.0,
            side=TradeSide.BUY,
        )
        assert "0.5" in trade.dedup_key
        assert "100.0" in trade.dedup_key


class TestOrderbookSnapshot:
    def test_from_polymarket_book(self):
        data = {
            "bids": [
                {"price": "0.48", "size": "500"},
                {"price": "0.45", "size": "300"},
            ],
            "asks": [
                {"price": "0.52", "size": "400"},
                {"price": "0.55", "size": "200"},
            ],
        }
        book = OrderbookSnapshot.from_polymarket_book(data, "token_1")
        assert book.best_bid == 0.48
        assert book.best_ask == 0.52
        assert book.spread == pytest.approx(0.04)
        assert book.mid == pytest.approx(0.50)
        assert book.total_bid_depth > 0
        assert book.total_ask_depth > 0
        assert len(book.bids) == 2
        assert len(book.asks) == 2
        # Bids sorted descending
        assert book.bids[0].price > book.bids[1].price
        # Asks sorted ascending
        assert book.asks[0].price < book.asks[1].price

    def test_empty_book(self):
        book = OrderbookSnapshot.from_polymarket_book({"bids": [], "asks": []}, "t")
        assert book.best_bid == 0.0
        assert book.best_ask == 1.0
        assert book.spread == 1.0


class TestGammaNormalization:
    def test_normalize_market_basic(self):
        raw = {
            "conditionId": "0xabc123",
            "question": "Will it rain tomorrow?",
            "slug": "will-it-rain",
            "active": True,
            "closed": False,
            "resolved": False,
            "outcomePrices": '["0.65", "0.35"]',
            "clobTokenIds": '["token_yes", "token_no"]',
            "volume": "150000",
            "liquidity": "25000",
            "minimumTickSize": "0.01",
            "minimumOrderSize": "5",
            "negRisk": False,
        }
        market = GammaClient.normalize_market(raw)

        assert market.source == MarketSource.POLYMARKET
        assert market.condition_id == "0xabc123"
        assert market.question == "Will it rain tomorrow?"
        assert market.status == MarketStatus.ACTIVE
        assert market.volume_total == 150000.0
        assert market.liquidity == 25000.0
        assert len(market.outcomes) == 2
        assert market.outcomes[0].label == "Yes"
        assert market.outcomes[0].current_price == 0.65
        assert market.outcomes[1].label == "No"
        assert market.outcomes[1].current_price == 0.35

    def test_normalize_resolved_market(self):
        raw = {
            "conditionId": "0xdef",
            "question": "Test?",
            "resolved": True,
            "outcomePrices": "[]",
            "clobTokenIds": "[]",
        }
        market = GammaClient.normalize_market(raw)
        assert market.status == MarketStatus.RESOLVED

    def test_normalize_event_with_markets(self):
        raw = {
            "slug": "test-event",
            "title": "Test Event",
            "description": "A test",
            "active": True,
            "tags": [{"label": "Politics"}],
            "markets": [
                {
                    "conditionId": "0x1",
                    "question": "Q1?",
                    "outcomePrices": '["0.5", "0.5"]',
                    "clobTokenIds": '["t1", "t2"]',
                    "volume": "1000",
                    "liquidity": "500",
                },
            ],
        }
        event = GammaClient.normalize_event(raw)

        assert event.slug == "test-event"
        assert event.title == "Test Event"
        assert event.category == "politics"
        assert len(event.markets) == 1
        assert event.volume_total == 1000.0

    def test_normalize_event_no_tags(self):
        raw = {
            "slug": "no-tags",
            "title": "No Tags",
            "active": True,
            "tags": [],
            "markets": [],
        }
        event = GammaClient.normalize_event(raw)
        assert event.category is None


class TestSignalSchema:
    def test_signal_creation(self):
        from uuid import uuid4
        signal = SignalSchema(
            market_id=str(uuid4()),
            source="polymarket",
            signal_type=SignalType.PROBABILITY_SHOCK,
            direction=Direction.BEARISH,
            confidence=0.82,
            horizon="1h",
            tradability_score=0.75,
            model_probability=0.42,
            market_price=0.50,
            edge=-0.08,
            feature_set_version="v1",
            rule_version="v1",
        )
        assert signal.status == SignalStatus.ACTIVE
        assert signal.edge == -0.08
        assert signal.actual_outcome is None

    def test_all_signal_types_exist(self):
        assert len(SignalType) == 6
        assert SignalType.PROBABILITY_SHOCK.value == "probability_shock"
        assert SignalType.CROSS_VENUE_DIVERGENCE.value == "cross_venue_divergence"
