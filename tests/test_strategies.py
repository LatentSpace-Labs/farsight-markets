"""Tests for strategy base classes and opportunity scoring."""

import pytest
from datetime import datetime, timedelta
from farsight.markets.strategies.base import (
    Opportunity, Action, ActionType, Strategy, StrategyMode,
)


class TestOpportunity:
    def test_compute_score_basic(self):
        opp = Opportunity(
            market_id="m1",
            market_question="Test?",
            edge=0.05,
            confidence=0.8,
            liquidity=50000,
            spread=0.02,
        )
        score = opp.compute_score()
        assert score > 0

    def test_higher_edge_higher_score(self):
        opp1 = Opportunity(market_id="m1", market_question="?", edge=0.03, confidence=0.7, liquidity=50000, spread=0.02)
        opp2 = Opportunity(market_id="m2", market_question="?", edge=0.08, confidence=0.7, liquidity=50000, spread=0.02)
        opp1.compute_score()
        opp2.compute_score()
        assert opp2.score > opp1.score

    def test_higher_liquidity_higher_score(self):
        opp1 = Opportunity(market_id="m1", market_question="?", edge=0.05, confidence=0.7, liquidity=5000, spread=0.02)
        opp2 = Opportunity(market_id="m2", market_question="?", edge=0.05, confidence=0.7, liquidity=100000, spread=0.02)
        opp1.compute_score()
        opp2.compute_score()
        assert opp2.score > opp1.score

    def test_wider_spread_lower_score(self):
        opp1 = Opportunity(market_id="m1", market_question="?", edge=0.05, confidence=0.7, liquidity=50000, spread=0.01)
        opp2 = Opportunity(market_id="m2", market_question="?", edge=0.05, confidence=0.7, liquidity=50000, spread=0.10)
        opp1.compute_score()
        opp2.compute_score()
        assert opp1.score > opp2.score

    def test_risk_flags_lower_score(self):
        opp1 = Opportunity(market_id="m1", market_question="?", edge=0.05, confidence=0.7, liquidity=50000, spread=0.02)
        opp2 = Opportunity(market_id="m2", market_question="?", edge=0.05, confidence=0.7, liquidity=50000, spread=0.02,
                          risk_flags=["low_liquidity", "near_resolution", "extreme_deviation"])
        opp1.compute_score()
        opp2.compute_score()
        assert opp1.score > opp2.score

    def test_to_dict(self):
        opp = Opportunity(
            market_id="m1", market_question="Will it rain?",
            strategy="scanner", edge=0.05, confidence=0.7,
        )
        opp.compute_score()
        d = opp.to_dict()
        assert d["market_id"] == "m1"
        assert d["strategy"] == "scanner"
        assert d["score"] > 0


class TestResolutionScalperFairValue:
    def test_fair_value_increases_near_resolution(self):
        from farsight.markets.strategies.resolution_scalper import ResolutionAnalyzer
        fv_7d = ResolutionAnalyzer._estimate_fair_value(0.90, 7.0)
        fv_1d = ResolutionAnalyzer._estimate_fair_value(0.90, 1.0)
        assert fv_1d > fv_7d

    def test_fair_value_capped_at_99(self):
        from farsight.markets.strategies.resolution_scalper import ResolutionAnalyzer
        fv = ResolutionAnalyzer._estimate_fair_value(0.98, 0.1)
        assert fv <= 0.99

    def test_fair_value_at_resolution(self):
        from farsight.markets.strategies.resolution_scalper import ResolutionAnalyzer
        fv = ResolutionAnalyzer._estimate_fair_value(0.90, 0)
        assert fv == 0.90


class TestNegRiskFiltering:
    """Arb strategy should ONLY consider neg_risk events (mutually exclusive outcomes)."""

    def test_neg_risk_event_included(self):
        """Events with neg_risk=True are mutually exclusive — arb applies."""
        from farsight.markets.schemas.events import MarketSchema, MarketSource, OutcomeSchema

        # Hungary election: neg_risk=True, candidates are mutually exclusive
        markets = [
            MarketSchema(source=MarketSource.POLYMARKET, condition_id="1",
                         question="Will Peter Magyar win?", neg_risk=True,
                         outcomes=[OutcomeSchema(token_id="t1", label="Peter Magyar", current_price=0.70)]),
            MarketSchema(source=MarketSource.POLYMARKET, condition_id="2",
                         question="Will Viktor Orban win?", neg_risk=True,
                         outcomes=[OutcomeSchema(token_id="t2", label="Viktor Orban", current_price=0.30)]),
        ]
        assert any(m.neg_risk for m in markets)

    def test_non_neg_risk_event_excluded(self):
        """Events without neg_risk are independent binary contracts — arb does NOT apply."""
        from farsight.markets.schemas.events import MarketSchema, MarketSource, OutcomeSchema

        # Hyperliquid airdrop: neg_risk=False, each market is independent YES/NO
        markets = [
            MarketSchema(source=MarketSource.POLYMARKET, condition_id="1",
                         question="Airdrop by March?", neg_risk=False,
                         outcomes=[OutcomeSchema(token_id="t1", label="Yes", current_price=0.10)]),
            MarketSchema(source=MarketSource.POLYMARKET, condition_id="2",
                         question="Airdrop by June?", neg_risk=False,
                         outcomes=[OutcomeSchema(token_id="t2", label="Yes", current_price=0.25)]),
        ]
        assert not any(m.neg_risk for m in markets)


class TestCrossEventArbRisk:
    def test_risk_flags_near_resolution(self):
        from farsight.markets.strategies.cross_event_arb import _assess_risk
        from farsight.markets.strategies.base import MarketContext
        ctx = MarketContext(
            end_date=datetime.utcnow() + timedelta(hours=12),
            liquidity=100000,
        )
        flags = _assess_risk(ctx, 0.05)
        assert "near_resolution" in flags

    def test_risk_flags_low_liquidity(self):
        from farsight.markets.strategies.cross_event_arb import _assess_risk
        from farsight.markets.strategies.base import MarketContext
        ctx = MarketContext(
            end_date=datetime.utcnow() + timedelta(days=30),
            liquidity=5000,
        )
        flags = _assess_risk(ctx, 0.04)
        assert "low_liquidity" in flags

    def test_risk_flags_extreme_deviation(self):
        from farsight.markets.strategies.cross_event_arb import _assess_risk
        from farsight.markets.strategies.base import MarketContext
        ctx = MarketContext(
            end_date=datetime.utcnow() + timedelta(days=30),
            liquidity=100000,
        )
        flags = _assess_risk(ctx, 0.15)
        assert "extreme_deviation" in flags
