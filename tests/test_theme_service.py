"""Tests for theme mapping and correlation."""

import pytest
from farsight.markets.services.theme_service import ThemeService


class TestThemeMapping:
    @pytest.fixture
    def svc(self):
        return ThemeService()

    def test_fed_rate_maps_to_financials(self, svc):
        tickers = svc.get_tickers_for_question("Will the Fed cut rates in June 2026?")
        assert "TLT" in tickers
        assert "SPY" in tickers

    def test_fed_theme(self, svc):
        theme = svc.get_theme_for_question("Will the Fed cut rates?")
        assert theme == "monetary_policy"

    def test_bitcoin_maps_to_crypto(self, svc):
        tickers = svc.get_tickers_for_question("Will Bitcoin hit $100K?")
        assert "BTC-USD" in tickers
        assert "MSTR" in tickers

    def test_ukraine_maps_to_energy(self, svc):
        tickers = svc.get_tickers_for_question("Russia-Ukraine ceasefire before 2027?")
        assert "XLE" in tickers or "USO" in tickers

    def test_trump_maps_to_market(self, svc):
        tickers = svc.get_tickers_for_question("Will Trump win 2028?")
        assert "SPY" in tickers

    def test_china_taiwan_maps_to_tech(self, svc):
        tickers = svc.get_tickers_for_question("Will China invade Taiwan?")
        assert "TSM" in tickers
        assert "SMH" in tickers

    def test_no_match_returns_empty(self, svc):
        tickers = svc.get_tickers_for_question("Will it rain tomorrow in my garden?")
        assert tickers == []

    def test_map_market_returns_mappings(self, svc):
        mappings = svc.map_market("0xabc", "Will the Fed cut rates in June?")
        assert len(mappings) > 0
        assert all(m.market_id == "0xabc" for m in mappings)
        assert all(m.theme == "monetary_policy" for m in mappings)
        tickers = [m.ticker for m in mappings]
        assert "TLT" in tickers

    def test_inflation_detection(self, svc):
        theme = svc.get_theme_for_question("Will CPI come in above 3%?")
        assert theme == "inflation"

    def test_oil_detection(self, svc):
        tickers = svc.get_tickers_for_question("Will OPEC cut production?")
        assert "USO" in tickers or "XLE" in tickers
