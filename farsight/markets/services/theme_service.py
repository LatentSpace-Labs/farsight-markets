"""
ThemeService — maps prediction markets to themes, sectors, and tickers.

Uses keyword matching and configurable rules to connect prediction market
questions to traditional finance concepts. These mappings enable:
  - Cross-asset correlation ("Fed rate cut" → TLT, SPY, DXY)
  - Portfolio exposure analysis ("Your AAPL holding is exposed to this event")
  - Agent context enrichment

Mappings can be auto-generated, manually overridden, or LLM-generated.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ThemeMatch:
    """A matched theme mapping for a prediction market."""
    market_id: str
    market_question: str
    theme: str
    sector: Optional[str] = None
    ticker: Optional[str] = None
    macro_factor: Optional[str] = None
    confidence: float = 0.7
    source: str = "auto"

# ── Keyword-based mapping rules ──────────────────────────────────────
# Each rule: (pattern, theme, sector, tickers, macro_factor)

THEME_RULES: list[dict] = [
    # Monetary policy
    {
        "patterns": [r"fed\b", r"fomc", r"interest rate", r"rate cut", r"rate hike", r"federal reserve"],
        "theme": "monetary_policy",
        "sector": "financials",
        "tickers": ["TLT", "SPY", "DXY", "GLD"],
        "macro_factor": "fed_funds_rate",
    },
    # Inflation
    {
        "patterns": [r"\bcpi\b", r"inflation", r"consumer price"],
        "theme": "inflation",
        "sector": "consumer_staples",
        "tickers": ["TIP", "SPY", "GLD"],
        "macro_factor": "cpi",
    },
    # Recession / GDP
    {
        "patterns": [r"recession", r"\bgdp\b", r"economic growth"],
        "theme": "recession",
        "sector": "broad_market",
        "tickers": ["SPY", "IWM", "TLT", "HYG"],
        "macro_factor": "gdp",
    },
    # Trade / tariffs
    {
        "patterns": [r"tariff", r"trade deal", r"trade war", r"china.*trade", r"import.*tax"],
        "theme": "trade_war",
        "sector": "industrials",
        "tickers": ["FXI", "EEM", "SPY", "SMH"],
        "macro_factor": "tariffs",
    },
    # US elections
    {
        "patterns": [r"president.*202[4-9]", r"presidential.*election", r"democrat.*nominee", r"republican.*nominee"],
        "theme": "us_election",
        "sector": "broad_market",
        "tickers": ["SPY", "IWM", "DXY"],
        "macro_factor": "political_risk",
    },
    # Trump-specific
    {
        "patterns": [r"trump"],
        "theme": "trump_policy",
        "sector": "broad_market",
        "tickers": ["SPY", "DJT", "DXY"],
        "macro_factor": "political_risk",
    },
    # Bitcoin / crypto
    {
        "patterns": [r"bitcoin", r"\bbtc\b", r"crypto.*regulation", r"crypto.*ban"],
        "theme": "crypto",
        "sector": "crypto",
        "tickers": ["BTC-USD", "IBIT", "MSTR", "COIN"],
        "macro_factor": None,
    },
    # Ethereum
    {
        "patterns": [r"ethereum", r"\beth\b"],
        "theme": "crypto",
        "sector": "crypto",
        "tickers": ["ETH-USD", "ETHA"],
        "macro_factor": None,
    },
    # AI / tech
    {
        "patterns": [r"\bai\b.*regulation", r"artificial intelligence", r"openai", r"chatgpt"],
        "theme": "ai_regulation",
        "sector": "technology",
        "tickers": ["NVDA", "MSFT", "GOOGL", "META"],
        "macro_factor": None,
    },
    # Geopolitics - Ukraine
    {
        "patterns": [r"ukraine", r"russia.*war", r"ceasefire"],
        "theme": "ukraine_conflict",
        "sector": "energy",
        "tickers": ["XLE", "USO", "WEAT", "RSX"],
        "macro_factor": "geopolitical_risk",
    },
    # Geopolitics - China/Taiwan
    {
        "patterns": [r"china.*taiwan", r"taiwan.*invasion", r"strait"],
        "theme": "china_taiwan",
        "sector": "technology",
        "tickers": ["TSM", "SMH", "FXI", "EWY"],
        "macro_factor": "geopolitical_risk",
    },
    # Geopolitics - Middle East
    {
        "patterns": [r"iran", r"israel", r"middle east", r"gaza"],
        "theme": "middle_east",
        "sector": "energy",
        "tickers": ["XLE", "USO", "GLD"],
        "macro_factor": "geopolitical_risk",
    },
    # Oil / energy
    {
        "patterns": [r"\boil\b", r"opec", r"crude", r"energy.*price"],
        "theme": "energy",
        "sector": "energy",
        "tickers": ["USO", "XLE", "CVX", "XOM"],
        "macro_factor": "oil_price",
    },
]


class ThemeService:
    """Maps prediction markets to themes, sectors, and tickers via keyword rules."""

    def map_market(self, condition_id: str, question: str) -> list[ThemeMatch]:
        """Generate theme mappings for a market question."""
        question_lower = question.lower()
        mappings = []

        for rule in THEME_RULES:
            matched = any(re.search(p, question_lower) for p in rule["patterns"])
            if not matched:
                continue

            for ticker in rule.get("tickers", []):
                mappings.append(ThemeMatch(
                    market_id=condition_id,
                    market_question=question[:500],
                    theme=rule["theme"],
                    sector=rule.get("sector"),
                    ticker=ticker,
                    macro_factor=rule.get("macro_factor"),
                ))

        return mappings

    def get_tickers_for_question(self, question: str) -> list[str]:
        """Get relevant tickers by analyzing the question text."""
        question_lower = question.lower()
        tickers = set()
        for rule in THEME_RULES:
            if any(re.search(p, question_lower) for p in rule["patterns"]):
                tickers.update(rule.get("tickers", []))
        return list(tickers)

    def get_theme_for_question(self, question: str) -> str | None:
        """Get the primary theme for a market question."""
        question_lower = question.lower()
        for rule in THEME_RULES:
            if any(re.search(p, question_lower) for p in rule["patterns"]):
                return rule["theme"]
        return None
