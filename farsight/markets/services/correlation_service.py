"""
CorrelationService — joins prediction market signals to external feeds.

When a PM signal fires, this service:
1. Looks up theme mappings to find related tickers
2. Fetches current prices for those tickers (via Farsight market_data_service)
3. Checks if the asset movement confirms or diverges from the PM signal
4. Builds a composite evidence bundle

This turns "probability moved" into "probability moved AND related assets confirm."

Design for testability:
  - MarketDataProvider protocol allows injecting fake price data
  - correlate_signal() is a standalone function
"""

import logging
from datetime import datetime
from typing import Optional, Protocol

from farsight.markets.schemas.signals import SignalEvidence, SignalSchema
from farsight.markets.services.theme_service import ThemeService

logger = logging.getLogger(__name__)


class MarketDataProvider(Protocol):
    """Protocol for fetching equity/crypto prices. Enables test injection."""

    async def get_price_change(self, ticker: str) -> dict | None:
        """Return {"price": float, "change_pct": float, "change_1d": float} or None."""
        ...


class ExternalMarketDataProvider:
    """Optional provider using an external MarketDataService.

    Attempts to import a MarketDataService at runtime. If unavailable
    (e.g., running standalone without farsight.data), all lookups
    return None gracefully.
    """

    def __init__(self):
        self._service = None
        self._available = True

    def _get_service(self):
        if self._service is None and self._available:
            try:
                from farsight.data.services.market_data_service import MarketDataService
                self._service = MarketDataService()
            except ImportError:
                logger.info("farsight.data not available — cross-asset correlation disabled")
                self._available = False
        return self._service

    async def get_price_change(self, ticker: str) -> dict | None:
        try:
            svc = self._get_service()
            if svc is None:
                return None
            quote = await svc.get_quote(ticker)
            if not quote:
                return None
            return {
                "price": quote.get("price") or quote.get("regularMarketPrice", 0),
                "change_pct": quote.get("changesPercentage") or quote.get("regularMarketChangePercent", 0),
            }
        except Exception as e:
            logger.debug(f"Could not fetch price for {ticker}: {e}")
            return None


class CorrelationService:
    """Correlates prediction market signals with traditional asset movements."""

    def __init__(
        self,
        theme_service: Optional[ThemeService] = None,
        market_data: Optional[MarketDataProvider] = None,
    ):
        self.themes = theme_service or ThemeService()
        self.market_data = market_data or ExternalMarketDataProvider()

    async def correlate_signal(
        self,
        signal: SignalSchema,
        market_question: str = "",
    ) -> dict:
        """Given a PM signal, find and check related asset movements.

        Returns a correlation result with:
          - related_tickers: which assets are relevant
          - confirmations: assets moving in the expected direction
          - divergences: assets moving against the signal
          - composite_confidence: signal confidence adjusted by cross-asset evidence
        """
        # Find related tickers from theme mapping
        tickers = self.themes.get_tickers_for_question(market_question)
        if not tickers:
            return {
                "related_tickers": [],
                "confirmations": [],
                "divergences": [],
                "composite_confidence": signal.confidence,
                "evidence": [],
            }

        # Fetch current prices for related tickers
        confirmations = []
        divergences = []
        evidence = []

        for ticker in tickers[:6]:  # Cap at 6 to avoid too many API calls
            price_data = await self.market_data.get_price_change(ticker)
            if not price_data:
                continue

            change_pct = price_data.get("change_pct", 0)
            if change_pct == 0:
                continue

            # Determine if the asset movement confirms the PM signal
            # Bullish PM signal + asset going up = confirming
            # Bearish PM signal + asset going down = confirming
            asset_direction = "up" if change_pct > 0 else "down"
            signal_direction = signal.direction.value

            if (signal_direction == "bullish" and change_pct > 0) or \
               (signal_direction == "bearish" and change_pct < 0):
                correlation = "confirming"
                confirmations.append({
                    "ticker": ticker,
                    "change_pct": round(change_pct, 2),
                    "direction": asset_direction,
                })
            else:
                correlation = "diverging"
                divergences.append({
                    "ticker": ticker,
                    "change_pct": round(change_pct, 2),
                    "direction": asset_direction,
                })

            evidence.append(SignalEvidence(
                source=f"cross_asset:{ticker}",
                description=f"{ticker} {change_pct:+.1f}% ({correlation})",
                value=change_pct,
                weight=0.3,
            ))

        # Adjust confidence based on confirmations vs divergences
        if confirmations and not divergences:
            confidence_boost = min(0.15, len(confirmations) * 0.05)
        elif divergences and not confirmations:
            confidence_boost = -min(0.15, len(divergences) * 0.05)
        elif confirmations and divergences:
            net = len(confirmations) - len(divergences)
            confidence_boost = net * 0.03
        else:
            confidence_boost = 0

        composite_confidence = max(0, min(1.0, signal.confidence + confidence_boost))

        return {
            "related_tickers": tickers,
            "confirmations": confirmations,
            "divergences": divergences,
            "composite_confidence": round(composite_confidence, 3),
            "confidence_boost": round(confidence_boost, 3),
            "evidence": evidence,
        }

