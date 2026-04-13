"""
Polymarket Gamma API client.

The Gamma API provides market discovery, metadata, and event groupings.
Public, no auth needed. Base URL: https://gamma-api.polymarket.com

Endpoints used:
  GET /events         — List events (paginated, bare JSON array)
  GET /events/{id}    — Single event by numeric ID
  GET /events/slug/{slug} — Single event by slug
  GET /markets        — List markets (paginated, bare JSON array)
  GET /markets/{id}   — Single market by numeric ID
  GET /markets/slug/{slug} — Single market by slug
  GET /tags           — List all tags

Key gotchas from API docs:
  - outcomes, outcomePrices, clobTokenIds are JSON-encoded strings, not arrays
  - volume/liquidity on Market objects are strings; use volumeNum/liquidityNum instead
  - volume/liquidity on Event objects are numbers
  - Response is bare array [], no envelope
  - No total count in response — paginate until len(batch) < limit
  - category field exists on both events and markets but not all have it
"""

import json
import logging
from datetime import datetime
from typing import Optional

import httpx

from farsight.markets.config import settings
from farsight.markets.schemas.events import (
    EventSchema,
    MarketSchema,
    MarketSource,
    MarketStatus,
    OutcomeSchema,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


def _fmt_dt(dt: datetime) -> str:
    """Gamma wants RFC3339 with 'Z' for UTC — naive isoformat is rejected."""
    if dt.tzinfo is None:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    from datetime import timezone as _tz
    return dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class GammaClient:
    """Async client for the Polymarket Gamma API."""

    def __init__(self, base_url: Optional[str] = None):
        self._base_url = base_url or settings.POLYMARKET_GAMMA_URL
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=DEFAULT_TIMEOUT,
                headers={"Accept": "application/json"},
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Markets ──────────────────────────────────────────────────────

    async def get_markets(
        self,
        active: Optional[bool] = True,
        closed: Optional[bool] = False,
        limit: int = 100,
        offset: int = 0,
        tag_id: Optional[int] = None,
        exclude_tag_id: Optional[int] = None,
        order: Optional[str] = None,
        ascending: Optional[bool] = None,
        *,
        slugs: Optional[list[str]] = None,
        condition_ids: Optional[list[str]] = None,
        clob_token_ids: Optional[list[str]] = None,
        liquidity_min: Optional[float] = None,
        liquidity_max: Optional[float] = None,
        volume_min: Optional[float] = None,
        volume_max: Optional[float] = None,
        end_date_min: Optional[datetime] = None,
        end_date_max: Optional[datetime] = None,
        include_tag: Optional[bool] = None,
    ) -> list[dict]:
        """Fetch markets from Gamma API with pagination.

        Args:
            active: Filter to active/tradable markets
            closed: Filter by closed status (default False = only open markets)
            tag_id: Include only this tag (single int per Gamma docs)
            exclude_tag_id: Exclude this tag. Undocumented in public API — may be
                silently ignored; the 5-min crypto skip in ResolutionScalper
                still uses a slug-match for that reason.
            order: Sort field — volume_24hr (recommended), volume, liquidity, start_date,
                   end_date, competitive, closed_time
            ascending: Sort direction (default False = descending)
            slugs / condition_ids / clob_token_ids: repeatable identifier filters
            liquidity_min/max, volume_min/max: numeric range filters (sent to Gamma
                as liquidity_num_min etc.)
            end_date_min/max: ISO-8601 serialized temporal bounds
            include_tag: include tag objects in each market response
        """
        client = await self._get_client()
        # Use a list-of-tuples so repeatable params (slug, condition_ids, …)
        # serialize as `?slug=a&slug=b` rather than CSV.
        params: list[tuple[str, str]] = [
            ("limit", str(limit)),
            ("offset", str(offset)),
        ]
        if active is not None:
            params.append(("active", str(active).lower()))
        if closed is not None:
            params.append(("closed", str(closed).lower()))
        if tag_id is not None:
            params.append(("tag_id", str(tag_id)))
        if exclude_tag_id is not None:
            params.append(("exclude_tag_id", str(exclude_tag_id)))
        if order:
            params.append(("order", order))
        if ascending is not None:
            params.append(("ascending", str(ascending).lower()))
        for s in slugs or []:
            params.append(("slug", s))
        for cid in condition_ids or []:
            params.append(("condition_ids", cid))
        for tid in clob_token_ids or []:
            params.append(("clob_token_ids", tid))
        if liquidity_min is not None:
            params.append(("liquidity_num_min", str(liquidity_min)))
        if liquidity_max is not None:
            params.append(("liquidity_num_max", str(liquidity_max)))
        if volume_min is not None:
            params.append(("volume_num_min", str(volume_min)))
        if volume_max is not None:
            params.append(("volume_num_max", str(volume_max)))
        if end_date_min is not None:
            params.append(("end_date_min", _fmt_dt(end_date_min)))
        if end_date_max is not None:
            params.append(("end_date_max", _fmt_dt(end_date_max)))
        if include_tag is not None:
            params.append(("include_tag", str(include_tag).lower()))

        try:
            resp = await client.get("/markets", params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            body = e.response.text[:500] if e.response is not None else ""
            logger.error(
                f"Gamma API {e.response.status_code} fetching markets: "
                f"params={dict(params)} body={body}"
            )
            return []
        except httpx.RequestError as e:
            logger.error(f"Gamma API request error: {e}")
            return []

    async def get_market_by_slug(self, slug: str) -> Optional[dict]:
        """Fetch a single market by slug. Correct endpoint: /markets/slug/{slug}."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/markets/slug/{slug}")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    async def get_market_by_id(self, market_id: str) -> Optional[dict]:
        """Fetch a single market by numeric ID."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/markets/{market_id}")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    async def get_all_active_markets(self, max_pages: int = 50) -> list[dict]:
        """Paginate through all active markets."""
        all_markets = []
        offset = 0
        limit = 100

        for _ in range(max_pages):
            batch = await self.get_markets(active=True, limit=limit, offset=offset)
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        logger.info(f"Fetched {len(all_markets)} active markets from Gamma API")
        return all_markets

    # ── Events ───────────────────────────────────────────────────────

    async def get_events(
        self,
        active: Optional[bool] = True,
        closed: Optional[bool] = False,
        limit: int = 100,
        offset: int = 0,
        tag_id: Optional[int] = None,
        exclude_tag_id: Optional[int] = None,
        order: Optional[str] = None,
        ascending: Optional[bool] = None,
        *,
        tag_slug: Optional[str] = None,
        slugs: Optional[list[str]] = None,
        liquidity_min: Optional[float] = None,
        liquidity_max: Optional[float] = None,
        volume_min: Optional[float] = None,
        volume_max: Optional[float] = None,
        end_date_min: Optional[datetime] = None,
        end_date_max: Optional[datetime] = None,
    ) -> list[dict]:
        """Fetch events (grouped markets) from Gamma API.

        Unlike /markets, /events accepts `tag_slug` directly — no ID lookup needed.
        """
        client = await self._get_client()
        params: list[tuple[str, str]] = [
            ("limit", str(limit)),
            ("offset", str(offset)),
        ]
        if active is not None:
            params.append(("active", str(active).lower()))
        if closed is not None:
            params.append(("closed", str(closed).lower()))
        if tag_id is not None:
            params.append(("tag_id", str(tag_id)))
        if exclude_tag_id is not None:
            params.append(("exclude_tag_id", str(exclude_tag_id)))
        if tag_slug:
            params.append(("tag_slug", tag_slug))
        if order:
            params.append(("order", order))
        if ascending is not None:
            params.append(("ascending", str(ascending).lower()))
        for s in slugs or []:
            params.append(("slug", s))
        if liquidity_min is not None:
            params.append(("liquidity_min", str(liquidity_min)))
        if liquidity_max is not None:
            params.append(("liquidity_max", str(liquidity_max)))
        if volume_min is not None:
            params.append(("volume_min", str(volume_min)))
        if volume_max is not None:
            params.append(("volume_max", str(volume_max)))
        if end_date_min is not None:
            params.append(("end_date_min", _fmt_dt(end_date_min)))
        if end_date_max is not None:
            params.append(("end_date_max", _fmt_dt(end_date_max)))

        try:
            resp = await client.get("/events", params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Gamma API error fetching events: {e.response.status_code}")
            return []
        except httpx.RequestError as e:
            logger.error(f"Gamma API request error: {e}")
            return []

    async def get_event_by_slug(self, slug: str) -> Optional[dict]:
        """Fetch a single event by slug."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/events/slug/{slug}")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    async def get_all_active_events(self, max_pages: int = 50) -> list[dict]:
        """Paginate through all active events."""
        all_events = []
        offset = 0
        limit = 100

        for _ in range(max_pages):
            batch = await self.get_events(active=True, limit=limit, offset=offset)
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        logger.info(f"Fetched {len(all_events)} active events from Gamma API")
        return all_events

    # ── Tags ─────────────────────────────────────────────────────────

    async def get_tags(self) -> list[dict]:
        """Fetch all available tags. Use tag IDs for filtering markets/events."""
        client = await self._get_client()
        try:
            resp = await client.get("/tags", params={"limit": 500})
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"Gamma API error fetching tags: {e}")
            return []

    async def get_tag_by_slug(self, slug: str) -> Optional[dict]:
        """Fetch a single tag by slug."""
        client = await self._get_client()
        try:
            resp = await client.get(f"/tags/slug/{slug}")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            return None

    async def resolve_tag_slugs(self, slugs: list[str]) -> dict[str, int]:
        """Resolve tag slugs to IDs, cached on the client for the session.

        /markets only accepts `tag_id`, not `tag_slug`. This helper fills
        that asymmetry with a single cached lookup.
        """
        if not slugs:
            return {}
        cache = getattr(self, "_tag_slug_cache", None)
        if cache is None:
            cache = {}
            for tag in await self.get_tags():
                tslug = tag.get("slug")
                tid = tag.get("id")
                if tslug and tid is not None:
                    try:
                        cache[tslug] = int(tid)
                    except (TypeError, ValueError):
                        continue
            self._tag_slug_cache = cache
        return {s: cache[s] for s in slugs if s in cache}

    # ── Normalization ────────────────────────────────────────────────

    @staticmethod
    def normalize_market(raw: dict) -> MarketSchema:
        """Convert a Gamma API market response into our canonical schema.

        Handles the API's quirks:
        - outcomePrices, clobTokenIds, outcomes are JSON-encoded strings
        - volume/liquidity are strings; prefer volumeNum/liquidityNum
        - bestBid/bestAsk/lastTradePrice/spread are available as numbers
        """
        # Parse JSON-encoded string fields
        outcome_prices = _parse_json_string(raw.get("outcomePrices", "[]"))
        clob_token_ids = _parse_json_string(raw.get("clobTokenIds", "[]"))
        outcome_labels = _parse_json_string(raw.get("outcomes", '["Yes", "No"]'))

        # Build outcomes with proper labels
        # For neg-risk events (multi-outcome like elections), groupItemTitle has the
        # candidate name ("Peter Magyar"), while outcomes is just ["Yes", "No"]
        group_title = raw.get("groupItemTitle") or ""
        outcomes = []
        for i, token_id in enumerate(clob_token_ids):
            price = float(outcome_prices[i]) if i < len(outcome_prices) else 0.0
            if i == 0 and group_title:
                # First outcome (YES) gets the candidate/group name
                label = group_title
            else:
                label = outcome_labels[i] if i < len(outcome_labels) else f"Outcome {i + 1}"
            outcomes.append(OutcomeSchema(
                token_id=str(token_id),
                label=label,
                current_price=price,
                volume_24h=0.0,
            ))

        # Determine status
        status = MarketStatus.ACTIVE
        if raw.get("resolved"):
            status = MarketStatus.RESOLVED
        elif raw.get("closed"):
            status = MarketStatus.CLOSED
        elif raw.get("archived"):
            status = MarketStatus.ARCHIVED

        # Parse end date
        end_date = _parse_datetime(raw.get("endDate"))

        # Use numeric fields (volumeNum/liquidityNum) when available, fall back to string parsing
        volume = raw.get("volumeNum") or _safe_float(raw.get("volume"))
        liquidity = raw.get("liquidityNum") or _safe_float(raw.get("liquidity"))

        return MarketSchema(
            source=MarketSource.POLYMARKET,
            condition_id=raw.get("conditionId", ""),
            question=raw.get("question", ""),
            slug=raw.get("slug"),
            status=status,
            min_tick_size=float(raw.get("orderPriceMinTickSize") or raw.get("minimumTickSize") or 0.01),
            min_order_size=float(raw.get("orderMinSize") or raw.get("minimumOrderSize") or 5.0),
            maker_fee=0.0,
            taker_fee=_safe_float(raw.get("fee")) or 0.02,
            neg_risk=raw.get("negRisk", False),
            neg_risk_market_id=raw.get("negRiskRequestID"),
            end_date=end_date,
            resolution_source="uma_oracle",
            volume_total=volume,
            liquidity=liquidity,
            outcomes=outcomes,
        )

    @staticmethod
    def normalize_event(raw: dict) -> EventSchema:
        """Convert a Gamma API event response into our canonical schema.

        Uses the API's category field when available, falls back to inference.
        Event-level volume/liquidity are numbers (unlike market-level strings).
        """
        raw_markets = raw.get("markets", [])
        markets = [GammaClient.normalize_market(m) for m in raw_markets if m.get("conditionId")]

        end_date = _parse_datetime(raw.get("endDate"))

        # Use API's category field first, then infer from tags/text
        category = raw.get("category")
        if category:
            category = category.lower()
        else:
            category = _infer_category(raw)

        # Event-level volume/liquidity are already numbers in the API
        volume = _safe_float(raw.get("volume")) or sum(m.volume_total for m in markets)
        liquidity = _safe_float(raw.get("liquidity")) or sum(m.liquidity for m in markets)

        return EventSchema(
            source=MarketSource.POLYMARKET,
            slug=raw.get("slug", ""),
            title=raw.get("title", ""),
            description=raw.get("description"),
            category=category,
            status=MarketStatus.ACTIVE if raw.get("active") else MarketStatus.CLOSED,
            end_date=end_date,
            tags=[t.get("label", "") for t in raw.get("tags", []) if isinstance(t, dict)],
            volume_total=volume,
            liquidity=liquidity,
            markets=markets,
        )


# ── Helpers ──────────────────────────────────────────────────────────


def _parse_json_string(val) -> list:
    """Parse a JSON-encoded string into a list. Returns [] on failure."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            result = json.loads(val)
            return result if isinstance(result, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _safe_float(val) -> float:
    """Safely convert a value to float. Returns 0.0 on failure."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _parse_datetime(val) -> Optional[datetime]:
    """Parse an ISO 8601 datetime string. Returns None on failure."""
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _infer_category(raw_event: dict) -> Optional[str]:
    """Infer category from tags first, then fall back to title/slug text scanning.

    Used only when the API's category field is null/empty.
    """
    # Collect tag labels
    tags = raw_event.get("tags", [])
    tag_labels = set()
    for t in tags:
        if isinstance(t, dict):
            tag_labels.add(t.get("label", "").lower())
        elif isinstance(t, str):
            tag_labels.add(t.lower())

    # Collect text from title, slug, and child market questions
    text_sources = [
        (raw_event.get("title") or "").lower(),
        (raw_event.get("slug") or "").lower(),
    ]
    for m in raw_event.get("markets", []):
        text_sources.append((m.get("question") or "").lower())
        text_sources.append((m.get("slug") or "").lower())
    combined_text = " ".join(text_sources)

    # Ordered by specificity — geopolitics before entertainment to avoid
    # "GTA VI" in geopolitics titles matching entertainment keywords
    category_map = {
        "geopolitics": {
            "tags": ["geopolitics", "war", "conflict", "international"],
            "text": ["ukraine", "russia", "china", "nato", "ceasefire", "sanctions",
                     "missile", "nuclear", "invasion", "peace deal", "middle east",
                     "israel", "iran", "north korea"],
        },
        "sports": {
            "tags": ["sports", "nfl", "nba", "mlb", "nhl", "soccer", "football",
                     "tennis", "golf", "ufc", "mma", "boxing", "f1", "racing"],
            "text": ["nfl", "nba", "mlb", "nhl", "super bowl", "world series",
                     "premier league", "champions league", "world cup",
                     "quarterback", "playoffs", "touchdown",
                     "lakers", "yankees", "cowboys", "chiefs", "eagles", "celtics",
                     "falcons", "panthers", "patriots", "49ers", "packers", "bears",
                     "ufc", "boxing", "tennis", "golf", "f1 ", "formula 1",
                     "march madness", "stanley cup"],
        },
        "politics": {
            "tags": ["politics", "elections", "government", "congress", "senate",
                     "democrat", "republican"],
            "text": ["president", "election", "congress", "senate", "governor",
                     "trump", "biden", "democrat", "republican", "gop", "dnc", "rnc",
                     "electoral", "ballot", "impeach", "scotus", "supreme court",
                     "midterm", "primary", "caucus"],
        },
        "crypto": {
            "tags": ["crypto", "bitcoin", "ethereum", "defi", "blockchain", "web3"],
            "text": ["bitcoin", "btc", "ethereum", "eth", "solana", "sol ",
                     "crypto", "defi", "nft", "blockchain", "web3",
                     "binance", "coinbase", "memecoin"],
        },
        "economics": {
            "tags": ["economics", "fed", "inflation", "gdp", "unemployment",
                     "rates", "finance", "markets"],
            "text": ["federal reserve", "fed ", "fomc", "interest rate",
                     "inflation", "cpi ", "gdp", "unemployment", "recession",
                     "tariff", "trade deal", "treasury", "stock market",
                     "s&p", "dow jones", "nasdaq", "earnings"],
        },
        "weather": {
            "tags": ["weather", "temperature", "climate"],
            "text": ["weather", "temperature", "hurricane", "tornado",
                     "earthquake", "flood", "wildfire", "high of", "degrees"],
        },
        "entertainment": {
            "tags": ["entertainment", "oscars", "emmys", "movies", "music",
                     "tv", "pop culture"],
            "text": ["oscar", "emmy", "grammy", "album", "movie", "box office",
                     "netflix", "spotify", "billboard", "gta vi", "rihanna",
                     "taylor swift", "concert"],
        },
        "science": {
            "tags": ["science", "space", "nasa", "technology", "ai"],
            "text": ["nasa", "spacex", "mars", "moon landing", "asteroid",
                     "artificial intelligence", "openai", "climate change"],
        },
    }

    # First pass: check tags
    for category, rules in category_map.items():
        if tag_labels & set(rules["tags"]):
            return category

    # Second pass: scan text
    for category, rules in category_map.items():
        for keyword in rules["text"]:
            if keyword in combined_text:
                return category

    return None
