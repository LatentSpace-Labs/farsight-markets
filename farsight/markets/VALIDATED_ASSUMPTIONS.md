# Validated Assumptions — Event Markets Platform

Document what we assumed vs. what actually works after testing the APIs.
Updated as we discover discrepancies.

## Polymarket Gamma API

| Assumption | Status | Notes |
|------------|--------|-------|
| Public, no auth needed | **CONFIRMED** | All GET endpoints work without auth |
| Endpoint for market by slug is `/markets/{slug}` | **FALSE** | Correct endpoint is `/markets/slug/{slug}`. Same pattern for events: `/events/slug/{slug}` |
| Returns JSON with outcomePrices as array | **FALSE** | `outcomePrices`, `clobTokenIds`, `outcomes`, `umaResolutionStatuses` are all **JSON-encoded strings**, not arrays. Must `json.loads()` them |
| Volume and liquidity are numeric | **MIXED** | On Event objects: numbers. On Market objects: **strings**. Use `volumeNum`/`liquidityNum` (number fields) instead |
| Markets have bestBid/bestAsk/spread | **CONFIRMED** | Available as number fields: `bestBid`, `bestAsk`, `spread`, `lastTradePrice`, `oneHourPriceChange`, `oneDayPriceChange`, etc. |
| Events have a `category` field | **CONFIRMED** | String field (e.g., "Sports") but **not always populated**. Many events have null category. Fall back to tag/text inference |
| `tag_id` param works for filtering | **CONFIRMED** | Integer tag IDs from `GET /tags`. Primary way to filter by category |
| `order` param for server-side sorting | **CONFIRMED** | Supports: `volume`, `volume_24hr`, `liquidity`, `competitive`, `end_date`, `closed_time` |
| Pagination via offset/limit | **CONFIRMED** | Default 100 per page. No total count — paginate until `len(batch) < limit` |
| `id` fields are UUIDs | **FALSE** | `id` is a **numeric string** (e.g., `"23784"`), not a UUID |
| endDate is ISO 8601 | **CONFIRMED** | UTC with Z suffix |
| `outcomes` field contains labels | **CONFIRMED** | JSON-encoded string array: `'["Yes", "No"]'` or `'["Trump", "Biden"]'` |
| `order=volume` returns trending markets | **FALSE** | Returns all-time volume including old resolved markets. Use `order=volume_24hr` for trending |
| `order=volume_24hr` works | **CONFIRMED** | Returns genuinely active markets sorted by 24h volume. Use with `active=true&closed=false` |
| `order=competitive` returns good markets | **FALSE** | Returns 5-min crypto spam (all score 1.0 for 50/50 markets) |
| `closed=false` default | **CONFIRMED** | Default is `false` but should be set explicitly for clarity |
| `exclude_tag_id` works | **PARTIALLY** | Works on `/events` but not reliably on `/markets` for filtering crypto spam |
| `groupItemTitle` has candidate names | **CONFIRMED** | For neg-risk events (elections), the candidate name is in `groupItemTitle`, not `outcomes` |

## Polymarket CLOB API

| Assumption | Status | Notes |
|------------|--------|-------|
| GET /book is public | **CONFIRMED** | Returns full L2 orderbook without auth |
| GET /price is public | **FAILS for some tokens** | Returns 400 for certain token IDs. Implemented fallback: compute mid from /book |
| GET /midpoint is public | **NEEDS TESTING** | May have same issue as /price. Fallback implemented |
| GET /spread is public | **NEEDS TESTING** | May have same issue. Fallback implemented |
| GET /trades is public for reads | **FALSE** | Returns 401 Unauthorized. Requires CLOB API key auth. Trade data for unauthenticated use comes from WebSocket `last_trade_price` events or Goldsky subgraph |
| GET /prices-history is public | **NEEDS TESTING** | |

## Polymarket WebSocket

| Assumption | Status | Notes |
|------------|--------|-------|
| Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market | **NEEDS TESTING** | Via /explore/stream-test endpoint |
| Pushes book, price_change, last_trade_price events | **NEEDS TESTING** | |
| Messages come as JSON arrays | **NEEDS TESTING** | |

## Kalshi API

| Assumption | Status | Notes |
|------------|--------|-------|
| GET /markets is public | **FALSE** | Returns 401 Unauthorized. Requires API key auth. |
| GET /events is public | **NEEDS TESTING** | May also require auth |
| GET /exchange/status is public | **CONFIRMED** | Returns exchange status without auth |
| GET /markets/trades is public | **NEEDS TESTING** | |
| Demo env at demo-api.kalshi.co | **NOT TESTED** | May have different auth requirements |

## Data Quality Observations

| Observation | Date | Notes |
|-------------|------|-------|
| Top Gamma markets include meme/joke markets | 2026-04-10 | "Will Jesus Christ return before GTA VI?" at $10.9M volume. Need category filtering for signal generation |
| Token IDs are very large uint256 strings | 2026-04-10 | 78+ digits. Must handle as strings, never parse as int |
| Some markets have null volume/liquidity | 2026-04-10 | Must default to 0.0 |
| Many events have empty tags AND null category | 2026-04-10 | Text-based inference needed for ~40% of events. Our inference covers sports, politics, crypto, economics, weather, geopolitics, entertainment, science |
| API returns old resolved markets as "active" | 2026-04-10 | NFL market from 2021 still returned with `active: true`. Check `endDate` and `closed` fields |
| Event `volume`/`liquidity` are numbers, Market `volume`/`liquidity` are strings | 2026-04-10 | Use `volumeNum`/`liquidityNum` on Market objects |
