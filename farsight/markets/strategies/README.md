# Prediction Markets — Strategy Architecture

## Overview

Each strategy is a **composable pipeline** of reusable stages:

```
Source → Enricher(s) → Analyzer(s) → Scorer → [Opportunity]
                                                    │
                                               Policy (Kelly, caps, dedup)
                                                    │
                                               Executor (paper trade + portfolio)
```

Stages are independent classes with single-method interfaces:
- `Source.fetch() → [MarketContext]` — fetch raw market data
- `Enricher.enrich(ctx) → ctx` — add orderbook, history, themes
- `Analyzer.analyze(ctx) → ctx` — compute features, detect patterns
- `Scorer.score(ctx) → [Opportunity]` — apply rules + quality gates inline;
  emit only what passes

There is no separate `Filter` stage any more. Quality gates (`min_edge`,
`min_confidence`, `min_liquidity`, `max_spread`) are part of the scorer —
it either emits an Opportunity that meets the rules, or nothing. Portfolio-
level concerns (sizing, position caps, dedup against open trades) live in
`policy.py`, not in strategies.

Each stage can be reused independently — by other strategies, by API endpoints, or as tools in your own applications.

## Strategy Modes

| Mode | Behavior | When it runs |
|------|----------|-------------|
| **SCAN** | Fetch → analyze → return. Called periodically. | Every N seconds by the runner |
| **STREAM** | Wired to event bus. Reacts to live ticks. | On every state update (real-time) |
| **HYBRID** | Scan to find candidates, stream for timing. | Both |

## Strategies

### 1. OpportunityScanner (`scanner`) — SCAN mode, every 5 min

Broad market scanner. Finds opportunities via features and technicals.

```
TopMarketsSource (Gamma API, top 50 by 24h volume)
  ├── Screens: no crypto noise, price 5-95%, min volume, min liquidity, max spread
  │
  → OrderbookEnricher (CLOB API /book)
  │   Adds: real bid/ask, spread, depth, depth imbalance
  │
  → PriceHistoryEnricher (CLOB API /prices-history, 2h of 1m candles)
  │   Adds: 120 price points into rolling windows
  │
  → FeatureAnalyzer (30 features)
  │   ├── Microstructure: spread, depth imbalance, trade imbalance, velocity
  │   ├── Probability: deltas (1m-4h), acceleration, drift, reversion, vol burst
  │   ├── Quality: liquidity score, staleness, manipulation heuristic
  │   └── Technicals: RSI, Bollinger, volume ratio, momentum score, percentile
  │
  → ThemeAnalyzer
  │   Maps: "Fed rate cut" → theme=monetary_policy, tickers=[TLT, SPY, DXY]
  │
  → SignalScorer (two paths)
  │   ├── Signal detectors (high bar): shock (5% in 5m), momentum, reversion
  │   └── Feature-based (lower bar):
  │       ├── 1h momentum > 2% with trading activity
  │       ├── Depth imbalance > 60% one-sided
  │       ├── VWAP reversion > 1.5σ
  │       ├── RSI crossing 70/30 boundaries
  │       ├── Volume surge (>2x) + momentum alignment
  │       └── Bollinger breakout (price outside bands)
  │
  → QualityFilter (min edge 1%, min liquidity $3K)
```

### 2. CrossEventArb (`arb`) — SCAN mode, every 10 min

Structural mispricing in multi-outcome events.

```
ActiveEventsSource (Gamma API /events, top 30 by 24h volume)
  ├── Filter: must have 2+ child markets
  │
  → StructuralAnalyzer
  │   Sum all outcome prices. Should be ~100%.
  │   Flag: overpriced (>102%) or underpriced (<98%)
  │
  → ArbScorer
  │   Overpriced: SELL the most expensive candidate
  │   Underpriced: BUY the leader
  │   Edge = deviation / num_outcomes
  │   Confidence scales with deviation size
  │
  → QualityFilter (min deviation 2%, min liquidity $5K)
```

**Mathematical edge:** Prices MUST converge to 100% at resolution.

### 3. ResolutionScalper (`resolution`) — HYBRID mode, every 15 min

Uncertainty discounts near market resolution.

```
NearResolutionSource (Gamma API, markets resolving within 14 days)
  ├── Sort: soonest resolution first
  │
  → OrderbookEnricher (CLOB API /book)
  │
  → ResolutionAnalyzer
  │   Estimate fair value based on time decay:
  │     90% with 7 days left → fair value ~92%
  │     90% with 1 day left  → fair value ~97%
  │   Only markets with one outcome > 75% certainty
  │
  → DiscountScorer (rules + quality gates inline)
  │   Edge = fair_value - current_price
  │   Direction: BUY the leading outcome at a discount
  │   Gates: min_edge, min_confidence, min_liquidity, max_spread
  │          (all configured in config/strategies/resolution.yaml)
```

**Monitor:** Closes at 97%+ (take profit) or entry-5% (stop loss).

### 4. CrossVenueArbitrage (`cross_venue`) — SCAN mode, every 15 min

Price divergences between Polymarket and Kalshi.

```
DualVenueSource
  ├── Polymarket: top 50 markets (Gamma API)
  └── Kalshi: all open markets (REST API)

  → VenueMatcher (match equivalent markets across venues)
  │   Priority 1: Manual mapping table (Fed, BTC, elections — 90% confidence)
  │   Priority 2: Keyword overlap with entity boosting (30-85% confidence)
  │   Deduplication: keep best match per Polymarket market
  │
  → DivergenceScoring
  │   Spread = |polymarket_price - kalshi_price|
  │   Direction: buy on the cheaper venue
  │
  → QualityFilter (min spread 3%, min match confidence 50%)
```

**Risk:** Match confidence matters. If questions are slightly different, the "arb" doesn't exist.

### 5. MomentumTracker (`momentum`) — STREAM mode, continuous

Real-time momentum detection from WebSocket feed.

```
WebSocket tick → StateEngine → FeatureEngine → MomentumTracker.on_state_update()

  Detects (on every tick):
  │
  ├── Multi-timeframe alignment
  │   5m, 15m, 1h all moving same direction + accelerating
  │   Confidence boosted if shorter timeframes are stronger
  │
  ├── RSI crossings
  │   RSI crossing above 70 (entering overbought) → SELL signal
  │   RSI crossing below 30 (entering oversold) → BUY signal
  │   Only fires on the CROSSING, not while extreme
  │
  └── Volume surge + momentum
      >3x normal volume + momentum > 0.2 in same direction
      Indicates informed trading

  Cooldown: 5 min per token (don't spam signals)
```

**Monitor:** Closes when momentum reverses or 3% stop loss.

## Data Flow

```
                 SCAN STRATEGIES                    STREAM STRATEGY
                 (every N min)                      (every tick)
                      │                                  │
    ┌─────────────────┼─────────────────┐               │
    │                 │                 │               │
Scanner           Arb            Resolution         Momentum
    │                 │                 │               │
    │  Gamma API      │  Gamma API      │  Gamma API    │  WebSocket
    │  CLOB API       │                 │  CLOB API     │  → StateEngine
    │  (orderbook,    │                 │  (orderbook)  │  → FeatureEngine
    │   history)      │                 │               │  → on_state_update()
    │                 │                 │               │
    ▼                 ▼                 ▼               ▼
    └─────────────────┴─────────────────┴───────────────┘
                              │
                    [Opportunity, Opportunity, ...]
                              │
                    Deduplicate by market_id
                    Rank by score (edge × confidence × liquidity)
                              │
                    ┌─────────┴──────────┐
                    │                    │
              Display in             Signal.from_opportunity
              console                       │
                                         Policy (policy.py)
                                         ├─ Kelly sizing
                                         ├─ max_concurrent_positions
                                         ├─ duplicate-market dedup
                                         └─ min_order_usd floor
                                                │
                                         Order (or telemetered reject)
                                                │
                                         Executor (executor.py)
                                         ├─ Paper trade write
                                         ├─ Portfolio update
                                         └─ trade.open telemetry
```

## Reusable Stage Classes

These stages can be used independently in your own code:

| Stage | Import | Use case |
|-------|--------|---------------|
| `TopMarketsSource` | `strategies.opportunity_scanner` | "What are the top prediction markets?" |
| `ActiveEventsSource` | `strategies.cross_event_arb` | "What multi-outcome events are active?" |
| `NearResolutionSource` | `strategies.resolution_scalper` | "What resolves this week?" |
| `OrderbookEnricher` | `strategies.opportunity_scanner` | "What's the orderbook depth?" |
| `FeatureAnalyzer` | `strategies.opportunity_scanner` | "Compute features for this market" |
| `ThemeAnalyzer` | `strategies.opportunity_scanner` | "What tickers relate to this event?" |
| `StructuralAnalyzer` | `strategies.cross_event_arb` | "Is this event mispriced?" |
| `ResolutionAnalyzer` | `strategies.resolution_scalper` | "What's the fair value?" |
| `VenueMatcher` | `strategies.venue_matcher` | "Do Polymarket and Kalshi agree?" |

## Feature Vector (30 features)

| Family | Features | Module |
|--------|----------|--------|
| Microstructure (7) | spread_pct, depth_imbalance, trade_imbalance_5m/1h, quote_velocity, trade_velocity, large_trade_ratio | `features/microstructure.py` |
| Probability (9) | delta_1m/5m/15m/1h/4h, acceleration, drift_score, reversion_score, volatility_burst | `features/probability.py` |
| Quality (4) | liquidity_score, stale_score, manipulation_heuristic, resolution_proximity_days | `features/quality.py` |
| Technicals (10) | rsi_1h/4h, bollinger_position/width, volume_ratio, volume_price_divergence, rate_of_change, momentum_score, distance_from_midpoint, price_percentile | `features/technicals.py` |
