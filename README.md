# Farsight Markets

Real-time prediction market intelligence engine. Ingests data from [Polymarket](https://polymarket.com) and [Kalshi](https://kalshi.com), computes 30 streaming features, generates ranked trading signals, and runs composable strategies with paper trading.

```
WebSocket ticks --> Event Bus --> State Engine --> Feature Engine --> Signal Engine --> Strategies --> Paper Trading
```

*The `farsight` namespace is the project name, not a dependency. This is a fully standalone package.*

## Quick Start

```bash
# Clone the repo
git clone https://github.com/LatentSpace-Labs/farsight-markets.git
cd farsight-markets

# Create environment (pick one)
conda create -n farsight-markets python=3.11 -y && conda activate farsight-markets
# or: python -m venv .venv && source .venv/bin/activate  (Windows: .venv\Scripts\activate)

# Install the package (this makes farsight.markets importable + installs all dependencies)
pip install -e ".[dev]"

# Check API connectivity
python -m farsight.markets health

# Browse top markets by 24h volume
python -m farsight.markets markets

# Analyze a specific market for signals
python -m farsight.markets analyze "will-the-fed-cut-rates"

# Run the full signal pipeline (interactive console)
python -m farsight.markets run

# Run tests
pytest tests/ -v
```

No database setup required. All data is stored locally in SQLite at `~/.farsight/markets.db`.

## What It Does

Farsight Markets connects to prediction market venues, computes quantitative features on live data, and detects trading opportunities via signal detection and composable strategies.

### Signal Types

| Signal | What it detects | Threshold |
|--------|----------------|-----------|
| **Probability Shock** | Large rapid price moves | 5% move in 5 minutes |
| **Momentum Continuation** | Sustained directional drift | 30 min+ consistent movement |
| **Mean Reversion** | Overextension from running average | 2 standard deviations |
| **Thematic Repricing** | Multiple related markets moving together | 3+ markets in same theme |
| **Structural Inconsistency** | Event outcome prices don't sum to 100% | >3% deviation |
| **Cross-Venue Divergence** | Price gap between Polymarket and Kalshi | >3% spread |

### Feature Vector (30 features)

| Family | Count | Examples |
|--------|-------|---------|
| **Microstructure** | 7 | spread, depth imbalance, trade velocity, large trade ratio |
| **Probability** | 9 | delta (1m-4h), acceleration, drift, reversion, volatility burst |
| **Quality** | 4 | liquidity score, staleness, manipulation heuristic |
| **Technicals** | 10 | RSI, Bollinger bands, volume ratio, momentum score |

### Strategies

Five composable strategies, each built as a pipeline of reusable stages:

| Strategy | Mode | Cycle | What it finds |
|----------|------|-------|---------------|
| **Opportunity Scanner** | SCAN | 5 min | Broad market scan using features + technicals |
| **Cross-Event Arb** | SCAN | 10 min | Structural mispricing in multi-outcome events |
| **Resolution Scalper** | HYBRID | 15 min | Uncertainty discounts near resolution |
| **Cross-Venue Arb** | SCAN | 15 min | Polymarket vs Kalshi price divergences |
| **Momentum Tracker** | STREAM | Every tick | Real-time momentum from WebSocket feed |

See [strategies/README.md](strategies/README.md) for detailed pipeline architecture.

## CLI Reference

```
python -m farsight.markets <command> [options]
```

| Command | Description |
|---------|-------------|
| `run` | Start the signal pipeline with interactive console |
| `health` | Test API connectivity (Gamma, CLOB, WebSocket) |
| `markets` | Browse top markets (--limit N, --order volume_24hr) |
| `events` | Browse top events |
| `market <slug>` | Market detail with outcomes and prices |
| `event <slug>` | Event detail with price consistency check |
| `book <token_id>` | L2 orderbook for a token |
| `price <token_id>` | Current price + spread |
| `features <token_id>` | Compute full feature vector from live data |
| `analyze <slug>` | Full market/event analysis with signal detection |
| `stream` | Stream live WebSocket data (--seconds N) |
| `tags` | List all Polymarket tag categories |
| `status` | Show local store stats (signals, trades, portfolio) |
| `kpi` | Realized-edge KPIs (hit rate at T+1h/4h/24h/final) |
| `sessions` | List recent bot sessions |
| `tail` | Live telemetry firehose (--kind, --strategy, --follow) |
| `dashboard` | Rich TUI: portfolio, strategies, funnel, trades, feed |
| `opps` | Page through every opportunity emitted this session |
| `trades` | Page through every trade open/close this session |
| `scans` | Page through every scan cycle with its funnel |
| `serve` | Start the REST API server on :8001 |

### Pipeline Runner

```bash
# Run all strategies (default)
python -m farsight.markets run

# Run specific strategies
python -m farsight.markets run --strategies scanner,arb

# Enable auto paper trading
python -m farsight.markets run --auto-trade

# Include real-time WebSocket streaming
python -m farsight.markets run --stream-markets 100
```

Strategy names: `scanner`, `arb`, `resolution`, `cross_venue`, `momentum`

### Live Observability

Three terminals, no server needed. Everything writes to
`~/.farsight/telemetry/<session>.jsonl`; both readers auto-switch when you
restart the runner.

```bash
# Terminal 1 — the pipeline
python -m farsight.markets run --strategies resolution --auto-trade

# Terminal 2 — event firehose (every stage drop, signal, trade, heartbeat)
python -m farsight.markets tail

# Terminal 3 — live TUI dashboard
python -m farsight.markets dashboard
```

Or launch all three at once from the **Live: Runner + Tail + Dashboard**
compound in [.vscode/launch.json](.vscode/launch.json).

### REST API

```bash
python -m farsight.markets serve --port 8001
```

Opens Swagger docs at `http://localhost:8001/docs`. All endpoints are public (no auth required):

- `GET /api/prediction-markets/markets` — Top markets by volume
- `GET /api/prediction-markets/events` — Top events
- `GET /api/prediction-markets/book?token_id=...` — L2 orderbook
- `GET /api/prediction-markets/price?token_id=...` — Current price
- `GET /api/prediction-markets/features?token_id=...` — Feature vector
- `GET /api/prediction-markets/analyze/market/{slug}` — Signal analysis
- `GET /api/prediction-markets/analyze/event/{slug}` — Event analysis

## Architecture

```
farsight/markets/
├── clients/              # API clients for prediction market venues
│   ├── polymarket/       #   Gamma (metadata), CLOB (orderbook/prices), WebSocket (streaming)
│   └── kalshi/           #   REST API client
├── engine/               # Core streaming infrastructure
│   ├── event_bus.py      #   Async pub/sub (raw.*, derived.*, signal.*, trade.*)
│   ├── window.py         #   RollingWindow, VolumeWeightedWindow (time-series buffers)
│   └── checkpoint.py     #   WebSocket resumption (memory + SQLite stores)
├── features/             # 30 streaming features across 4 families
│   ├── microstructure.py #   Spread, depth imbalance, trade velocity
│   ├── probability.py    #   Delta, acceleration, drift, reversion
│   ├── quality.py        #   Liquidity score, staleness, manipulation heuristic
│   └── technicals.py     #   RSI, Bollinger, volume ratio, momentum
├── schemas/              # Pydantic data models (the canonical contract)
│   ├── events.py         #   EventSchema, MarketSchema, OutcomeSchema
│   ├── signals.py        #   SignalSchema, SignalType, Direction, SignalEvidence
│   └── ticks.py          #   PriceTickSchema, TradeSchema, OrderbookSchema
├── services/             # Stateful processing engines
│   ├── state_engine.py   #   MarketState — rolling tick/trade buffers + derived state
│   ├── feature_engine.py #   Computes features from state
│   ├── signal_engine.py  #   6 signal detectors + multi-stage filter chain
│   ├── theme_service.py  #   Maps markets to themes/sectors/tickers via keyword rules
│   ├── correlation_service.py  # Cross-asset correlation (optional external data)
│   └── replay_service.py #   Historical backtesting
├── strategies/           # 5 composable trading strategies
│   ├── base.py           #   Strategy, Source, Enricher, Analyzer, Scorer protocols
│   ├── config.py         #   Shared StrategyConfig (scope/thresholds/risk/scheduling)
│   ├── types.py          #   Leg / Signal / Order / Fill — the unified trade types
│   ├── opportunity_scanner.py
│   ├── cross_event_arb.py
│   ├── resolution_scalper.py
│   ├── cross_venue_arb.py
│   ├── momentum_tracker.py
│   └── venue_matcher.py  #   Cross-venue market matching (Polymarket <-> Kalshi)
├── policy.py             # Signal → Order gate (Kelly sizing, caps, dedup)
├── executor.py           # Order → Fill (paper trades, portfolio updates)
├── telemetry.py          # JSONL session writer (live event stream)
├── telemetry_tail.py     # `tail` CLI — live event firehose
├── telemetry_dashboard.py# `dashboard` CLI — rich TUI
├── routes/               # FastAPI REST endpoints
│   └── explore_routes.py #   Discovery, orderbook, features, analysis (public, no auth)
├── core/                 # Standalone utilities
│   └── auth.py           #   Pluggable auth dependency (default: no auth)
├── config.py             # All settings (PM_ env prefix), env-var overridable
├── store.py              # SQLite persistence (~/.farsight/markets.db)
├── runner.py             # Pipeline orchestrator (strategies + paper trading)
├── app.py                # Standalone FastAPI server (:8001)
└── cli.py                # CLI entry point
```

Per-strategy config lives at `config/strategies/<name>.yaml`
(e.g. [config/strategies/resolution.yaml](config/strategies/resolution.yaml)).

### Processing Pipeline

```
Layer 1 — Raw Ingestion
  WebSocket ticks / REST polling
  → Normalized PriceTickSchema, TradeSchema, OrderbookSchema
  → Published to event bus: raw.price_tick, raw.trade_print, raw.orderbook

Layer 2 — State & Features
  StateEngine: maintains rolling windows per market (up to 10K entries)
  FeatureEngine: computes 30 features on demand from state
  → Published to event bus: derived.state_update, derived.features

Layer 3 — Signals & Strategies
  SignalEngine: 6 rule-based detectors + filter chain (cooldown, dedup, liquidity)
  Strategies: Source → Enrich → Analyze → Score (rules + quality gates inline)
  → Emit Opportunity / Signal (unified type; baskets supported via Signal.legs)

Layer 4 — Policy & Execution
  Policy: Signal → Order. Owns Kelly sizing, max_concurrent_positions,
          duplicate-market dedup. Rejections telemetered with reason.
  Executor: Order → Fill. Writes paper trade, updates portfolio,
            emits trade.open / trade.close / portfolio events.

Layer 5 — Delivery & Observability
  Console display, REST API, SQLite persistence.
  Telemetry: one JSONL per session at ~/.farsight/telemetry/<session>.jsonl.
  `farsight tail`      — live, colour-coded event firehose.
  `farsight dashboard` — rich TUI: portfolio, strategies, last-scan funnel,
                         opportunities, trades, signals, event feed.
```

### Event Bus Topics

| Topic | Publisher | Subscribers |
|-------|-----------|-------------|
| `raw.price_tick` | WebSocket client | StateEngine |
| `raw.trade_print` | WebSocket client | StateEngine |
| `raw.orderbook` | WebSocket client | StateEngine |
| `derived.state_update` | StateEngine | FeatureEngine, MomentumTracker |
| `derived.features` | FeatureEngine | SignalEngine |
| `signal.generated` | SignalEngine | Runner (display + trade) |
| `trade.executed` | Runner | Console output |

## Configuration

All settings use the `PM_` environment variable prefix and can be set in a `.env` file.

### Key Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `PM_POLYMARKET_GAMMA_URL` | `https://gamma-api.polymarket.com` | Gamma API base URL |
| `PM_POLYMARKET_CLOB_URL` | `https://clob.polymarket.com` | CLOB API base URL |
| `PM_POLYMARKET_WS_URL` | `wss://ws-subscriptions-clob...` | WebSocket URL |
| `PM_KALSHI_API_KEY` | (none) | Kalshi API key (optional) |
| `PM_SIGNAL_PROBABILITY_SHOCK_DELTA` | `0.05` | Shock detection threshold (5%) |
| `PM_FILTER_MIN_CONFIDENCE` | `0.4` | Minimum signal confidence |
| `PM_FILTER_COOLDOWN_MINUTES` | `30` | Signal cooldown per market |
| `PM_PAPER_STARTING_BALANCE` | `10000` | Paper trading starting balance ($) |
| `PM_PAPER_DEFAULT_KELLY_FRACTION` | `0.15` | Kelly criterion fraction |

See [config.py](config.py) for the full list of ~50 tunable parameters.

## Storage

All persistence uses **SQLite** at `~/.farsight/markets.db` (auto-created on first use). No database setup, no migrations.

Tables:
- `signals` — generated signal history
- `paper_portfolio` — portfolio balance and risk settings
- `paper_trades` — simulated trade log with P&L
- `subscriptions` — watched markets/categories
- `config` — key-value settings
- `event_log` — audit trail

## Data Sources

| Source | Auth Required | What It Provides |
|--------|--------------|------------------|
| Polymarket Gamma API | No | Market metadata, events, categories, tags |
| Polymarket CLOB API | No | Orderbooks, prices, price history |
| Polymarket WebSocket | No | Real-time price ticks, trades, orderbook updates |
| Kalshi REST API | Yes (API key) | Market listings, prices (optional venue for cross-venue arb) |
| Goldsky Subgraphs | No | On-chain positions, activity, PnL (GraphQL) |

See [VALIDATED_ASSUMPTIONS.md](VALIDATED_ASSUMPTIONS.md) for tested API behaviors and known quirks.

## Development

### Running Tests

```bash
pytest tests/ -v
```

221 unit tests covering clients, engine, features, services, strategies, and schemas.

### Project Dependencies

Core (required):
- `httpx` — async HTTP client
- `websockets` — WebSocket streaming
- `pydantic` + `pydantic-settings` — data validation and configuration
- `uuid-utils` — UUID v7 generation

Optional:
- `gql[httpx]` — Goldsky on-chain data queries
- `fastapi` + `uvicorn` — REST API server (only for `serve` command)
- `rich` — TUI dashboard (only for `dashboard` command)
- `pyyaml` — strategy config files

### Adding a New Strategy

A strategy owns `source → enrich → analyze → score` (rules + quality gates
live in the scorer; no separate Filter stage). It emits `Opportunity`s
(or `Signal`s natively); the runner funnels them through **Policy** (sizing,
caps, dedup) and **Executor** (paper trade). Strategies never touch the
store or portfolio directly.

```python
from farsight.markets.strategies.base import Strategy, StrategyMode
from farsight.markets.strategies.config import StrategyConfig

class MyStrategy(Strategy):
    name = "my_strategy"
    mode = StrategyMode.SCAN

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.source = MySource(...)
        self.analyzer = MyAnalyzer(...)
        self.scorer = MyScorer(thresholds=config.thresholds)

    async def scan(self) -> list[Opportunity]:
        ctxs = await self.source.fetch()
        analyzed = [self.analyzer.analyze(c) for c in ctxs]
        return [o for c in analyzed for o in self.scorer.score(c)]
```

Config lives in `config/strategies/<name>.yaml`. See
[config/strategies/resolution.yaml](config/strategies/resolution.yaml) for
scope / thresholds / risk / scheduling sections. Load with
`MyConfig.load("my_strategy")`.

### Adding a New Feature

Features are pure functions that take a `MarketState` and return a float:

```python
# In features/my_family.py
from farsight.markets.services.state_engine import MarketState

def my_feature(state: MarketState) -> float | None:
    """Compute something from the rolling price/trade windows."""
    if not state.ticks:
        return None
    # ... computation ...
    return value
```

Register it in `services/feature_engine.py` to include it in the feature vector.

### Adding a New Signal Detector

Signal detectors check features against thresholds:

```python
# In services/signal_engine.py, add to the detector list
def _detect_my_signal(self, features: dict, state: MarketState) -> SignalSchema | None:
    if features.get("my_feature", 0) > threshold:
        return SignalSchema(
            signal_type=SignalType.MY_TYPE,
            direction=Direction.BULLISH,
            confidence=0.7,
            # ...
        )
    return None
```

## Integration

### As a Library

```python
from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.services.state_engine import MarketState
from farsight.markets.services.feature_engine import compute_features

# Fetch market data
client = GammaClient()
markets = await client.get_markets(limit=10, order="volume_24hr")

# Compute features on a market
state = MarketState(token_id="...")
# ... feed ticks into state ...
features = compute_features(state)
```

### With Authentication (for multi-user deployments)

```python
from farsight.markets.core.auth import set_auth_dependency

# Inject your own auth dependency
set_auth_dependency(my_app_get_current_user)
```

The REST API routes will then use your auth instead of the default no-op.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, code style, and PR guidelines.

## Disclaimer

This software is for **informational and educational purposes only**. It does not constitute financial advice. No real money is traded — all trading functionality is simulated (paper trading). Users are responsible for compliance with applicable laws and platform terms of service in their jurisdiction.

The authors are not affiliated with Polymarket, Kalshi, or any prediction market platform.

## License

MIT — see [LICENSE](LICENSE) for details.
