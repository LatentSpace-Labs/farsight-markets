# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Install (editable, with dev extras):
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest tests/ -v                          # all tests
pytest tests/test_signal_engine.py -v     # single file
pytest tests/ -k "test_momentum" -v       # by pattern
```

Common CLI entry points (module is `farsight.markets`):
```bash
python -m farsight.markets health                   # check API connectivity
python -m farsight.markets markets                  # browse top markets
python -m farsight.markets analyze <slug>           # signal analysis for a market
python -m farsight.markets run [--strategies ...] [--auto-trade] [--stream-markets N]
python -m farsight.markets serve --port 8001       # FastAPI server, /docs for Swagger
```

Strategy names for `run --strategies`: `scanner`, `arb`, `resolution`, `cross_venue`, `momentum`.

There is no linter/formatter configured. Python 3.11+ required. `pytest-asyncio` is in `asyncio_mode = "auto"` — async tests do not need explicit marks.

## Architecture

Farsight Markets is a **standalone package** despite the `farsight.*` namespace (it is the project name, not an external dependency). All source lives under [farsight/markets/](farsight/markets/).

### Four-layer streaming pipeline

Data flows through an in-process async event bus. Each layer publishes to topics the next layer subscribes to:

```
Layer 1 Raw Ingestion      WebSocket/REST -> normalized schemas -> raw.price_tick, raw.trade_print, raw.orderbook
Layer 2 State & Features   StateEngine (rolling windows) -> FeatureEngine -> derived.state_update, derived.features
Layer 3 Signals/Strategies SignalEngine (6 detectors + filter chain) + Strategies -> signal.generated
Layer 4 Delivery           Runner dispatches to console, paper trading, REST, SQLite
```

The event bus ([engine/event_bus.py](farsight/markets/engine/event_bus.py)) is the spine — most components do not call each other directly; they publish/subscribe. When adding functionality, prefer wiring through a topic over direct coupling.

### Key module boundaries

- [clients/](farsight/markets/clients/) — venue API clients. Polymarket split into `gamma_client` (metadata), `clob_client` (orderbook/prices), `ws_client` (streaming). Kalshi is REST-only.
- [engine/](farsight/markets/engine/) — transport-agnostic infrastructure: event bus, rolling windows (`RollingWindow`, `VolumeWeightedWindow`), WebSocket checkpoint/resume.
- [schemas/](farsight/markets/schemas/) — **the canonical contract.** Pydantic models for ticks, events, signals. All cross-module data travels as these schemas; normalize at the client boundary.
- [services/](farsight/markets/services/) — stateful engines. `StateEngine` maintains per-market `MarketState` (rolling tick/trade buffers, up to 10K entries). `FeatureEngine` computes the 30-feature vector on demand. `SignalEngine` runs 6 detectors through a filter chain (cooldown, dedup, liquidity).
- [features/](farsight/markets/features/) — 30 features split across 4 families (microstructure, probability, quality, technicals). Each feature is a **pure function** `(MarketState) -> float | None`. Register new features in [services/feature_engine.py](farsight/markets/services/feature_engine.py).
- [strategies/](farsight/markets/strategies/) — composable pipelines built from stage protocols in [strategies/base.py](farsight/markets/strategies/base.py): `Source -> Enricher -> Analyzer -> Scorer -> Filter`. Each strategy declares a `StrategyMode` (SCAN/STREAM/HYBRID) and `interval_seconds`.
- [routes/](farsight/markets/routes/) — FastAPI endpoints (public by default).
- [core/auth.py](farsight/markets/core/auth.py) — pluggable auth dependency. Default is no-op; embedders call `set_auth_dependency(...)` to inject their own.
- [config.py](farsight/markets/config.py) — Pydantic settings, all env vars prefixed `PM_`, loadable from `.env`.
- [store.py](farsight/markets/store.py) — SQLite persistence at `~/.farsight/markets.db` (auto-created, no migrations).
- [runner.py](farsight/markets/runner.py) — orchestrator; wires strategies + paper trading onto the bus.
- [app.py](farsight/markets/app.py) / [cli.py](farsight/markets/cli.py) — server and CLI entry points.

### Extension patterns (from the README)

- **New feature**: pure function in `features/<family>.py`, then register in `services/feature_engine.py`.
- **New signal detector**: add `_detect_*` method in `services/signal_engine.py`, return `SignalSchema | None`.
- **New strategy**: subclass `Strategy` in `strategies/`, implement `build_pipeline()` returning ordered stage instances.

## Code style (from CONTRIBUTING.md)

- Type hints required, `async/await` for all I/O.
- Services take explicit dependencies — no external state mutation.
- Pydantic for data crossing boundaries; dataclasses for internal types.
- Tests must be self-contained and mock external APIs (no network).
- Keep dependencies light — avoid heavy additions.

## Out of scope

Live trading with real money and UI/frontend code are explicitly out of scope — paper trading only.
