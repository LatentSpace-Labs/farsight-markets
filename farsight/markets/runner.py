"""
Farsight Prediction Markets Runner — strategy-based intelligence bot.

Runs multiple strategies in parallel:
  - Scan strategies: periodic opportunity detection
  - Stream strategies: real-time reaction to market data
  - Hybrid strategies: scan + stream

Each strategy produces Opportunities, which are ranked, filtered, and
optionally auto-traded via paper execution.

Usage:
    python -m farsight.markets run
    python -m farsight.markets run --auto-trade
    python -m farsight.markets run --strategies scanner,arb
"""

import asyncio
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from farsight.markets.clients.polymarket.clob_client import ClobClient
from farsight.markets.clients.polymarket.gamma_client import GammaClient
from farsight.markets.clients.polymarket.ws_client import PolymarketWsClient
from farsight.markets.config import settings
from farsight.markets.engine.checkpoint import MemoryCheckpointStore
from farsight.markets.engine.event_bus import EventBus
from farsight.markets.services.feature_engine import FeatureEngine
from farsight.markets.services.outcome_tracker import OutcomeTracker
from farsight.markets.services.session_service import SessionService
from farsight.markets.services.signal_engine import SignalEngine
from farsight.markets.services.state_engine import StateEngine
from farsight.markets.store import LocalStore
from farsight.markets.strategies.base import Action, ActionType, Opportunity, Strategy, StrategyMode

logger = logging.getLogger(__name__)

ORANGE = "\033[38;5;208m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
WHITE = "\033[97m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"

LOGO = (
    f"\n"
    f"  {ORANGE}    ______              _       __    __\n"
    f"   / ____/___ ________ (_)___ _/ /_  / /_\n"
    f"  / /_  / __ `/ ___/ ___/ / __ `/ __ \\/ __/\n"
    f" / __/ / /_/ / /  (__  ) / /_/ / / / / /_\n"
    f"/_/    \\__,_/_/  /____/_/\\__, /_/ /_/\\__/\n"
    f"                        /____/{RESET}\n"
    f"  {DIM}Prediction Markets{RESET} {DIM}v0.1{RESET}\n"
)




class PipelineRunner:
    """Strategy-based prediction markets intelligence bot."""

    def __init__(
        self,
        strategies: list[str] | None = None,
        auto_trade: bool = False,
        max_trades_per_scan: int = 3,
        stream_markets: int = 20,
        resolution_config=None,
    ):
        from farsight.markets.strategies.resolution_scalper import ResolutionConfig
        self.auto_trade = auto_trade
        self.max_trades_per_scan = max_trades_per_scan
        self.stream_markets = stream_markets
        self._resolution_config = resolution_config or ResolutionConfig.load("resolution")
        self._running = False
        self._start_time: Optional[float] = None

        # Infrastructure
        self.store = LocalStore()
        self.bus = EventBus()
        self.checkpoint = MemoryCheckpointStore()
        self.gamma = GammaClient()
        self.clob = ClobClient()
        self.ws = PolymarketWsClient(self.bus, self.checkpoint)
        self.state_engine = StateEngine(event_bus=self.bus)
        self.feature_engine = FeatureEngine(self.state_engine, event_bus=self.bus)

        # Phase 0: session + label-loop infrastructure
        self.session = SessionService(self.store)
        self.outcome_tracker = OutcomeTracker(self.store, clob=self.clob, gamma=self.gamma)
        self.signal_engine = SignalEngine(
            event_bus=self.bus,
            on_emit=self._persist_and_track_signal,
        )

        # Strategies
        self._strategies: list[Strategy] = []
        self._requested_strategies = strategies or ["scanner", "arb", "resolution"]
        self._load_strategies(self._requested_strategies)

        # Tracking
        self.all_opportunities: list[Opportunity] = []
        self.token_to_question: dict[str, str] = {}

        # View mode
        self._active_view: Optional[str] = None

    def _load_strategies(self, names: list[str]):
        """Load strategy instances by name."""
        from farsight.markets.strategies.opportunity_scanner import OpportunityScanner
        from farsight.markets.strategies.cross_event_arb import CrossEventArb
        from farsight.markets.strategies.resolution_scalper import ResolutionScalper
        from farsight.markets.strategies.cross_venue_arb import CrossVenueArbitrage
        from farsight.markets.strategies.momentum_tracker import MomentumTracker
        from farsight.markets.clients.kalshi.rest_client import KalshiClient

        kalshi = KalshiClient()
        registry = {
            "scanner": lambda: OpportunityScanner(self.gamma, self.clob),
            "arb": lambda: CrossEventArb(self.gamma, self.clob),
            "resolution": lambda: ResolutionScalper(
                self.gamma, self.clob, config=self._resolution_config,
            ),
            "cross_venue": lambda: CrossVenueArbitrage(self.gamma, kalshi),
            "momentum": lambda: MomentumTracker(self.state_engine),
        }

        for name in names:
            factory = registry.get(name)
            if factory:
                self._strategies.append(factory())
            else:
                logger.warning(f"Unknown strategy: {name}")

    async def start(self):
        """Start the bot."""
        self._running = True
        self._start_time = time.time()

        print(LOGO)
        mode = f"{GREEN}AUTO-TRADE{RESET}" if self.auto_trade else f"{CYAN}MONITOR{RESET}"
        strat_names = ", ".join(s.name for s in self._strategies)
        portfolio = self.store.get_portfolio()

        # Phase 0: open a session and perform warm-start of the signal filter.
        session_id = self.session.start(settings, self._requested_strategies, self.auto_trade)
        self.signal_engine.set_session_id(session_id)
        self._warm_start_signal_filter()
        self.signal_engine.begin_warmup(settings.WARMUP_SECONDS)

        # Telemetry: one JSONL per session; readers (tail/dashboard) tail it.
        from farsight.markets import telemetry
        from farsight.markets.policy import Policy, PolicyConfig
        from farsight.markets.executor import Executor, SessionRef
        self._telemetry = telemetry.TelemetrySink(session_id)
        telemetry.set_sink(self._telemetry)
        # Policy caps come from the resolution strategy's risk block when it's
        # the only active strategy; otherwise fall back to conservative defaults.
        # (A proper per-strategy policy routing comes when more strategies are
        # migrated to the new shape.)
        rc = self._resolution_config
        active_only_resolution = list(self._strategies) and all(
            s.name == "resolution" for s in self._strategies
        )
        if active_only_resolution:
            policy_cfg = PolicyConfig(
                max_concurrent_positions=rc.risk.max_positions,
                kelly_fraction=rc.risk.kelly_fraction,
                max_position_usd=rc.risk.max_position_usd,
            )
        else:
            policy_cfg = PolicyConfig(
                max_concurrent_positions=20,
                kelly_fraction=portfolio.get("kelly_fraction", 0.15),
                max_position_pct=portfolio.get("max_position_pct", 5.0),
            )
        self._policy = Policy(self.store, policy_cfg)
        self._executor = Executor(self.store, SessionRef(session_id=session_id))
        self._telemetry.emit(
            "session.start",
            strategies=self._requested_strategies,
            auto_trade=self.auto_trade,
            portfolio_balance=portfolio["current_balance"],
        )
        self._wire_bus_telemetry()

        print(f"  Mode:       {mode}")
        print(f"  Strategies: {strat_names}")
        print(f"  Portfolio:  ${portfolio['current_balance']:,.2f}")
        print(f"  Session:    {DIM}{session_id[:8]}{RESET}  "
              f"{DIM}(warmup {settings.WARMUP_SECONDS}s){RESET}")
        print(f"  Store:      {DIM}{self.store.db_path}{RESET}")
        print()

        # Wire streaming pipeline (for hybrid/stream strategies)
        self.state_engine.wire(self.bus)
        self.feature_engine.wire(self.bus)
        self.signal_engine.wire(self.bus)

        # Kick off the outcome/resolution background loops
        await self.outcome_tracker.start()

        # Wire stream-mode strategies to bus
        for strat in self._strategies:
            if strat.mode in (StrategyMode.STREAM, StrategyMode.HYBRID):
                self.bus.subscribe("derived.state_update", strat.on_state_update)

        # Initial scan
        print(f"  {DIM}Running initial scan...{RESET}\n")
        await self._run_scan_cycle()

        # Start streaming for monitored markets
        await self._start_streaming()

        # Start async loops
        scan_task = asyncio.create_task(self._scan_loop(), name="scan")
        monitor_task = asyncio.create_task(self._monitor_loop(), name="monitor")
        display_task = asyncio.create_task(self._display_loop(), name="display")
        input_task = asyncio.create_task(self._input_loop(), name="input")

        # Show the main menu prompt
        print(f"  {BOLD}Ready.{RESET} Press {BOLD}Enter{RESET} for menu, or type a command.\n")

        try:
            await asyncio.gather(scan_task, monitor_task, display_task, input_task)
        except asyncio.CancelledError:
            pass

    async def stop(self):
        self._running = False
        print(f"\n  {YELLOW}Shutting down...{RESET}")
        await self.outcome_tracker.stop()
        await self.ws.stop()
        await self.gamma.close()
        await self.clob.close()
        if getattr(self, "_telemetry", None):
            self._telemetry.emit("session.end")
            from farsight.markets import telemetry as _tel
            _tel.set_sink(None)
            self._telemetry.close()

        elapsed = (time.time() - self._start_time) / 60 if self._start_time else 0
        portfolio = self.store.get_portfolio()
        health = self.signal_engine.get_health()
        self.session.increment("signals_emitted", health["signals_generated"] - self.session.counts["signals_emitted"])
        self.session.increment("signals_suppressed", health["signals_suppressed"] - self.session.counts["signals_suppressed"])
        self.session.end()

        print(f"  Session: {elapsed:.1f}m | Signals: {health['signals_generated']} "
              f"(suppressed {health['signals_suppressed']}) | "
              f"Opportunities: {len(self.all_opportunities)} | "
              f"PnL: ${portfolio['total_pnl']:+,.2f}")
        self.store.close()
        print()

    # ── Phase 0 helpers ──────────────────────────────────────────────

    def _wire_bus_telemetry(self):
        """Bridge event-bus topics to telemetry. Sampled for firehose topics.

        Enriches tick/trade.print with a human-readable market slug resolved
        from `self.token_to_question`, so the event feed shows
        `tick  nba-lakers-win  mid=0.95` instead of orphan numbers.
        """
        import random

        sink = self._telemetry

        def _slug_for(msg: dict) -> str:
            tid = msg.get("token_id") or ""
            return (self.token_to_question.get(tid) or "?")[:40]

        async def _on_signal(msg):
            sink.emit("signal", **(msg if isinstance(msg, dict) else {"raw": str(msg)}))

        async def _on_trade(msg):
            sink.emit("trade.executed", **(msg if isinstance(msg, dict) else {"raw": str(msg)}))

        async def _on_tick(msg):
            # Sample 1-in-10 to keep JSONL manageable under heavy streams.
            if random.random() < 0.1 and isinstance(msg, dict):
                sink.emit("tick", slug=_slug_for(msg), **msg)

        async def _on_trade_print(msg):
            if isinstance(msg, dict):
                sink.emit("trade.print", slug=_slug_for(msg), **msg)
            else:
                sink.emit("trade.print", raw=str(msg))

        self.bus.subscribe("signal.generated", _on_signal)
        self.bus.subscribe("trade.executed", _on_trade)
        self.bus.subscribe("raw.price_tick", _on_tick)
        self.bus.subscribe("raw.trade_print", _on_trade_print)

    def _warm_start_signal_filter(self):
        """Rebuild cooldown + dedup state from persisted recent signals.

        Eliminates duplicate-spam after a restart. Pulls signals from the
        last COOLDOWN_WARMSTART_HOURS window (a superset of the cooldown
        window — the dedup set benefits from the longer tail).
        """
        from datetime import timedelta as _td
        since = (datetime.utcnow() - _td(hours=settings.COOLDOWN_WARMSTART_HOURS)).isoformat()
        recent = self.store.get_signals_since(since)
        if recent:
            self.signal_engine.warm_start(recent)
            logger.info(f"warm-start: reloaded {len(recent)} recent signals for cooldown/dedup")

    def _persist_and_track_signal(self, payload: dict):
        """on_emit hook: persist the signal and schedule outcome tracking."""
        try:
            self.store.save_signal(payload)
        except Exception as e:
            logger.warning(f"save_signal failed: {e}")
        self.outcome_tracker.on_signal_emitted(payload)
        self.session.increment("signals_emitted")

    # ── Scan Loop ────────────────────────────────────────────────────

    async def _scan_loop(self):
        """Periodically run scan strategies."""
        from farsight.markets import telemetry as _tel
        while self._running:
            # Wait for next scan interval (use shortest strategy interval)
            min_interval = min(s.scan_interval_seconds for s in self._strategies if s.mode != StrategyMode.STREAM)
            # Tick every 15s emitting a heartbeat so the dashboard shows
            # liveness + time-to-next-scan. Cheap and decoupled from scans.
            elapsed = 0
            while elapsed < min_interval and self._running:
                await asyncio.sleep(15)
                elapsed += 15
                portfolio = self.store.get_portfolio()
                _tel.emit(
                    "heartbeat",
                    next_scan_in=max(0, min_interval - elapsed),
                    open_positions=len(self.store.get_open_trades()),
                    balance=portfolio.get("current_balance", 0),
                    total_pnl=portfolio.get("total_pnl", 0),
                )
            if self._running:
                await self._run_scan_cycle()

    async def _run_scan_cycle(self):
        """Run all strategies and process opportunities."""
        from farsight.markets import telemetry as _tel
        all_opps: list[Opportunity] = []

        for strategy in self._strategies:
            t0 = time.time()
            _tel.emit("scan.start", strategy=strategy.name)
            try:
                opps = await strategy.scan()
                all_opps.extend(opps)
                if opps:
                    mode_tag = f" {DIM}[stream]{RESET}" if strategy.mode == StrategyMode.STREAM else ""
                    print(f"  {ORANGE}{strategy.name}{RESET}: {len(opps)} opportunities{mode_tag}")
                _tel.emit(
                    "scan.end", strategy=strategy.name,
                    elapsed_ms=int((time.time() - t0) * 1000),
                    emitted=len(opps),
                )
            except Exception as e:
                logger.error(f"Strategy {strategy.name} scan failed: {e}")
                _tel.emit("error", strategy=strategy.name, where="scan", message=str(e))

        # Dedup by market_id (keep highest score)
        seen = {}
        for opp in all_opps:
            key = opp.market_id
            if key not in seen or opp.score > seen[key].score:
                seen[key] = opp
        all_opps = sorted(seen.values(), key=lambda o: o.score, reverse=True)

        self.all_opportunities = all_opps

        if all_opps:
            self._print_opportunities(all_opps[:10])
        else:
            print(f"  {DIM}No opportunities found this scan.{RESET}")

        # Route every opportunity through Policy → Executor. Policy enforces
        # caps, dedup vs open trades, sizing; Executor places the paper trade.
        # No arbitrary top-k truncation.
        if self.auto_trade and all_opps:
            await self._execute_top_opportunities(all_opps)

        for opp in all_opps[:20]:
            self.store.log_event("opportunity", json.dumps(opp.to_dict()))

    # ── Monitor Loop ─────────────────────────────────────────────────

    async def _monitor_loop(self):
        """Periodically check open positions for stop-loss/take-profit."""
        while self._running:
            await asyncio.sleep(60)
            if not self._running:
                break

            open_trades = self.store.get_open_trades()
            if not open_trades:
                continue

            for strategy in self._strategies:
                try:
                    actions = await strategy.monitor(open_trades)
                    for action in actions:
                        await self._execute_action(action)
                except Exception as e:
                    logger.error(f"Strategy {strategy.name} monitor failed: {e}")

    # ── Streaming ────────────────────────────────────────────────────

    async def _start_streaming(self):
        """Start WebSocket streaming for markets with open positions or high opportunities.

        Skipped entirely if no active strategy consumes streaming data
        (STREAM or HYBRID mode). SCAN-only runs don't burn bandwidth on
        ticks that nothing will read.
        """
        needs_streams = any(
            s.mode in (StrategyMode.STREAM, StrategyMode.HYBRID)
            for s in self._strategies
        )
        if not needs_streams:
            print(f"  {DIM}No streaming strategies active — skipping WS subscriptions.{RESET}")
            return

        token_ids = set()

        # Stream markets we have positions in
        for trade in self.store.get_open_trades():
            tid = trade.get("token_id", "")
            if tid:
                token_ids.add(tid)
                self.token_to_question[tid] = trade.get("market_question", "?")[:50]

        # Also stream top opportunity markets
        for opp in self.all_opportunities[:self.stream_markets]:
            if opp.token_id:
                token_ids.add(opp.token_id)
                self.token_to_question[opp.token_id] = opp.market_question[:50]

        if token_ids:
            # Share token→question mapping with streaming strategies
            for strat in self._strategies:
                if hasattr(strat, '_token_questions'):
                    strat._token_questions = self.token_to_question

            # Backfill history first
            await self._backfill_history(token_ids)
            await self.ws.update_subscriptions(token_ids)
            asyncio.create_task(self.ws.connect(), name="ws")
            print(f"  {DIM}Streaming {len(token_ids)} tokens{RESET}")

    async def _backfill_history(self, token_ids: set[str]):
        """Backfill price history so rolling windows have data immediately."""
        filled = 0
        for token_id in token_ids:
            try:
                history = await self.clob.get_price_history(token_id, interval="1m", fidelity=120)
                for point in history:
                    try:
                        ts = datetime.fromtimestamp(int(point["t"]), tz=timezone.utc).replace(tzinfo=None)
                        price = float(point["p"])
                        if price > 0:
                            state = self.state_engine._get_or_create(token_id)
                            state.update_price(ts, mid=price)
                    except (ValueError, TypeError, KeyError):
                        continue
                filled += 1
            except Exception:
                pass
        if filled:
            print(f"  {DIM}Backfilled {filled}/{len(token_ids)} tokens{RESET}")

    # ── Display ──────────────────────────────────────────────────────

    async def _display_loop(self):
        """Periodic status output."""
        while self._running:
            await asyncio.sleep(60)
            if self._running and not self._active_view:
                self._print_status_line()

    def _print_opportunities(self, opps: list[Opportunity]):
        """Print ranked opportunity table."""
        print(f"\n  {BOLD}Top Opportunities{RESET}")
        print(f"  {'#':>3}  {'Score':>7}  {'Edge':>6}  {'Conf':>5}  {'Prob':>6}  {'Liq':>7}  {'Strategy':10} Question")
        print(f"  {DIM}{'─' * 90}{RESET}")

        for i, opp in enumerate(opps, 1):
            edge_color = GREEN if opp.edge > 0 else RED
            print(
                f"  {i:3}  {opp.score:7.4f}  "
                f"{edge_color}{opp.edge:+5.1%}{RESET}  "
                f"{opp.confidence:5.0%}  "
                f"{CYAN}{opp.entry_price:5.0%}{RESET}  "
                f"{opp.liquidity / 1000:6.0f}K  "
                f"{opp.strategy:10} "
                f"{opp.market_question[:40]}"
            )
            if opp.reasoning:
                print(f"       {DIM}{opp.reasoning[:80]}{RESET}")
        print()

    def _print_status_line(self):
        elapsed = (time.time() - self._start_time) / 60 if self._start_time else 0
        ws = self.ws.get_health()
        portfolio = self.store.get_portfolio()
        pnl = portfolio["total_pnl"]
        pnl_color = GREEN if pnl >= 0 else RED
        open_count = len(self.store.get_open_trades())

        print(f"  {DIM}[{elapsed:.0f}m]{RESET} "
              f"opps={len(self.all_opportunities)} "
              f"stream={ws.get('messages_received', 0)}msgs "
              f"open={open_count} "
              f"pnl={pnl_color}${pnl:+,.2f}{RESET}")

    # ── Execution ────────────────────────────────────────────────────

    async def _execute_top_opportunities(self, opps: list[Opportunity]):
        """Route opportunities through Policy → Executor.

        Opportunities are converted to Signals via the compat shim; once all
        strategies emit Signals natively, this wrapper goes away.
        """
        from farsight.markets.strategies.types import Signal
        for opp in opps:
            signal = Signal.from_opportunity(opp)
            order = self._policy.apply(signal)
            if order is None:
                continue
            fill = await self._executor.execute(order)
            if fill and fill.trade_ids:
                self.session.increment("trades_opened")
                leg = fill.legs[0]
                print(f"  {GREEN}TRADE{RESET} {leg.side.upper()} {leg.outcome_label} "
                      f"${fill.size_usd:.2f} @ {fill.fill_prices[0]:.4f}  "
                      f"{DIM}{opp.market_question[:45]}{RESET}")

    async def _execute_action(self, action: Action):
        """Execute a monitor action (close, stop-loss)."""
        if action.action_type in (ActionType.CLOSE, ActionType.STOP_LOSS):
            trade = next((t for t in self.store.get_open_trades() if t["id"] == action.trade_id), None)
            if not trade:
                return

            exit_price = action.exit_price or 0
            entry = trade.get("entry_price", 0)
            direction = trade.get("direction", "BUY")
            num_shares = trade.get("num_shares", 0)

            if direction == "BUY":
                pnl = (exit_price - trade["fill_price"]) * num_shares
            else:
                pnl = (trade["fill_price"] - exit_price) * num_shares

            return_pct = (pnl / trade["size_usd"]) * 100 if trade["size_usd"] > 0 else 0

            self.store.close_trade(trade["id"], exit_price, action.action_type.value, round(pnl, 2), round(return_pct, 2))

            portfolio = self.store.get_portfolio()
            self.store.update_portfolio(
                current_balance=portfolio["current_balance"] + trade["size_usd"] + pnl,
                total_pnl=portfolio["total_pnl"] + pnl,
                winning_trades=portfolio["winning_trades"] + (1 if pnl > 0 else 0),
            )

            from farsight.markets import telemetry as _tel
            _tel.emit(
                "trade.close", strategy=trade.get("strategy"),
                trade_id=trade["id"], slug=trade.get("event_slug"),
                exit=exit_price, pnl=round(pnl, 2), return_pct=round(return_pct, 2),
                reason=action.reason, kind=action.action_type.value,
            )
            portfolio = self.store.get_portfolio()
            _tel.emit(
                "portfolio",
                balance=portfolio["current_balance"],
                total_pnl=portfolio["total_pnl"],
                open_positions=len(self.store.get_open_trades()),
            )

            pnl_color = GREEN if pnl >= 0 else RED
            label = "CLOSE" if action.action_type == ActionType.CLOSE else "STOP"
            print(f"  {YELLOW}{label}{RESET} {trade.get('outcome', '?')} "
                  f"pnl={pnl_color}${pnl:+,.2f}{RESET}  {DIM}{action.reason}{RESET}")

    # ── Interactive Commands ─────────────────────────────────────────

    async def _input_loop(self):
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                line = await loop.run_in_executor(None, self._read_input)
                if line is None:
                    continue
                await self._handle_command(line.strip())
            except (EOFError, KeyboardInterrupt):
                self._running = False
                break
            except Exception as e:
                print(f"  {RED}Error: {e}{RESET}")

    @staticmethod
    def _read_input() -> Optional[str]:
        try:
            return input()
        except (EOFError, KeyboardInterrupt):
            return None

    def _prompt_choice(self, title: str, options: list[tuple[str, str]]) -> Optional[str]:
        """Show a numbered menu and return the selected key. None if cancelled."""
        print(f"\n  {BOLD}{title}{RESET}")
        for i, (key, label) in enumerate(options, 1):
            print(f"    {ORANGE}{i}{RESET}  {label}")
        print(f"    {DIM}0  Cancel{RESET}")
        print()

        try:
            choice = input(f"  {DIM}>{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            return None

        if not choice or choice == "0":
            return None

        try:
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx][0]
        except ValueError:
            # Try matching by key name
            for key, label in options:
                if choice.lower() == key.lower():
                    return key

        print(f"  {DIM}Invalid choice.{RESET}")
        return None

    async def _handle_command(self, cmd: str):
        if not cmd:
            if self._active_view:
                self._active_view = None
                print(f"  View: {BOLD}OFF{RESET}")
            else:
                # Empty input with no view active — show main menu
                await self._cmd_main_menu()
            return

        parts = cmd.split()
        command = parts[0].lower()
        arg = parts[1].lower() if len(parts) > 1 else ""

        if command in ("help", "h", "?", "menu"):
            await self._cmd_main_menu()
        elif command in ("scan", "sc"):
            await self._cmd_scan_menu()
        elif command in ("opportunities", "opp", "o"):
            self._print_opportunities(self.all_opportunities[:15])
        elif command in ("explore", "browse", "e"):
            await self._cmd_explore_menu(arg)
        elif command in ("analyze", "a"):
            await self._cmd_analyze_menu(arg)
        elif command in ("markets",):
            await self._cmd_list_markets(arg)
        elif command in ("events",):
            await self._cmd_list_events(arg)
        elif command in ("categories", "cats"):
            await self._cmd_categories()
        elif command in ("portfolio", "p", "pf"):
            self._cmd_portfolio()
        elif command in ("trades", "t"):
            self._cmd_trades()
        elif command in ("status", "s"):
            self._cmd_status()
        elif command in ("view", "v"):
            await self._cmd_view_menu(arg)
        elif command in ("strategy", "strat"):
            self._cmd_strategy_menu()
        elif command in ("auto",):
            self.auto_trade = not self.auto_trade
            print(f"  Auto-trade: {GREEN}ON{RESET}" if self.auto_trade else f"  Auto-trade: {YELLOW}OFF{RESET}")
        elif command in ("reset",):
            self._cmd_reset_menu()
        elif command in ("quit", "exit", "stop"):
            self._running = False
        else:
            try:
                num = int(command)
                await self._handle_menu_shortcut(num)
            except ValueError:
                print(f"  Unknown: {command}. Press Enter for menu.")

    async def _cmd_main_menu(self):
        choice = self._prompt_choice("What would you like to do?", [
            ("scan", "Run strategies now"),
            ("opportunities", "View opportunities from last scan"),
            ("explore", "Browse markets & events"),
            ("analyze", "Analyze a specific market or event"),
            ("portfolio", "Paper portfolio & positions"),
            ("trades", "Trade history"),
            ("view", "Live data views (stream, movers)"),
            ("strategy", "Manage strategies"),
            ("status", "Bot status & health"),
            ("settings", "Settings (auto-trade, reset)"),
        ])
        if choice == "scan":
            await self._cmd_scan_menu()
        elif choice == "opportunities":
            self._print_opportunities(self.all_opportunities[:15])
        elif choice == "explore":
            await self._cmd_explore_menu()
        elif choice == "analyze":
            await self._cmd_analyze_menu()
        elif choice == "portfolio":
            self._cmd_portfolio()
        elif choice == "trades":
            self._cmd_trades()
        elif choice == "view":
            await self._cmd_view_menu("")
        elif choice == "strategy":
            self._cmd_strategy_menu()
        elif choice == "status":
            self._cmd_status()
        elif choice == "settings":
            self._cmd_settings_menu()

    async def _cmd_scan_menu(self):
        active = [(s.name, f"{s.name} — {s.__class__.__name__}") for s in self._strategies]
        choice = self._prompt_choice("Run strategy scan", [
            ("all", "Run all active strategies"),
        ] + active)
        if choice == "all":
            print(f"  {DIM}Scanning all strategies...{RESET}")
            await self._run_scan_cycle()
        elif choice:
            # Run single strategy
            for strat in self._strategies:
                if strat.name == choice:
                    print(f"  {DIM}Running {strat.name}...{RESET}")
                    try:
                        opps = await strat.scan()
                        if opps:
                            print(f"  {ORANGE}{strat.name}{RESET}: {len(opps)} opportunities")
                            self._print_opportunities(opps[:10])
                        else:
                            print(f"  {DIM}No opportunities found.{RESET}")
                    except Exception as e:
                        print(f"  {RED}Error: {e}{RESET}")
                    break

    async def _cmd_view_menu(self, arg: str):
        if arg:
            # Direct view switch
            self._activate_view(arg)
            return

        choice = self._prompt_choice("Live data view", [
            ("stream", "Raw tick & trade stream"),
            ("off", "Turn off live view"),
        ])
        if choice:
            self._activate_view(choice)

    def _activate_view(self, view: str):
        if view in ("stream", "s"):
            self._active_view = "stream"
            self.bus.subscribe("raw.price_tick", self._on_view_tick)
            self.bus.subscribe("raw.trade_print", self._on_view_trade)
            print(f"\n  View: {ORANGE}Live Stream{RESET} {DIM}(press Enter to exit){RESET}\n")
        elif view in ("off", "exit"):
            self._active_view = None
            print(f"  View: OFF")

    def _cmd_strategy_menu(self):
        print(f"\n  {BOLD}Active Strategies{RESET}")
        for i, strat in enumerate(self._strategies, 1):
            mode_label = {
                StrategyMode.SCAN: f"{CYAN}SCAN{RESET}",
                StrategyMode.STREAM: f"{GREEN}STREAM{RESET}",
                StrategyMode.HYBRID: f"{YELLOW}HYBRID{RESET}",
            }.get(strat.mode, strat.mode.value)
            print(f"    {ORANGE}{i}{RESET}  {strat.name:15} {mode_label}  every {strat.scan_interval_seconds}s")
        print()

    def _cmd_settings_menu(self):
        choice = self._prompt_choice("Settings", [
            ("auto", f"Auto-trade: {'ON' if self.auto_trade else 'OFF'} — toggle"),
            ("reset", "Reset paper portfolio to $10K"),
            ("quiet", "Toggle periodic stats output"),
        ])
        if choice == "auto":
            self.auto_trade = not self.auto_trade
            print(f"  Auto-trade: {GREEN}ON{RESET}" if self.auto_trade else f"  Auto-trade: {YELLOW}OFF{RESET}")
        elif choice == "reset":
            self._cmd_reset_menu()
        elif choice == "quiet":
            pass  # Could add quiet toggle here

    def _cmd_reset_menu(self):
        choice = self._prompt_choice("Reset", [
            ("portfolio", "Reset paper portfolio to $10K (deletes all trades)"),
            ("signals", "Clear signal history"),
        ])
        if choice == "portfolio":
            self.store.reset_portfolio()
            print(f"  {GREEN}Portfolio reset to $10,000{RESET}")
        elif choice == "signals":
            conn = self.store._get_conn()
            conn.execute("DELETE FROM signals")
            conn.commit()
            print(f"  {GREEN}Signal history cleared{RESET}")

    async def _handle_menu_shortcut(self, num: int):
        """Handle bare number input as main menu shortcut."""
        menu = ["scan", "opportunities", "explore", "analyze",
                "portfolio", "trades", "view", "strategy", "status", "settings"]
        if 1 <= num <= len(menu):
            await self._handle_command(menu[num - 1])

    # ── Explore Commands ─────────────────────────────────────────────

    async def _cmd_explore_menu(self, arg: str = ""):
        if arg:
            if arg in ("markets", "m"):
                await self._cmd_list_markets("")
            elif arg in ("events", "e"):
                await self._cmd_list_events("")
            elif arg in ("categories", "cats", "c"):
                await self._cmd_categories()
            elif arg in ("trending", "t"):
                await self._cmd_trending()
            return

        choice = self._prompt_choice("Browse Prediction Markets", [
            ("trending", "Trending Polymarket markets (24h volume)"),
            ("markets", "List top Polymarket markets"),
            ("events", "List top Polymarket events"),
            ("kalshi", "Browse Kalshi markets (CFTC-regulated)"),
            ("categories", "Browse by category"),
            ("search", "Search markets by keyword"),
        ])
        if choice == "trending":
            await self._cmd_trending()
        elif choice == "markets":
            await self._cmd_list_markets("")
        elif choice == "events":
            await self._cmd_list_events("")
        elif choice == "kalshi":
            await self._cmd_kalshi_markets()
        elif choice == "categories":
            await self._cmd_categories()
        elif choice == "search":
            await self._cmd_search()

    async def _cmd_trending(self):
        """Show trending markets with 24h volume and price changes."""
        raw = await self.gamma.get_markets(
            active=True, closed=False, limit=15,
            order="volume_24hr", ascending=False,
        )

        print(f"\n  {BOLD}Trending Markets{RESET}")
        print(f"  {'#':>3}  {'Prob':>6}  {'24h Chg':>7}  {'Vol 24h':>9}  Question")
        print(f"  {DIM}{'-' * 75}{RESET}")

        for i, m in enumerate(raw, 1):
            slug = m.get("slug", "")
            if "updown-5m" in slug or "updown-15m" in slug:
                continue
            market = GammaClient.normalize_market(m)
            if not market.outcomes:
                continue
            price = market.outcomes[0].current_price
            change = m.get("oneDayPriceChange")
            vol24 = float(m.get("volume24hr") or 0)
            q = market.question[:45]

            change_str = f"{GREEN}{change:+.1%}{RESET}" if change and change > 0 else \
                         f"{RED}{change:+.1%}{RESET}" if change and change < 0 else f"{DIM}  --{RESET}"

            print(f"  {i:3}  {CYAN}{price:5.0%}{RESET}  {change_str:>16}  "
                  f"{'${:,.0f}'.format(vol24):>9}  {q}")
        print()

    async def _cmd_list_markets(self, category: str):
        """List top markets, optionally filtered by category keyword."""
        raw = await self.gamma.get_markets(
            active=True, closed=False, limit=50,
            order="volume_24hr", ascending=False,
        )

        if category:
            raw = [m for m in raw if category.lower() in (m.get("question") or "").lower()
                   or category.lower() in (m.get("slug") or "").lower()]

        print(f"\n  {BOLD}Markets{' (' + category + ')' if category else ''}{RESET}")
        print(f"  {'#':>3}  {'Prob':>6}  {'Liq':>7}  Question")
        print(f"  {DIM}{'-' * 70}{RESET}")

        for i, m in enumerate(raw[:20], 1):
            slug = m.get("slug", "")
            if "updown-5m" in slug or "updown-15m" in slug:
                continue
            market = GammaClient.normalize_market(m)
            if not market.outcomes:
                continue
            price = market.outcomes[0].current_price
            liq = market.liquidity
            q = market.question[:50]

            print(f"  {i:3}  {CYAN}{price:5.0%}{RESET}  {'${:,.0f}'.format(liq):>7}  {q}")

            # Show selectable slug
            if i <= 20:
                print(f"       {DIM}slug: {market.slug}{RESET}")
        print()

    async def _cmd_list_events(self, category: str):
        """List top events."""
        raw = await self.gamma.get_events(
            active=True, closed=False, limit=30,
            order="volume_24hr", ascending=False,
        )

        if category:
            raw = [e for e in raw if category.lower() in (e.get("title") or "").lower()]

        print(f"\n  {BOLD}Events{' (' + category + ')' if category else ''}{RESET}")
        print(f"  {DIM}{'-' * 70}{RESET}")

        for i, e in enumerate(raw[:15], 1):
            event = GammaClient.normalize_event(e)
            cat = event.category or "uncategorized"
            num_markets = len(event.markets)
            vol24 = float(e.get("volume24hr") or 0)

            print(f"  {ORANGE}{i:3}.{RESET} {event.title[:55]}")
            print(f"       {DIM}{cat} | {num_markets} markets | vol24h ${'${:,.0f}'.format(vol24)} | {event.slug}{RESET}")

            # Show top 3 outcomes
            prices = []
            for m in event.markets[:3]:
                if m.outcomes:
                    p = m.outcomes[0]
                    prices.append(f"{CYAN}{p.current_price:.0%}{RESET} {p.label}")
            if prices:
                print(f"       {' | '.join(prices)}")
            print()

    async def _cmd_categories(self):
        """Show available categories with market counts."""
        raw = await self.gamma.get_events(active=True, closed=False, limit=100, order="volume_24hr")

        cat_counts: dict[str, int] = {}
        cat_volume: dict[str, float] = {}
        for e in raw:
            event = GammaClient.normalize_event(e)
            cat = event.category or "uncategorized"
            cat_counts[cat] = cat_counts.get(cat, 0) + len(event.markets)
            cat_volume[cat] = cat_volume.get(cat, 0) + float(e.get("volume24hr") or 0)

        sorted_cats = sorted(cat_counts.items(), key=lambda x: cat_volume.get(x[0], 0), reverse=True)

        print(f"\n  {BOLD}Categories{RESET}")
        print(f"  {'#':>3}  {'Markets':>8}  {'Vol 24h':>10}  Category")
        print(f"  {DIM}{'-' * 50}{RESET}")
        for i, (cat, count) in enumerate(sorted_cats, 1):
            vol = cat_volume.get(cat, 0)
            print(f"  {i:3}  {count:>8}  {'${:,.0f}'.format(vol):>10}  {cat}")
        print(f"\n  {DIM}Use 'markets <category>' or 'events <category>' to filter{RESET}\n")

    async def _cmd_kalshi_markets(self):
        """Browse Kalshi markets and events."""
        from farsight.markets.clients.kalshi.rest_client import KalshiClient

        kalshi = KalshiClient()
        try:
            # Check connectivity first
            status = await kalshi.get_exchange_status()
            if not status:
                print(f"  {RED}Could not connect to Kalshi API{RESET}")
                return

            exchange_status = status.get("exchange_active", False)
            trading = status.get("trading_active", False)
            print(f"\n  {BOLD}Kalshi Exchange{RESET}  "
                  f"Exchange: {'ACTIVE' if exchange_status else 'INACTIVE'}  "
                  f"Trading: {'OPEN' if trading else 'CLOSED'}")

            # Fetch events with nested markets
            result = await kalshi.get_events(status="open", with_nested_markets=True, limit=15)
            events = result.get("events", [])

            if not events:
                print(f"  {DIM}No open events found.{RESET}\n")
                return

            print(f"\n  {BOLD}Open Kalshi Events ({len(events)}){RESET}")
            print(f"  {DIM}{'-' * 70}{RESET}")

            for i, event in enumerate(events[:15], 1):
                title = event.get("title", "?")[:55]
                ticker = event.get("event_ticker", "?")
                markets = event.get("markets", [])
                category = event.get("category", "")

                print(f"  {ORANGE}{i:3}.{RESET} {title}")
                print(f"       {DIM}{ticker} | {len(markets)} markets | {category}{RESET}")

                # Show top 3 markets
                for m in markets[:3]:
                    prob = KalshiClient.market_to_probability(m)
                    yes_title = m.get("yes_sub_title", "?")[:45]
                    vol24 = float(m.get("volume_24h_fp") or 0)
                    print(f"       {CYAN}{prob:5.0%}{RESET}  {yes_title}  {DIM}vol24h={vol24:.0f}{RESET}")

                if len(markets) > 3:
                    print(f"       {DIM}... +{len(markets) - 3} more markets{RESET}")
                print()

        except Exception as e:
            print(f"  {RED}Error: {e}{RESET}")
        finally:
            await kalshi.close()

    async def _cmd_search(self):
        """Search markets by keyword."""
        try:
            query = input(f"  {DIM}Search:{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            return
        if not query:
            return

        await self._cmd_list_markets(query)

    # ── Analyze Commands ─────────────────────────────────────────────

    async def _cmd_analyze_menu(self, slug: str = ""):
        if slug:
            await self._cmd_analyze_slug(slug)
            return

        choice = self._prompt_choice("Analyze a market or event", [
            ("slug", "Enter a market/event slug"),
            ("top", "Analyze the top opportunity"),
            ("event", "Pick from recent events"),
        ])
        if choice == "slug":
            try:
                slug = input(f"  {DIM}Slug:{RESET} ").strip()
            except (EOFError, KeyboardInterrupt):
                return
            if slug:
                await self._cmd_analyze_slug(slug)
        elif choice == "top":
            if self.all_opportunities:
                opp = self.all_opportunities[0]
                slug = opp.event_slug or opp.market_id
                await self._cmd_analyze_slug(slug)
            else:
                print(f"  {DIM}No opportunities to analyze. Run 'scan' first.{RESET}")
        elif choice == "event":
            raw = await self.gamma.get_events(active=True, closed=False, limit=10, order="volume_24hr")
            events = [(GammaClient.normalize_event(e).slug,
                       GammaClient.normalize_event(e).title[:50])
                      for e in raw if len(GammaClient.normalize_event(e).markets) >= 2]
            if not events:
                print(f"  {DIM}No events found.{RESET}")
                return
            picked = self._prompt_choice("Pick an event to analyze", events[:8])
            if picked:
                await self._cmd_analyze_slug(picked)

    async def _cmd_analyze_slug(self, slug: str):
        """Full analysis of a market or event by slug."""
        from farsight.markets.services.state_engine import MarketState
        from farsight.markets.services.feature_engine import compute_features

        print(f"  {DIM}Analyzing {slug}...{RESET}")

        # Try as event first
        raw = await self.gamma.get_event_by_slug(slug)
        if raw:
            event = GammaClient.normalize_event(raw)
            print(f"\n  {BOLD}{event.title}{RESET}")
            print(f"  {DIM}Category: {event.category or '?'} | Markets: {len(event.markets)} | Slug: {event.slug}{RESET}")

            price_sum = 0.0
            for market in sorted(event.markets, key=lambda m: m.outcomes[0].current_price if m.outcomes else 0, reverse=True):
                if not market.outcomes:
                    continue
                primary = market.outcomes[0]
                price_sum += primary.current_price

                if primary.current_price < 0.005:
                    continue

                # Fetch features
                book = await self.clob.get_orderbook(primary.token_id)
                state = MarketState(primary.token_id)
                history = await self.clob.get_price_history(primary.token_id, interval="1m", fidelity=60)
                for pt in history:
                    try:
                        ts = datetime.fromtimestamp(int(pt["t"]), tz=timezone.utc).replace(tzinfo=None)
                        p = float(pt["p"])
                        if p > 0:
                            state.update_price(ts, mid=p)
                    except (ValueError, TypeError, KeyError):
                        continue
                if book and book.mid > 0:
                    state.update_price(datetime.utcnow(), mid=book.mid, bid=book.best_bid, ask=book.best_ask)

                features = compute_features(state)
                d5m = features.get("delta_5m")
                d1h = features.get("delta_1h")
                liq = features.get("liquidity_score", 0)
                rsi_val = features.get("rsi_1h")
                mom = features.get("momentum_score")
                bb_pos = features.get("bollinger_position")
                vol_r = features.get("volume_ratio")
                depth = f"{'${:,.0f}'.format(book.total_bid_depth + book.total_ask_depth)}" if book and book.mid > 0 else "--"

                d5m_str = f"{GREEN}{d5m:+.1%}{RESET}" if d5m and d5m > 0 else \
                          f"{RED}{d5m:+.1%}{RESET}" if d5m and d5m < 0 else f"{DIM}--{RESET}"
                d1h_str = f"{GREEN}{d1h:+.1%}{RESET}" if d1h and d1h > 0 else \
                          f"{RED}{d1h:+.1%}{RESET}" if d1h and d1h < 0 else f"{DIM}--{RESET}"
                rsi_color = RED if rsi_val and rsi_val > 70 else GREEN if rsi_val and rsi_val < 30 else ""
                rsi_str = f"{rsi_color}{rsi_val:.0f}{RESET}" if rsi_val is not None else f"{DIM}--{RESET}"
                mom_str = f"{GREEN}{mom:+.2f}{RESET}" if mom and mom > 0.2 else \
                          f"{RED}{mom:+.2f}{RESET}" if mom and mom < -0.2 else \
                          f"{mom:+.2f}" if mom is not None else f"{DIM}--{RESET}"

                print(f"\n  {CYAN}{primary.current_price:>5.0%}{RESET}  {primary.label}")
                print(f"       d5m={d5m_str}  d1h={d1h_str}  RSI={rsi_str}  mom={mom_str}  liq={liq:.2f}  depth={depth}")

            # Structural check
            deviation = abs(price_sum - 1.0)
            if price_sum > 1.03:
                print(f"\n  {RED}Structural: Prices sum to {price_sum:.1%} -- OVERPRICED by {deviation:.1%}{RESET}")
            elif price_sum < 0.97:
                print(f"\n  {YELLOW}Structural: Prices sum to {price_sum:.1%} -- UNDERPRICED{RESET}")
            else:
                print(f"\n  {GREEN}Structural: Prices sum to {price_sum:.1%} -- consistent{RESET}")
            print()
            return

        # Try as market
        raw = await self.gamma.get_market_by_slug(slug)
        if raw:
            market = GammaClient.normalize_market(raw)
            print(f"\n  {BOLD}{market.question}{RESET}")
            print(f"  {DIM}Status: {market.status.value} | Vol: ${'${:,.0f}'.format(market.volume_total)} | Slug: {market.slug}{RESET}")

            for outcome in market.outcomes:
                book = await self.clob.get_orderbook(outcome.token_id)
                bid = f"{book.best_bid:.4f}" if book and book.mid > 0 else "--"
                ask = f"{book.best_ask:.4f}" if book and book.mid > 0 else "--"
                depth = f"{'${:,.0f}'.format(book.total_bid_depth + book.total_ask_depth)}" if book and book.mid > 0 else "--"

                print(f"\n  {CYAN}{outcome.current_price:>5.0%}{RESET}  {outcome.label}")
                print(f"       bid={bid}  ask={ask}  depth={depth}")
            print()
            return

        print(f"  {RED}Not found: {slug}{RESET}\n")

    def _cmd_help(self):
        print(f"""
  {BOLD}Strategy Bot Commands{RESET}
    {CYAN}scan{RESET}             Run all strategies now (don't wait for interval)
    {CYAN}opportunities{RESET} (o) Show ranked opportunities from last scan
    {CYAN}portfolio{RESET}     (p) Paper portfolio + open positions
    {CYAN}trades{RESET}        (t) Trade history
    {CYAN}status{RESET}        (s) Bot status and stats
    {CYAN}view stream{RESET}      Watch live tick/trade data (Enter to exit)
    {CYAN}auto{RESET}             Toggle auto paper trading
    {CYAN}reset{RESET}            Reset portfolio to $10K
    {CYAN}quit{RESET}             Stop the bot
""")

    def _cmd_portfolio(self):
        portfolio = self.store.get_portfolio()
        pnl = portfolio["total_pnl"]
        pnl_color = GREEN if pnl >= 0 else RED
        win_rate = (portfolio["winning_trades"] / portfolio["total_trades"] * 100) if portfolio["total_trades"] > 0 else 0
        open_trades = self.store.get_open_trades()

        print(f"\n  {BOLD}Paper Portfolio{RESET}")
        print(f"  Balance: ${portfolio['current_balance']:,.2f}  "
              f"PnL: {pnl_color}${pnl:+,.2f}{RESET}  "
              f"Trades: {portfolio['total_trades']}  "
              f"Win: {win_rate:.0f}%")

        if open_trades:
            print(f"\n  {BOLD}Open Positions ({len(open_trades)}){RESET}")
            for t in open_trades[:10]:
                q = (t.get("market_question") or "?")[:40]
                strat = t.get("strategy", "?")
                print(f"  {t.get('direction', '?'):4} {t.get('outcome', '?'):6} "
                      f"${t['size_usd']:>7,.2f} @ {t['entry_price']:.4f}  "
                      f"{DIM}[{strat}] {q}{RESET}")
        print()

    def _cmd_trades(self):
        trades = self.store.get_trade_history(limit=15)
        if not trades:
            print(f"  {DIM}No trades yet.{RESET}")
            return
        print(f"\n  {BOLD}Trade History{RESET}")
        for t in trades:
            pnl = t.get("pnl_usd")
            if t["is_open"]:
                status = f"{CYAN}OPEN{RESET}"
            elif pnl is not None and pnl > 0:
                status = f"{GREEN}+${pnl:.2f}{RESET}"
            elif pnl is not None:
                status = f"{RED}${pnl:.2f}{RESET}"
            else:
                status = f"{DIM}?{RESET}"
            q = (t.get("market_question") or "?")[:35]
            print(f"  {t['opened_at'][:16]}  {t.get('direction', '?'):4} {t.get('outcome', '?'):6} "
                  f"${t['size_usd']:>7,.2f}  {status}  {DIM}{q}{RESET}")
        print()

    def _cmd_status(self):
        elapsed = (time.time() - self._start_time) / 60 if self._start_time else 0
        ws = self.ws.get_health()
        portfolio = self.store.get_portfolio()

        print(f"\n  {BOLD}Bot Status{RESET}")
        print(f"  Uptime:        {elapsed:.1f} minutes")
        print(f"  Strategies:    {', '.join(s.name for s in self._strategies)}")
        print(f"  Auto-trade:    {'ON' if self.auto_trade else 'OFF'}")
        print(f"  Opportunities: {len(self.all_opportunities)} from last scan")
        print(f"  Stream:        {ws.get('messages_received', 0)} msgs, {ws.get('subscribed_tokens', 0)} tokens")
        print(f"  Open trades:   {len(self.store.get_open_trades())}")
        print(f"  Portfolio:     ${portfolio['current_balance']:,.2f} (PnL: ${portfolio['total_pnl']:+,.2f})")
        print()

    # ── View mode handlers ───────────────────────────────────────────

    async def _on_view_tick(self, payload: dict):
        if self._active_view != "stream":
            return
        mid = payload.get("mid", 0)
        if mid <= 0:
            return
        name = self.token_to_question.get(payload.get("token_id", ""), "?")
        print(f"  {CYAN}TICK{RESET}  mid={mid:.4f}  {DIM}{name}{RESET}")

    async def _on_view_trade(self, payload: dict):
        if self._active_view != "stream":
            return
        price = payload.get("price", 0)
        size = payload.get("size_usd", 0)
        side = payload.get("side", "")
        name = self.token_to_question.get(payload.get("token_id", ""), "?")
        color = GREEN if side == "buy" else RED
        print(f"  {color}TRADE{RESET} ${size:>8,.2f} @ {price:.4f} {side:4}  {DIM}{name}{RESET}")


def suppress_loggers():
    """Suppress all noisy loggers — console is managed by the runner, not logging."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("farsight").setLevel(logging.WARNING)


async def run_pipeline(
    strategies: list[str] | None = None,
    auto_trade: bool = False,
    max_trades: int = 3,
    stream_markets: int = 20,
):
    """Entry point for running the bot."""
    suppress_loggers()

    runner = PipelineRunner(
        strategies=strategies,
        auto_trade=auto_trade,
        max_trades_per_scan=max_trades,
        stream_markets=stream_markets,
    )

    try:
        await runner.start()
    except KeyboardInterrupt:
        await runner.stop()
