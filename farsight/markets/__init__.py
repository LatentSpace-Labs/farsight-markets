"""
Farsight Markets — Prediction Market Intelligence Engine.

Real-time signal engine for prediction markets. Ingests Polymarket + Kalshi
data, computes 30 streaming features, generates ranked trading signals, and
runs composable strategies with paper trading.

Pipeline: WebSocket -> Event Bus -> State Engine -> Features -> Signals -> Strategies
"""
