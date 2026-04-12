"""
Standalone Event Markets API — lightweight server for testing and development.

Runs only the event markets explore endpoints. No DB, no auth, no main API dependencies.

Launch via VS Code "Event Markets API" config or:
    conda activate farsight
    uvicorn farsight.markets.app:app --reload --port 8001

CLI:
    conda activate farsight
    python -m farsight.markets.cli health
    python -m farsight.markets.cli markets --limit 10
    python -m farsight.markets.cli stream --seconds 15
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from farsight.markets.routes.explore_routes import router as explore_router

TAGS_METADATA = [
    {"name": "Health", "description": "API connectivity and status checks"},
    {"name": "Discovery", "description": "Browse markets, events, categories, and tags from Polymarket"},
    {"name": "Market Data", "description": "Live orderbooks, prices, trades, and price history"},
    {"name": "Streaming", "description": "WebSocket stream testing — live price ticks, trades, and orderbook updates"},
    {"name": "Analysis", "description": "Feature computation and signal detection on live market data"},
]

app = FastAPI(
    title="Farsight Prediction Markets API",
    description=(
        "Live prediction market intelligence platform.\n\n"
        "Proxies Polymarket Gamma + CLOB APIs with normalization, "
        "streaming feature computation, and signal detection.\n\n"
        "**No auth required** — all endpoints use public market data.\n\n"
        "### Workflow\n"
        "1. **Discover** markets via `/api/prediction-markets/markets` or `/api/prediction-markets/events`\n"
        "2. **Inspect** orderbook and price via `/api/prediction-markets/book?token_id=...`\n"
        "3. **Analyze** signals via `/api/prediction-markets/analyze/market/{slug}` or `/api/prediction-markets/analyze/event/{slug}`\n"
        "4. **Stream** live WebSocket data via `/api/prediction-markets/stream-test`"
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=TAGS_METADATA,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(explore_router)


@app.get("/", tags=["Health"])
def root():
    return {
        "service": "Farsight Prediction Markets API",
        "docs": "/docs",
        "endpoints": {
            "health": "/api/prediction-markets/health",
            "discovery": {
                "markets": "/api/prediction-markets/markets",
                "events": "/api/prediction-markets/events",
                "categories": "/api/prediction-markets/categories",
                "tags": "/api/prediction-markets/tags",
            },
            "market_data": {
                "book": "/api/prediction-markets/book?token_id=...",
                "price": "/api/prediction-markets/price?token_id=...",
                "price_history": "/api/prediction-markets/price-history?token_id=...&interval=1h",
                "trades": "/api/prediction-markets/trades?condition_id=...",
            },
            "analysis": {
                "features": "/api/prediction-markets/features?token_id=...",
                "analyze_market": "/api/prediction-markets/analyze/market/{slug}",
                "analyze_event": "/api/prediction-markets/analyze/event/{slug}",
            },
            "streaming": {
                "stream_test": "/api/prediction-markets/stream-test?seconds=10",
            },
        },
    }
