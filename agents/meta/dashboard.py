"""
APEX Dashboard — FastAPI-based monitoring and observability API.

NOT a BaseAgent — runs as a standalone FastAPI app within the APEX process.
Provides REST endpoints, WebSocket for real-time updates, and Prometheus
metrics for Grafana integration.

Start via: ``asyncio.create_task(start_dashboard(bus))``
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from config.agent_registry import AGENT_REGISTRY
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_AGENT_RANKINGS,
    STREAM_DECISIONS_APPROVED,
    STREAM_DECISIONS_REJECTED,
    STREAM_MARKET_PRICES,
    STREAM_PERFORMANCE_REPORTS,
    STREAM_PORTFOLIO_STATE,
    STREAM_RISK_ANOMALY,
    STREAM_SIGNALS,
    STREAM_TRADES_EXECUTED,
    STREAM_WEIGHT_UPDATES,
)

logger = logging.getLogger("apex.dashboard")

# ---------------------------------------------------------------------------
# In-memory caches (populated by background stream listeners)
# ---------------------------------------------------------------------------

_portfolio: dict[str, Any] = {}
_agent_health: dict[str, dict[str, Any]] = {}
_recent_signals: deque[dict[str, Any]] = deque(maxlen=200)
_recent_decisions: deque[dict[str, Any]] = deque(maxlen=200)
_recent_trades: deque[dict[str, Any]] = deque(maxlen=200)
_recent_anomalies: deque[dict[str, Any]] = deque(maxlen=100)
_agent_weights: dict[str, float] = {}
_agent_performance: dict[str, dict[str, Any]] = {}
_agent_rankings: list[dict[str, Any]] = []
_agent_rankings_by_id: dict[str, dict[str, Any]] = {}
_recent_journal: deque[dict[str, Any]] = deque(maxlen=200)
_mid_prices: dict[str, float] = {}
_ws_clients: set[Any] = set()  # Active WebSocket connections.
_bus_ref: MessageBus | None = None
_agents_ref: list[Any] = []


# ---------------------------------------------------------------------------
# Prometheus metrics (lazy import to avoid hard dep if not installed)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

    _prom_nav = Gauge("apex_portfolio_nav", "Portfolio NAV in USD")
    _prom_drawdown = Gauge("apex_portfolio_drawdown", "Current drawdown fraction")
    _prom_positions = Gauge("apex_portfolio_positions", "Number of open positions")
    _prom_daily_pnl = Gauge("apex_daily_pnl", "Daily PnL in USD")
    _prom_trades = Counter("apex_trades_total", "Total trades executed", ["side"])
    _prom_decisions = Counter("apex_decisions_total", "Total decisions", ["status"])
    _prom_signals = Counter("apex_signals_total", "Total signals emitted", ["agent_id"])
    _prom_anomalies = Counter("apex_anomalies_total", "Total anomalies detected", ["type"])
    _prom_agent_weight = Gauge("apex_agent_weight", "Current agent weight", ["agent_id"])
    _prom_agent_sharpe = Gauge("apex_agent_sharpe", "Agent rolling Sharpe ratio", ["agent_id"])
    _prom_agent_win_rate = Gauge("apex_agent_win_rate", "Agent win rate", ["agent_id"])
    _prom_agent_rank = Gauge("apex_agent_rank_level", "Agent rank as numeric level", ["agent_id"])
    _prom_agent_xp = Gauge("apex_agent_xp", "Agent XP", ["agent_id"])

    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False


def _update_prom_portfolio(data: dict[str, Any]) -> None:
    if not _PROM_AVAILABLE:
        return
    _prom_nav.set(data.get("total_nav", 0))
    _prom_drawdown.set(data.get("drawdown", 0))
    _prom_positions.set(len(data.get("positions", [])))
    _prom_daily_pnl.set(data.get("daily_pnl", 0))


# ---------------------------------------------------------------------------
# Background stream listeners
# ---------------------------------------------------------------------------

async def _listen_portfolio(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_PORTFOLIO_STATE, group="dashboard_portfolio", consumer="dashboard",
    ):
        try:
            _portfolio.update(payload)
            _update_prom_portfolio(payload)
            await _broadcast_ws({"type": "portfolio", "data": payload})
        except Exception:
            pass


async def _listen_signals(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_SIGNALS, group="dashboard_signals", consumer="dashboard",
    ):
        try:
            _recent_signals.append(payload)
            if _PROM_AVAILABLE:
                _prom_signals.labels(agent_id=payload.get("agent_id", "unknown")).inc()
            await _broadcast_ws({"type": "signal", "data": payload})
        except Exception:
            pass


async def _listen_decisions(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_DECISIONS_APPROVED, group="dashboard_decisions", consumer="dashboard",
    ):
        try:
            payload["_status"] = "approved"
            _recent_decisions.append(payload)
            if _PROM_AVAILABLE:
                _prom_decisions.labels(status="approved").inc()
            await _broadcast_ws({"type": "decision", "data": payload})
        except Exception:
            pass


async def _listen_rejections(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_DECISIONS_REJECTED, group="dashboard_rejections", consumer="dashboard",
    ):
        try:
            payload["_status"] = "rejected"
            _recent_decisions.append(payload)
            if _PROM_AVAILABLE:
                _prom_decisions.labels(status="rejected").inc()
        except Exception:
            pass


async def _listen_trades(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_TRADES_EXECUTED, group="dashboard_trades", consumer="dashboard",
    ):
        try:
            _recent_trades.append(payload)
            if _PROM_AVAILABLE:
                _prom_trades.labels(side=payload.get("side", "unknown")).inc()
            await _broadcast_ws({"type": "trade", "data": payload})
        except Exception:
            pass


async def _listen_anomalies(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_RISK_ANOMALY, group="dashboard_anomalies", consumer="dashboard",
    ):
        try:
            _recent_anomalies.append(payload)
            if _PROM_AVAILABLE:
                _prom_anomalies.labels(type=payload.get("anomaly_type", "unknown")).inc()
        except Exception:
            pass


async def _listen_prices(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_MARKET_PRICES, group="dashboard_prices", consumer="dashboard",
    ):
        try:
            prices = payload.get("prices", {})
            _mid_prices.update(prices)
        except Exception:
            pass


async def _listen_weights(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_WEIGHT_UPDATES, group="dashboard_weights", consumer="dashboard",
    ):
        try:
            weights = payload.get("weights", {})
            _agent_weights.update(weights)
            if _PROM_AVAILABLE:
                for aid, w in weights.items():
                    _prom_agent_weight.labels(agent_id=aid).set(w)
        except Exception:
            pass


async def _listen_performance(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_PERFORMANCE_REPORTS, group="dashboard_perf", consumer="dashboard",
    ):
        try:
            aid = payload.get("agent_id", "")
            if aid:
                _agent_performance[aid] = payload
                if _PROM_AVAILABLE:
                    _prom_agent_sharpe.labels(agent_id=aid).set(payload.get("sharpe_ratio", 0))
                    _prom_agent_win_rate.labels(agent_id=aid).set(payload.get("win_rate", 0))
        except Exception:
            pass


async def _listen_rankings(bus: MessageBus) -> None:
    async for _mid, payload in bus.subscribe_to(
        STREAM_AGENT_RANKINGS, group="dashboard_rankings", consumer="dashboard",
    ):
        try:
            rankings = payload.get("rankings", [])
            _agent_rankings.clear()
            _agent_rankings.extend(rankings)
            _agent_rankings_by_id.clear()
            for r in rankings:
                aid = r.get("agent_id", "")
                if aid:
                    _agent_rankings_by_id[aid] = r
            if _PROM_AVAILABLE:
                for r in rankings:
                    aid = r.get("agent_id", "")
                    if aid:
                        _prom_agent_rank.labels(agent_id=aid).set(r.get("rank_level", 0))
                        _prom_agent_xp.labels(agent_id=aid).set(r.get("xp", 0))
            await _broadcast_ws({"type": "rankings", "data": rankings})
        except Exception:
            pass


async def _broadcast_ws(message: dict) -> None:
    """Send a message to all connected WebSocket clients."""
    if not _ws_clients:
        return
    text = json.dumps(message, default=str)
    disconnected = set()
    for ws in _ws_clients:
        try:
            await ws.send_text(text)
        except Exception:
            disconnected.add(ws)
    _ws_clients -= disconnected


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

def create_app(bus: MessageBus | None = None, agents: list | None = None) -> Any:
    """Create the FastAPI application.

    Imported lazily so the module can be imported without fastapi installed
    (e.g., in agent instantiation checks).
    """
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import PlainTextResponse, JSONResponse

    global _bus_ref, _agents_ref
    _bus_ref = bus
    _agents_ref = agents or []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        tasks: list[asyncio.Task] = []
        if bus:
            listeners = [
                _listen_portfolio, _listen_signals, _listen_decisions,
                _listen_rejections, _listen_trades, _listen_anomalies,
                _listen_prices, _listen_weights, _listen_performance,
                _listen_rankings,
            ]
            for fn in listeners:
                tasks.append(asyncio.create_task(fn(bus)))
        yield
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    app = FastAPI(
        title="APEX Dashboard",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/")
    async def root():
        return {
            "name": "APEX",
            "status": "running",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "agents_registered": len(AGENT_REGISTRY),
            "agents_running": sum(1 for a in _agents_ref if getattr(a, "_running", False)),
            "paper_mode": settings.paper_trade_mode,
            "testnet": settings.hyperliquid_testnet,
            "portfolio_nav": _portfolio.get("total_nav", 0),
            "open_positions": len(_portfolio.get("positions", [])),
        }

    @app.get("/portfolio")
    async def portfolio():
        return _portfolio or {"message": "No portfolio data yet"}

    @app.get("/agents")
    async def agents_endpoint():
        result = []
        for entry in AGENT_REGISTRY.values():
            agent_data = {
                "agent_id": entry.agent_id,
                "type": entry.agent_type,
                "status": entry.status,
                "default_weight": entry.default_weight,
                "dynamic_weight": _agent_weights.get(entry.agent_id),
                "running": False,
            }
            for a in _agents_ref:
                if getattr(a, "agent_id", None) == entry.agent_id:
                    agent_data["running"] = getattr(a, "_running", False)
                    agent_data["health"] = a.health() if hasattr(a, "health") else {}
                    break
            result.append(agent_data)
        return result

    @app.get("/performance")
    async def performance():
        return _agent_performance

    @app.get("/performance/{agent_id}")
    async def performance_agent(agent_id: str):
        data = _agent_performance.get(agent_id)
        if data is None:
            return JSONResponse({"error": "Agent not found"}, status_code=404)
        return data

    @app.get("/signals")
    async def signals(limit: int = 50, offset: int = 0):
        items = list(_recent_signals)
        return items[offset:offset + limit]

    @app.get("/decisions")
    async def decisions(limit: int = 50, offset: int = 0):
        items = list(_recent_decisions)
        return items[offset:offset + limit]

    @app.get("/trades")
    async def trades(limit: int = 50, offset: int = 0):
        items = list(_recent_trades)
        return items[offset:offset + limit]

    @app.get("/risk")
    async def risk():
        return {
            "anomalies": list(_recent_anomalies),
            "portfolio_drawdown": _portfolio.get("drawdown", 0),
            "gross_exposure": _portfolio.get("gross_exposure", 0),
            "net_exposure": _portfolio.get("net_exposure", 0),
        }

    @app.get("/weights")
    async def weights():
        return _agent_weights

    @app.get("/leaderboard")
    async def leaderboard():
        return _agent_rankings

    @app.get("/agent/{agent_id}/profile")
    async def agent_profile(agent_id: str):
        data = _agent_rankings_by_id.get(agent_id)
        if data is None:
            return JSONResponse({"error": "Agent profile not found"}, status_code=404)
        return data

    @app.get("/journal")
    async def journal(limit: int = 50, offset: int = 0):
        items = list(_recent_journal)
        return items[offset:offset + limit]

    @app.get("/journal/{entry_id}")
    async def journal_entry(entry_id: str):
        for item in _recent_journal:
            if item.get("entry_id") == entry_id:
                return item
        return JSONResponse({"error": "Journal entry not found"}, status_code=404)

    @app.get("/metrics")
    async def metrics():
        if not _PROM_AVAILABLE:
            return PlainTextResponse("prometheus_client not installed", status_code=503)
        return PlainTextResponse(
            generate_latest().decode("utf-8"),
            media_type=CONTENT_TYPE_LATEST,
        )

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        _ws_clients.add(websocket)
        try:
            while True:
                # Keep connection alive; ignore incoming messages.
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            _ws_clients.discard(websocket)

    return app


# ---------------------------------------------------------------------------
# Entry-point for in-process startup
# ---------------------------------------------------------------------------

async def start_dashboard(
    bus: MessageBus,
    agents: list | None = None,
) -> None:
    """Start the dashboard as an async task within the APEX process."""
    import uvicorn

    app = create_app(bus=bus, agents=agents)
    config = uvicorn.Config(
        app,
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    logger.info(
        "Dashboard starting on http://%s:%d",
        settings.dashboard_host,
        settings.dashboard_port,
    )
    await server.serve()
