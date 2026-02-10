"""
MarketDataCollector — real-time Hyperliquid WebSocket ingestor.

Connects to the HL WebSocket, subscribes to allMids / l2Book / trades /
userFills, builds OHLCV candles at multiple timeframes, persists to
TimescaleDB, and publishes live price updates to Redis.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import websockets
import websockets.asyncio.client
from sqlalchemy import text

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import async_session_factory, OHLCVRow
from core.message_bus import (
    MessageBus,
    STREAM_MARKET_PRICES,
)
from core.models import PriceUpdate

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WS_MAINNET = "wss://api.hyperliquid.xyz/ws"
_WS_TESTNET = "wss://api.hyperliquid-testnet.xyz/ws"

_TIMEFRAMES: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

# Reconnect parameters.
_BASE_BACKOFF = 1.0
_MAX_BACKOFF = 60.0
_BACKOFF_FACTOR = 2.0

# How many top-volume assets get L2 book subscriptions.
_L2_TOP_N = 20


# ---------------------------------------------------------------------------
# Candle accumulator
# ---------------------------------------------------------------------------

class _CandleBuffer:
    """Accumulates ticks into OHLCV candles for a single (asset, timeframe)."""

    __slots__ = (
        "asset", "tf_label", "tf_secs", "open", "high", "low", "close",
        "volume", "period_start", "tick_count",
    )

    def __init__(self, asset: str, tf_label: str, tf_secs: int) -> None:
        self.asset = asset
        self.tf_label = tf_label
        self.tf_secs = tf_secs
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0.0
        self.tick_count = 0
        self.period_start = self._current_period_start()

    def _current_period_start(self) -> float:
        now = time.time()
        return now - (now % self.tf_secs)

    def update(self, price: float, volume: float = 0.0) -> OHLCVRow | None:
        """Feed a tick; returns a completed OHLCVRow when the period rolls."""
        current_period = self._current_period_start()

        # Period rolled — emit the completed candle and start fresh.
        completed: OHLCVRow | None = None
        if current_period > self.period_start and self.tick_count > 0:
            completed = OHLCVRow(
                ts=datetime.fromtimestamp(self.period_start, tz=timezone.utc),
                asset=self.asset,
                timeframe=self.tf_label,
                open=self.open,
                high=self.high,
                low=self.low,
                close=self.close,
                volume=self.volume,
            )
            self.tick_count = 0
            self.volume = 0.0
            self.period_start = current_period

        # Accumulate into the current candle.
        if self.tick_count == 0:
            self.open = self.high = self.low = self.close = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
            self.close = price
        self.volume += volume
        self.tick_count += 1

        return completed


# ---------------------------------------------------------------------------
# MarketDataCollector agent
# ---------------------------------------------------------------------------

class MarketDataCollector(BaseAgent):
    """Streams Hyperliquid market data and builds multi-timeframe OHLCV candles."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="market_data",
            agent_type="ingestion",
            bus=bus,
            **kw,
        )
        self._ws_url = _WS_TESTNET if settings.hyperliquid_testnet else _WS_MAINNET
        self._ws: Any = None

        # {(asset, tf_label): _CandleBuffer}
        self._candles: dict[tuple[str, str], _CandleBuffer] = {}

        # Latest mid-prices for all assets.
        self._mid_prices: dict[str, float] = {}

        # Assets currently subscribed to L2/trades.
        self._subscribed_assets: set[str] = set()

        # Trade-volume tracker for ranking assets (asset -> rolling 24h vol).
        self._volume_24h: dict[str, float] = defaultdict(float)

        # Backoff state.
        self._backoff = _BASE_BACKOFF

        # Persistent tasks besides the main loop.
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle overrides -------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks.append(
            asyncio.create_task(self._flush_candles_loop(), name="candle_flush")
        )

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        if self._ws:
            await self._ws.close()
            self._ws = None
        await super().stop()

    # -- main processing loop ------------------------------------------------

    async def process(self) -> None:
        """Connect to the WS and consume messages until disconnect."""
        try:
            await self._connect_and_subscribe()
            self._backoff = _BASE_BACKOFF  # Reset on successful connect.
            await self._consume_messages()
        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.InvalidURI,
            OSError,
        ) as exc:
            self.log.warning(
                "WebSocket disconnected: %s — reconnecting in %.1fs",
                exc, self._backoff,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("Unexpected error in WS loop.")

        # Exponential backoff before retry.
        await asyncio.sleep(self._backoff)
        self._backoff = min(self._backoff * _BACKOFF_FACTOR, _MAX_BACKOFF)

    # -- WebSocket management ------------------------------------------------

    async def _connect_and_subscribe(self) -> None:
        """Open WS and send subscription messages."""
        self.log.info("Connecting to %s", self._ws_url)
        self._ws = await websockets.asyncio.client.connect(
            self._ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        self.log.info("WebSocket connected.")

        # Always subscribe to all mid-prices.
        await self._send({"method": "subscribe", "subscription": {"type": "allMids"}})

        # Subscribe to userFills if we have a vault address.
        if settings.hyperliquid_vault_address:
            await self._send({
                "method": "subscribe",
                "subscription": {
                    "type": "userFills",
                    "user": settings.hyperliquid_vault_address,
                },
            })

        # L2 + trades for top-N assets will be subscribed once we have prices.

    async def _subscribe_top_assets(self, top_assets: list[str]) -> None:
        """Subscribe to L2 book and trades for the top-volume assets."""
        new = set(top_assets) - self._subscribed_assets
        stale = self._subscribed_assets - set(top_assets)

        for coin in stale:
            await self._send({
                "method": "unsubscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            })
            await self._send({
                "method": "unsubscribe",
                "subscription": {"type": "trades", "coin": coin},
            })
            self._subscribed_assets.discard(coin)

        for coin in new:
            await self._send({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin},
            })
            await self._send({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": coin},
            })
            self._subscribed_assets.add(coin)

        if new or stale:
            self.log.info(
                "L2/trades subscriptions updated: +%d -%d (total %d)",
                len(new), len(stale), len(self._subscribed_assets),
            )

    async def _send(self, msg: dict[str, Any]) -> None:
        if self._ws:
            await self._ws.send(json.dumps(msg))

    # -- message consumption -------------------------------------------------

    async def _consume_messages(self) -> None:
        """Read WS frames and dispatch by channel."""
        last_top_refresh = 0.0
        async for raw in self._ws:
            msg = json.loads(raw)
            channel = msg.get("channel")
            data = msg.get("data")

            if channel == "allMids":
                await self._handle_all_mids(data)

                # Periodically refresh top-asset subscriptions (every 60s).
                now = time.time()
                if now - last_top_refresh > 60:
                    top = self._rank_top_assets(_L2_TOP_N)
                    if top:
                        await self._subscribe_top_assets(top)
                    last_top_refresh = now

            elif channel == "l2Book":
                self._handle_l2_book(data)
            elif channel == "trades":
                self._handle_trades(data)
            elif channel == "userFills":
                self._handle_user_fills(data)
            elif channel == "subscriptionResponse":
                self.log.debug("Subscription ack: %s", data)
            else:
                self.log.debug("Unknown channel: %s", channel)

    # -- handlers ------------------------------------------------------------

    async def _handle_all_mids(self, data: dict[str, Any]) -> None:
        """Process allMids update: update prices + candle buffers + publish."""
        mids: dict[str, str] = data.get("mids", {})
        prices: dict[str, float] = {}

        for asset, price_str in mids.items():
            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            prices[asset] = price
            self._mid_prices[asset] = price

            # Feed every candle timeframe.
            for tf_label, tf_secs in _TIMEFRAMES.items():
                key = (asset, tf_label)
                buf = self._candles.get(key)
                if buf is None:
                    buf = _CandleBuffer(asset, tf_label, tf_secs)
                    self._candles[key] = buf
                completed = buf.update(price)
                if completed is not None:
                    await self._persist_candle(completed)

        # Publish live prices to Redis.
        if prices:
            update = PriceUpdate(prices=prices)
            await self.bus.publish_to(STREAM_MARKET_PRICES, update)

    def _handle_l2_book(self, data: dict[str, Any]) -> None:
        """Process L2 book snapshot — stored in-memory for depth queries."""
        coin = data.get("coin", "")
        levels = data.get("levels", [[], []])
        bid_depth = sum(float(lv.get("sz", 0)) for lv in levels[0][:10]) if levels[0] else 0
        ask_depth = sum(float(lv.get("sz", 0)) for lv in levels[1][:10]) if levels[1] else 0
        mid = self._mid_prices.get(coin, 0)
        if mid:
            bid_usd = bid_depth * mid
            ask_usd = ask_depth * mid
            self.log.debug(
                "L2 %s: bid_depth=$%.0f ask_depth=$%.0f",
                coin, bid_usd, ask_usd,
            )

    def _handle_trades(self, data: list[dict[str, Any]]) -> None:
        """Process trade prints — accumulate volume for ranking."""
        if not isinstance(data, list):
            return
        for trade in data:
            coin = trade.get("coin", "")
            sz = float(trade.get("sz", 0))
            px = float(trade.get("px", 0))
            self._volume_24h[coin] += sz * px

            # Update candle volume.
            for tf_label in _TIMEFRAMES:
                buf = self._candles.get((coin, tf_label))
                if buf:
                    buf.volume += sz * px

    def _handle_user_fills(self, data: dict[str, Any]) -> None:
        """Log user fills — full handling deferred to ExecutionEngine."""
        fills = data.get("fills", []) if isinstance(data, dict) else data
        if not isinstance(fills, list):
            return
        for fill in fills:
            self.log.info(
                "User fill: %s %s %.6f @ %.2f",
                fill.get("side", "?"),
                fill.get("coin", "?"),
                float(fill.get("sz", 0)),
                float(fill.get("px", 0)),
            )

    # -- helpers -------------------------------------------------------------

    def _rank_top_assets(self, n: int) -> list[str]:
        """Return top-N assets by accumulated trade volume."""
        if not self._volume_24h:
            # Fallback: use assets with highest mid-prices (proxy for liquidity).
            sorted_assets = sorted(
                self._mid_prices.items(), key=lambda kv: kv[1], reverse=True,
            )
            return [a for a, _ in sorted_assets[:n]]
        sorted_vol = sorted(
            self._volume_24h.items(), key=lambda kv: kv[1], reverse=True,
        )
        return [a for a, _ in sorted_vol[:n]]

    async def _persist_candle(self, row: OHLCVRow) -> None:
        """Write a completed OHLCV candle to TimescaleDB."""
        try:
            async with async_session_factory() as session:
                session.add(row)
                await session.commit()
        except Exception:
            self.log.exception(
                "Failed to persist candle %s/%s", row.asset, row.timeframe,
            )

    async def _flush_candles_loop(self) -> None:
        """Periodically force-flush candle buffers that have ticks but haven't
        rolled naturally (e.g. low-volume assets on long timeframes)."""
        while True:
            await asyncio.sleep(60)
            now_ts = time.time()
            for key, buf in list(self._candles.items()):
                period_end = buf.period_start + buf.tf_secs
                if now_ts >= period_end and buf.tick_count > 0:
                    row = OHLCVRow(
                        ts=datetime.fromtimestamp(buf.period_start, tz=timezone.utc),
                        asset=buf.asset,
                        timeframe=buf.tf_label,
                        open=buf.open,
                        high=buf.high,
                        low=buf.low,
                        close=buf.close,
                        volume=buf.volume,
                    )
                    await self._persist_candle(row)
                    buf.tick_count = 0
                    buf.volume = 0.0
                    buf.period_start = buf._current_period_start()

    # -- health override -----------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "ws_connected": self._ws is not None and self._ws.open if self._ws else False,
            "assets_tracked": len(self._mid_prices),
            "l2_subscriptions": len(self._subscribed_assets),
            "candle_buffers": len(self._candles),
        })
        return base
