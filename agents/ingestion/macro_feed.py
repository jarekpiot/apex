"""
MacroFeed — centralised macro-economic data ingestion agent.

4 concurrent sub-tasks:
  a) FRED — 6 macro series from the Federal Reserve                every 1 hr
  b) Yahoo Finance — SPY, QQQ, GLD, USO, DX-Y.NYB, ^VIX          every 1 min (market open) / 5 min (closed)
  c) Fear & Greed — alternative.me /fng                            every 1 hr
  d) Economic Calendar — Forex Factory JSON feed                   every 1 hr

Publishes to ``data:macro``, persists to ``macro_indicators``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone, time as dt_time
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import MacroIndicatorRow, async_session_factory
from core.message_bus import MessageBus, STREAM_DATA_MACRO
from core.models import MacroDataPoint


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FRED_SERIES = ["FEDFUNDS", "CPIAUCSL", "WM2NS", "DGS10", "DGS2", "DTWEXBGS"]
_FRED_NAMES = {
    "FEDFUNDS": "fed_funds_rate",
    "CPIAUCSL": "cpi_yoy",
    "WM2NS": "m2_money_supply",
    "DGS10": "us10y_yield",
    "DGS2": "us2y_yield",
    "DTWEXBGS": "dxy_index",
}

_YFINANCE_TICKERS = ["SPY", "QQQ", "GLD", "USO", "DX-Y.NYB", "^VIX"]
_TICKER_FIELDS = {
    "SPY": "spy_price",
    "QQQ": "qqq_price",
    "GLD": "gld_price",
    "USO": "uso_price",
    "DX-Y.NYB": "dxy_price",
    "^VIX": "vix_price",
}

# High-impact events to flag 24h ahead.
_HIGH_IMPACT_KEYWORDS = {"FOMC", "CPI", "NFP", "Non-Farm", "GDP", "Federal Reserve"}


class MacroFeed(BaseAgent):
    """Centralised macro-economic data ingestion."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="macro_feed",
            agent_type="ingestion",
            bus=bus,
            **kw,
        )
        self._http: httpx.AsyncClient | None = None
        self._sub_tasks: list[asyncio.Task[None]] = []
        self._data = MacroDataPoint()
        self._backoff: dict[str, float] = {}

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=30.0)
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._fred_loop(), name="mf:fred"),
            asyncio.create_task(self._yfinance_loop(), name="mf:yfinance"),
            asyncio.create_task(self._fear_greed_loop(), name="mf:fear_greed"),
            asyncio.create_task(self._calendar_loop(), name="mf:calendar"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        if self._http:
            await self._http.aclose()
            self._http = None
        await super().stop()

    async def process(self) -> None:
        """Main loop publishes the latest macro snapshot."""
        await asyncio.sleep(30)
        self._data.timestamp = datetime.now(timezone.utc)
        self._data.us_market_open = self._is_us_market_open()
        await self.bus.publish_to(STREAM_DATA_MACRO, self._data)

    # -- helpers -------------------------------------------------------------

    async def _get_json(self, url: str, source: str) -> Any:
        assert self._http is not None
        resp = await self._http.get(url)
        if resp.status_code == 429 or resp.status_code >= 500:
            delay = self._backoff.get(source, 1.0)
            self._backoff[source] = min(delay * 2, 60.0)
            self.log.warning("%s rate-limited/error (%d), backoff %.0fs", source, resp.status_code, delay)
            await asyncio.sleep(delay)
            return None
        resp.raise_for_status()
        self._backoff.pop(source, None)
        return resp.json()

    async def _persist_indicators(self, rows: list[MacroIndicatorRow]) -> None:
        if not rows:
            return
        async with async_session_factory() as session:
            session.add_all(rows)
            await session.commit()

    @staticmethod
    def _is_us_market_open() -> bool:
        """Check if US stock market is open (Mon-Fri 9:30-16:00 ET)."""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            # Fallback: approximate with UTC-5.
            utc_now = datetime.now(timezone.utc)
            et_hour = (utc_now.hour - 5) % 24
            weekday = utc_now.weekday()
            if weekday >= 5:
                return False
            return (et_hour == 9 and utc_now.minute >= 30) or (10 <= et_hour < 16)

        et = ZoneInfo("America/New_York")
        now_et = datetime.now(et)
        if now_et.weekday() >= 5:
            return False
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        return market_open <= now_et.time() < market_close

    # -- (a) FRED ------------------------------------------------------------

    async def _fred_loop(self) -> None:
        while True:
            try:
                await self._fetch_fred()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("FRED fetch failed.")
            await asyncio.sleep(settings.macro_feed_fred_interval)

    async def _fetch_fred(self) -> None:
        api_key = settings.fred_api_key.get_secret_value()
        if not api_key:
            self.log.debug("FRED API key not configured, skipping.")
            return

        now = datetime.now(timezone.utc)
        db_rows: list[MacroIndicatorRow] = []

        async def _fetch_series(series_id: str) -> tuple[str, float | None]:
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id={series_id}&api_key={api_key}"
                f"&file_type=json&sort_order=desc&limit=1"
            )
            data = await self._get_json(url, f"fred_{series_id}")
            if data and data.get("observations"):
                val = data["observations"][0].get("value", ".")
                if val != ".":
                    return series_id, float(val)
            return series_id, None

        results = await asyncio.gather(
            *[_fetch_series(sid) for sid in _FRED_SERIES],
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, Exception):
                self.log.warning("FRED series fetch error: %s", result)
                continue
            series_id, value = result
            if value is None:
                continue
            field = _FRED_NAMES.get(series_id)
            if field:
                setattr(self._data, field, value)
            db_rows.append(MacroIndicatorRow(
                ts=now, source="fred", indicator_name=series_id,
                indicator_value=value,
            ))

        # Compute yield curve spread.
        if self._data.us10y_yield is not None and self._data.us2y_yield is not None:
            self._data.yield_curve_spread = self._data.us10y_yield - self._data.us2y_yield

        await self._persist_indicators(db_rows)
        self.log.info("FRED updated: %d series", len(db_rows))

    # -- (b) Yahoo Finance ---------------------------------------------------

    async def _yfinance_loop(self) -> None:
        while True:
            try:
                await self._fetch_yfinance()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("yfinance fetch failed.")
            interval = settings.macro_feed_yfinance_interval
            if not self._is_us_market_open():
                interval = max(interval, 300)  # 5 min when closed
            await asyncio.sleep(interval)

    async def _fetch_yfinance(self) -> None:
        loop = asyncio.get_running_loop()
        prices = await loop.run_in_executor(None, self._sync_fetch_yfinance)
        if not prices:
            return

        now = datetime.now(timezone.utc)
        db_rows: list[MacroIndicatorRow] = []

        for ticker, price in prices.items():
            field = _TICKER_FIELDS.get(ticker)
            if field:
                setattr(self._data, field, price)
            db_rows.append(MacroIndicatorRow(
                ts=now, source="yfinance", indicator_name=ticker,
                indicator_value=price,
            ))

        await self._persist_indicators(db_rows)
        self.log.info("yfinance updated: %d tickers", len(prices))

    @staticmethod
    def _sync_fetch_yfinance() -> dict[str, float]:
        """Synchronous yfinance call — run via executor."""
        try:
            import yfinance as yf
        except ImportError:
            return {}

        prices: dict[str, float] = {}
        for ticker in _YFINANCE_TICKERS:
            try:
                t = yf.Ticker(ticker)
                info = t.fast_info
                price = getattr(info, "last_price", None)
                if price is None:
                    price = getattr(info, "previous_close", None)
                if price is not None:
                    prices[ticker] = float(price)
            except Exception:
                pass
        return prices

    # -- (c) Fear & Greed ----------------------------------------------------

    async def _fear_greed_loop(self) -> None:
        while True:
            try:
                await self._fetch_fear_greed()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Fear & Greed fetch failed.")
            await asyncio.sleep(settings.onchain_fear_greed_interval)

    async def _fetch_fear_greed(self) -> None:
        data = await self._get_json("https://api.alternative.me/fng/?limit=1", "alternative_fng")
        if not data:
            return
        entries = data.get("data", [])
        if entries:
            idx = int(entries[0].get("value", 0))
            self._data.crypto_fear_greed = idx

            now = datetime.now(timezone.utc)
            await self._persist_indicators([MacroIndicatorRow(
                ts=now, source="alternative.me", indicator_name="crypto_fear_greed",
                indicator_value=float(idx),
            )])
            self.log.info("Crypto Fear & Greed: %d", idx)

    # -- (d) Economic Calendar -----------------------------------------------

    async def _calendar_loop(self) -> None:
        while True:
            try:
                await self._fetch_calendar()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Economic calendar fetch failed.")
            await asyncio.sleep(settings.macro_feed_calendar_interval)

    async def _fetch_calendar(self) -> None:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        data = await self._get_json(url, "forex_factory")
        if not data or not isinstance(data, list):
            return

        now = datetime.now(timezone.utc)
        upcoming: list[dict[str, Any]] = []

        for event in data:
            title = event.get("title", "")
            date_str = event.get("date", "")
            impact = event.get("impact", "")

            # Only care about high-impact events.
            if impact not in ("High", "high"):
                continue

            try:
                event_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except (ValueError, TypeError, AttributeError):
                continue

            delta = event_dt - now
            if timedelta(0) < delta < timedelta(hours=48):
                entry = {
                    "title": title,
                    "date": date_str,
                    "impact": impact,
                    "country": event.get("country", ""),
                }
                upcoming.append(entry)

                # Flag key events 24h ahead.
                if delta < timedelta(hours=24):
                    if any(kw.lower() in title.lower() for kw in _HIGH_IMPACT_KEYWORDS):
                        self.log.warning("HIGH-IMPACT EVENT in <24h: %s (%s)", title, date_str)

        self._data.upcoming_events = upcoming
        self.log.info("Economic calendar: %d upcoming high-impact events", len(upcoming))
