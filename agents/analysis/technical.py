"""
TechnicalAnalyst — multi-timeframe technical analysis across all Hyperliquid perps.

Uses pandas-ta to compute: RSI, MACD, Bollinger Bands, ATR, EMA crossovers,
Volume profile (POC/VAH/VAL), Support/Resistance from swing highs/lows,
Stochastic RSI, ADX for trend strength.

Combines into a composite signal: direction (-1 to +1) and conviction (0 to 1).

Signal logic:
  - Strong trend (ADX>25 + EMA aligned + RSI not extreme) → high conviction directional
  - Mean reversion (RSI extreme + price at Bollinger band) → moderate conviction counter-trend
  - Conflicting signals → low conviction, skip

Runs every 1 minute for short TFs (5m, 15m), every 5 min for higher TFs (1h, 4h, 1d).
Configurable asset watchlist (top N by volume).
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import pandas_ta as ta
from sqlalchemy import select

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import OHLCVRow, async_session_factory
from core.message_bus import (
    MessageBus,
    STREAM_MARKET_PRICES,
    STREAM_SIGNALS_TECHNICAL,
)
from core.models import AgentSignal, PriceUpdate, Timeframe

# ---------------------------------------------------------------------------
# Timeframe mapping
# ---------------------------------------------------------------------------

_TF_TO_SIGNAL: dict[str, Timeframe] = {
    "5m": Timeframe.SCALP,
    "15m": Timeframe.SCALP,
    "1h": Timeframe.INTRADAY,
    "4h": Timeframe.INTRADAY,
    "1d": Timeframe.SWING,
}

_SHORT_TFS = ["5m", "15m"]
_LONG_TFS = ["1h", "4h", "1d"]


class TechnicalAnalyst(BaseAgent):
    """Multi-timeframe technical analysis engine for all HL perps."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="technical_analyst",
            agent_type="technical",
            bus=bus,
            **kw,
        )
        self._mid_prices: dict[str, float] = {}
        self._watchlist: list[str] = []
        self._last_watchlist_update: float = 0.0
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._price_listener(), name="ta:prices"),
            asyncio.create_task(self._short_tf_loop(), name="ta:short"),
            asyncio.create_task(self._long_tf_loop(), name="ta:long"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        await super().stop()

    async def process(self) -> None:
        """Main loop idles — real work runs in sub-tasks."""
        await asyncio.sleep(3600)

    # -- watchlist & price tracking ------------------------------------------

    async def _price_listener(self) -> None:
        """Track all mid-prices and maintain a watchlist of top assets."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="technical_analyst",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)

                now = time.monotonic()
                if now - self._last_watchlist_update > 300:
                    self._update_watchlist()
                    self._last_watchlist_update = now
            except Exception:
                pass

    def _update_watchlist(self) -> None:
        """Select top N assets by price (proxy for liquidity/volume)."""
        sorted_assets = sorted(
            self._mid_prices.keys(),
            key=lambda a: self._mid_prices.get(a, 0),
            reverse=True,
        )
        self._watchlist = sorted_assets[: settings.ta_watchlist_size]
        if self._watchlist:
            self.log.info("Watchlist updated: %d assets", len(self._watchlist))

    # -- analysis loops ------------------------------------------------------

    async def _short_tf_loop(self) -> None:
        """Analyze short timeframes every ta_short_tf_interval seconds."""
        await asyncio.sleep(30)  # Let candles accumulate.
        while True:
            try:
                for tf in _SHORT_TFS:
                    await self._analyze_timeframe(tf)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Short TF analysis failed.")
            await asyncio.sleep(settings.ta_short_tf_interval)

    async def _long_tf_loop(self) -> None:
        """Analyze long timeframes every ta_long_tf_interval seconds."""
        await asyncio.sleep(60)
        while True:
            try:
                for tf in _LONG_TFS:
                    await self._analyze_timeframe(tf)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Long TF analysis failed.")
            await asyncio.sleep(settings.ta_long_tf_interval)

    # -- core analysis -------------------------------------------------------

    async def _analyze_timeframe(self, tf: str) -> None:
        """Run TA on all watchlist assets for a single timeframe."""
        if not self._watchlist:
            return

        candles = await self._fetch_candles_batch(self._watchlist, tf)

        for asset in self._watchlist:
            df = candles.get(asset)
            if df is None or len(df) < settings.ta_min_candles:
                continue
            try:
                indicators = self._compute_indicators(df)
                direction, conviction, reasoning = self._composite_signal(
                    asset, df, indicators,
                )
                if conviction < 0.1:
                    continue

                signal_tf = _TF_TO_SIGNAL.get(tf, Timeframe.INTRADAY)
                expires = datetime.now(timezone.utc) + _expiry_for_tf(tf)

                signal = AgentSignal(
                    agent_id=self.agent_id,
                    asset=asset,
                    direction=direction,
                    conviction=conviction,
                    timeframe=signal_tf,
                    reasoning=reasoning,
                    data_sources=["ohlcv:%s" % tf],
                    metadata={
                        "timeframe": tf,
                        "indicators": self._indicator_summary(indicators),
                    },
                    expires_at=expires,
                )
                await self.emit_signal(signal)
                await self.bus.publish_to(STREAM_SIGNALS_TECHNICAL, signal)
                self._signals_emitted += 1
            except Exception:
                self.log.exception("Analysis failed for %s/%s", asset, tf)

    # -- data fetching -------------------------------------------------------

    async def _fetch_candles_batch(
        self, assets: list[str], tf: str,
    ) -> dict[str, pd.DataFrame]:
        """Query OHLCV candles from TimescaleDB for all watchlist assets."""
        lookback = settings.ta_min_candles + 50  # Extra for indicator warm-up.

        async with async_session_factory() as session:
            stmt = (
                select(OHLCVRow)
                .where(OHLCVRow.asset.in_(assets))
                .where(OHLCVRow.timeframe == tf)
                .order_by(OHLCVRow.ts.desc())
                .limit(len(assets) * lookback)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

        by_asset: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_asset[row.asset].append({
                "ts": row.ts,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
            })

        dfs: dict[str, pd.DataFrame] = {}
        for asset, records in by_asset.items():
            df = pd.DataFrame(records).sort_values("ts").reset_index(drop=True)
            dfs[asset] = df
        return dfs

    # -- indicator computation -----------------------------------------------

    def _compute_indicators(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute all technical indicators on an OHLCV DataFrame."""
        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        indicators: dict[str, Any] = {}

        # RSI(14)
        indicators["rsi"] = ta.rsi(close, length=14)

        # MACD(12,26,9) → columns: MACD, histogram, signal
        macd_df = ta.macd(close, fast=12, slow=26, signal=9)
        if macd_df is not None:
            indicators["macd"] = macd_df.iloc[:, 0]
            indicators["macd_hist"] = macd_df.iloc[:, 1]
            indicators["macd_signal"] = macd_df.iloc[:, 2]

        # Bollinger Bands(20,2) → columns: lower, mid, upper, bandwidth, %b
        bb_df = ta.bbands(close, length=20, std=2)
        if bb_df is not None:
            indicators["bb_lower"] = bb_df.iloc[:, 0]
            indicators["bb_mid"] = bb_df.iloc[:, 1]
            indicators["bb_upper"] = bb_df.iloc[:, 2]
            if bb_df.shape[1] > 3:
                indicators["bb_bandwidth"] = bb_df.iloc[:, 3]
            if bb_df.shape[1] > 4:
                indicators["bb_pct"] = bb_df.iloc[:, 4]

        # ATR(14)
        indicators["atr"] = ta.atr(high, low, close, length=14)

        # EMAs for crossover detection
        indicators["ema9"] = ta.ema(close, length=9)
        indicators["ema21"] = ta.ema(close, length=21)
        indicators["ema50"] = ta.ema(close, length=50)
        indicators["ema200"] = ta.ema(close, length=200)

        # ADX(14) → columns: ADX, DI+, DI-
        adx_df = ta.adx(high, low, close, length=14)
        if adx_df is not None:
            indicators["adx"] = adx_df.iloc[:, 0]
            indicators["dmp"] = adx_df.iloc[:, 1]
            indicators["dmn"] = adx_df.iloc[:, 2]

        # Stochastic RSI → columns: %K, %D
        stoch_df = ta.stochrsi(close)
        if stoch_df is not None:
            indicators["stoch_k"] = stoch_df.iloc[:, 0]
            indicators["stoch_d"] = stoch_df.iloc[:, 1]

        # Volume profile (POC, VAH, VAL)
        indicators["volume_profile"] = self._volume_profile(df)

        # Support / Resistance from swing highs/lows
        indicators["support"], indicators["resistance"] = self._swing_levels(df)

        return indicators

    # -- volume profile ------------------------------------------------------

    @staticmethod
    def _volume_profile(df: pd.DataFrame, bins: int = 50) -> dict[str, float]:
        """Compute POC, VAH, VAL from the price-volume distribution."""
        total_vol = df["volume"].sum()
        if total_vol == 0:
            mid = df["close"].iloc[-1]
            return {"poc": mid, "vah": mid, "val": mid}

        price_min = float(df["low"].min())
        price_max = float(df["high"].max())
        if price_max == price_min:
            return {"poc": price_min, "vah": price_min, "val": price_min}

        bin_edges = np.linspace(price_min, price_max, bins + 1)
        bin_volumes = np.zeros(bins)

        highs = df["high"].values
        lows = df["low"].values
        vols = df["volume"].values

        for idx in range(len(df)):
            for b in range(bins):
                if highs[idx] >= bin_edges[b] and lows[idx] <= bin_edges[b + 1]:
                    bin_volumes[b] += vols[idx]

        poc_idx = int(np.argmax(bin_volumes))
        poc = float((bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2)

        # Value area: 70% of total volume centered on POC.
        target = float(bin_volumes.sum()) * 0.7
        lo, hi = poc_idx, poc_idx
        accumulated = float(bin_volumes[poc_idx])

        while accumulated < target and (lo > 0 or hi < bins - 1):
            expand_lo = float(bin_volumes[lo - 1]) if lo > 0 else 0.0
            expand_hi = float(bin_volumes[hi + 1]) if hi < bins - 1 else 0.0
            if expand_lo >= expand_hi and lo > 0:
                lo -= 1
                accumulated += float(bin_volumes[lo])
            elif hi < bins - 1:
                hi += 1
                accumulated += float(bin_volumes[hi])
            else:
                break

        val = float((bin_edges[lo] + bin_edges[lo + 1]) / 2)
        vah = float((bin_edges[hi] + bin_edges[hi + 1]) / 2)
        return {"poc": poc, "vah": vah, "val": val}

    # -- swing high/low detection --------------------------------------------

    @staticmethod
    def _swing_levels(
        df: pd.DataFrame, lookback: int = 5,
    ) -> tuple[list[float], list[float]]:
        """Detect recent swing highs and lows as S/R levels."""
        highs: list[float] = []
        lows: list[float] = []

        high_arr = df["high"].values
        low_arr = df["low"].values
        n = len(df)

        for i in range(lookback, n - lookback):
            window_high = high_arr[i - lookback : i + lookback + 1]
            if high_arr[i] == window_high.max():
                highs.append(float(high_arr[i]))
            window_low = low_arr[i - lookback : i + lookback + 1]
            if low_arr[i] == window_low.min():
                lows.append(float(low_arr[i]))

        # Return only the most recent levels.
        return lows[-5:], highs[-5:]

    # -- composite signal logic ----------------------------------------------

    def _composite_signal(
        self,
        asset: str,
        df: pd.DataFrame,
        indicators: dict[str, Any],
    ) -> tuple[float, float, str]:
        """Combine indicators into (direction, conviction, reasoning).

        direction: -1.0 (max short) to +1.0 (max long)
        conviction: 0.0 (uncertain) to 1.0 (very high)
        """
        price = float(df["close"].iloc[-1])
        reasons: list[str] = []
        scores: list[float] = []

        # Retrieve latest indicator values.
        adx_val = _safe_last(indicators.get("adx"))
        dmp_val = _safe_last(indicators.get("dmp"))
        dmn_val = _safe_last(indicators.get("dmn"))
        ema9 = _safe_last(indicators.get("ema9"))
        ema21 = _safe_last(indicators.get("ema21"))
        ema50 = _safe_last(indicators.get("ema50"))
        ema200 = _safe_last(indicators.get("ema200"))
        rsi = _safe_last(indicators.get("rsi"))
        macd_val = _safe_last(indicators.get("macd"))
        macd_hist = _safe_last(indicators.get("macd_hist"))
        macd_sig = _safe_last(indicators.get("macd_signal"))
        bb_upper = _safe_last(indicators.get("bb_upper"))
        bb_lower = _safe_last(indicators.get("bb_lower"))
        stoch_k = _safe_last(indicators.get("stoch_k"))
        stoch_d = _safe_last(indicators.get("stoch_d"))

        # ---- 1. EMA alignment ----
        ema_score = 0.0
        if all(v is not None for v in [ema9, ema21, ema50]):
            if ema9 > ema21 > ema50:
                ema_score = 0.8
                reasons.append("EMA bullish (9>21>50)")
            elif ema9 < ema21 < ema50:
                ema_score = -0.8
                reasons.append("EMA bearish (9<21<50)")

            if ema200 is not None:
                if price > ema200:
                    ema_score += 0.2
                else:
                    ema_score -= 0.2

                # Golden / death cross detection.
                ema50_prev = _safe_at(indicators.get("ema50"), -2)
                ema200_prev = _safe_at(indicators.get("ema200"), -2)
                if ema50_prev is not None and ema200_prev is not None:
                    if ema50 > ema200 and ema50_prev <= ema200_prev:
                        ema_score = 1.0
                        reasons.append("Golden cross (50>200)")
                    elif ema50 < ema200 and ema50_prev >= ema200_prev:
                        ema_score = -1.0
                        reasons.append("Death cross (50<200)")
        scores.append(ema_score)

        # ---- 2. MACD ----
        macd_score = 0.0
        if macd_val is not None and macd_sig is not None:
            if macd_val > macd_sig:
                macd_score = 0.5
            else:
                macd_score = -0.5

            if macd_hist is not None:
                prev_hist = _safe_at(indicators.get("macd_hist"), -2)
                if prev_hist is not None:
                    if macd_hist > 0 and macd_hist > prev_hist:
                        macd_score = 0.7
                        reasons.append("MACD bullish momentum")
                    elif macd_hist < 0 and macd_hist < prev_hist:
                        macd_score = -0.7
                        reasons.append("MACD bearish momentum")
        scores.append(macd_score)

        # ---- 3. RSI ----
        rsi_score = 0.0
        if rsi is not None:
            if rsi > settings.ta_rsi_overbought:
                rsi_score = -0.6
                reasons.append("RSI overbought (%.1f)" % rsi)
            elif rsi < settings.ta_rsi_oversold:
                rsi_score = 0.6
                reasons.append("RSI oversold (%.1f)" % rsi)
            elif rsi > 50:
                rsi_score = 0.2
            else:
                rsi_score = -0.2
        scores.append(rsi_score)

        # ---- 4. ADX / trend strength ----
        trend_score = 0.0
        if adx_val is not None and adx_val > settings.ta_adx_threshold:
            if dmp_val is not None and dmn_val is not None:
                if dmp_val > dmn_val:
                    trend_score = min(adx_val / 50.0, 1.0)
                    reasons.append("Strong uptrend (ADX=%.1f)" % adx_val)
                else:
                    trend_score = -min(adx_val / 50.0, 1.0)
                    reasons.append("Strong downtrend (ADX=%.1f)" % adx_val)
        scores.append(trend_score)

        # ---- 5. Stochastic RSI ----
        stoch_score = 0.0
        if stoch_k is not None:
            if stoch_k > 80:
                stoch_score = -0.3
            elif stoch_k < 20:
                stoch_score = 0.3
            if stoch_d is not None:
                if stoch_k > stoch_d and stoch_k < 50:
                    stoch_score += 0.2
                elif stoch_k < stoch_d and stoch_k > 50:
                    stoch_score -= 0.2
        scores.append(stoch_score)

        # ---- 6. Bollinger Band position ----
        bb_score = 0.0
        if bb_upper is not None and bb_lower is not None:
            if price >= bb_upper:
                bb_score = -0.4
                reasons.append("Price at upper BB")
            elif price <= bb_lower:
                bb_score = 0.4
                reasons.append("Price at lower BB")
        scores.append(bb_score)

        # ---- Composite direction ----
        if not scores:
            return 0.0, 0.0, "Insufficient indicator data"

        direction = sum(scores) / len(scores)
        direction = max(-1.0, min(1.0, direction))

        # ---- Agreement: how many indicators agree on direction ----
        nonzero = [s for s in scores if s != 0]
        if nonzero:
            signs = [1 if s > 0 else -1 for s in nonzero]
            agreement = abs(sum(signs)) / len(signs)
        else:
            agreement = 0.0

        # ---- Regime classification ----
        is_trending = adx_val is not None and adx_val > settings.ta_adx_threshold
        rsi_extreme = rsi is not None and (
            rsi > settings.ta_rsi_overbought or rsi < settings.ta_rsi_oversold
        )
        price_at_bb = bb_score != 0.0

        if is_trending and abs(ema_score) > 0.5 and not rsi_extreme:
            # Strong trend: high conviction directional.
            conviction = min(agreement * 1.2, 1.0)
            reasons.insert(0, "TREND regime")
        elif rsi_extreme and price_at_bb and not is_trending:
            # Mean reversion: moderate conviction counter-trend.
            conviction = min(agreement * 0.9, 0.8)
            reasons.insert(0, "MEAN-REVERSION regime")
        elif agreement < 0.3:
            # Conflicting signals: skip.
            conviction = 0.0
            reasons.insert(0, "CONFLICTING — skip")
        else:
            conviction = agreement * 0.7
            reasons.insert(0, "MODERATE signal")

        return direction, conviction, " | ".join(reasons[:6])

    # -- indicator summary for metadata --------------------------------------

    def _indicator_summary(self, indicators: dict[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key in ("rsi", "adx", "atr"):
            val = _safe_last(indicators.get(key))
            if val is not None:
                summary[key] = round(val, 2)

        vp = indicators.get("volume_profile")
        if vp:
            summary["vpoc"] = round(vp["poc"], 2)

        support = indicators.get("support", [])
        resistance = indicators.get("resistance", [])
        if support:
            summary["nearest_support"] = round(support[-1], 2)
        if resistance:
            summary["nearest_resistance"] = round(resistance[-1], 2)
        return summary

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "watchlist_size": len(self._watchlist),
            "assets_tracked": len(self._mid_prices),
            "signals_emitted": self._signals_emitted,
        })
        return base


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _safe_last(series: Any) -> float | None:
    """Safely extract the last value from a pandas Series."""
    if series is None:
        return None
    try:
        val = series.iloc[-1]
        if pd.isna(val):
            return None
        return float(val)
    except (IndexError, AttributeError):
        return None


def _safe_at(series: Any, idx: int) -> float | None:
    """Safely extract a value at index from a pandas Series."""
    if series is None:
        return None
    try:
        val = series.iloc[idx]
        if pd.isna(val):
            return None
        return float(val)
    except (IndexError, AttributeError):
        return None


def _expiry_for_tf(tf: str) -> timedelta:
    """How long a signal stays valid based on its timeframe."""
    return {
        "5m": timedelta(minutes=10),
        "15m": timedelta(minutes=30),
        "1h": timedelta(hours=2),
        "4h": timedelta(hours=8),
        "1d": timedelta(days=2),
    }.get(tf, timedelta(hours=1))
