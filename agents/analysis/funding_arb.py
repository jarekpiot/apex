"""
FundingRateArb — cross-exchange funding rate arbitrage scanner.

Scans funding rates on Hyperliquid and compares with Binance/Bybit via CCXT.
Identifies delta-neutral carry opportunities when the funding rate differential
is significant (>0.01% per 8h).

Strategy:
  - Go long on the exchange with negative funding, short on the one with positive
    funding (cross-exchange delta-neutral carry).
  - Single-leg on HL if the rate is extreme enough (>2× the funding_rate_extreme
    threshold) — collect funding while accepting directional risk.

Tracks funding rate history to avoid entering when rates are about to flip.
Calculates expected annualised carry yield and breakeven time.
Runs every 5 minutes.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_SIGNALS_FUNDING_ARB,
)
from core.models import AgentSignal, Timeframe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HL_INFO_URL = "https://api.hyperliquid.xyz/info"
_HL_TESTNET_INFO_URL = "https://api.hyperliquid-testnet.xyz/info"

# Funding rate is per 8 hours.  Annualised = rate × 3 × 365
_ANNUALISE_FACTOR = 3 * 365

# Round-trip trading cost estimate for breakeven calc (bps → fraction).
_ROUND_TRIP_COST = 10.0 / 10_000  # 10 bps


# ---------------------------------------------------------------------------
# Funding rate history tracker
# ---------------------------------------------------------------------------

class _FundingHistory:
    """Rolling window of (timestamp, rate) tuples for stability analysis."""

    __slots__ = ("_data",)

    def __init__(self, maxlen: int = 144) -> None:  # ~72 hours at 5-min samples
        self._data: deque[tuple[float, float]] = deque(maxlen=maxlen)

    def append(self, ts: float, rate: float) -> None:
        self._data.append((ts, rate))

    @property
    def count(self) -> int:
        return len(self._data)

    def rates(self) -> list[float]:
        return [r for _, r in self._data]

    def mean(self) -> float:
        rates = self.rates()
        return sum(rates) / len(rates) if rates else 0.0

    def std(self) -> float:
        rates = self.rates()
        if len(rates) < 2:
            return 0.0
        m = self.mean()
        var = sum((r - m) ** 2 for r in rates) / (len(rates) - 1)
        return var ** 0.5

    def trend(self) -> float:
        """Linear-regression slope of recent rates.

        Positive → rates increasing, negative → rates decreasing.
        """
        rates = self.rates()
        n = len(rates)
        if n < 5:
            return 0.0
        x_mean = (n - 1) / 2.0
        y_mean = sum(rates) / n
        num = sum((i - x_mean) * (rates[i] - y_mean) for i in range(n))
        den = sum((i - x_mean) ** 2 for i in range(n))
        return num / den if den != 0 else 0.0

    def is_stable(self) -> bool:
        """Rate is stable if the last 10 samples haven't flipped sign."""
        rates = self.rates()
        if len(rates) < 10:
            return False
        recent = rates[-10:]
        return all(r > 0 for r in recent) or all(r < 0 for r in recent)


# ---------------------------------------------------------------------------
# FundingRateArb agent
# ---------------------------------------------------------------------------

class FundingRateArb(BaseAgent):
    """Cross-exchange funding rate arbitrage scanner."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="funding_arb",
            agent_type="technical",
            bus=bus,
            **kw,
        )
        self._hl_funding: dict[str, float] = {}
        self._history: dict[str, _FundingHistory] = defaultdict(_FundingHistory)
        self._binance_funding: dict[str, float] = {}
        self._bybit_funding: dict[str, float] = {}
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []
        self._info_url = (
            _HL_TESTNET_INFO_URL if settings.hyperliquid_testnet
            else _HL_INFO_URL
        )

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="farb:scan"),
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
        """Main loop idles — real work in _scan_loop."""
        await asyncio.sleep(3600)

    # -- scan loop -----------------------------------------------------------

    async def _scan_loop(self) -> None:
        await asyncio.sleep(10)  # Let infrastructure settle.
        while True:
            try:
                await self._scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Funding arb scan failed.")
            await asyncio.sleep(settings.funding_arb_scan_interval)

    async def _scan(self) -> None:
        """Fetch funding rates from all sources and evaluate opportunities."""
        hl_task = self._fetch_hl_funding()
        binance_task = self._fetch_ccxt_funding("binance")
        bybit_task = self._fetch_ccxt_funding("bybit")

        hl_rates, binance_rates, bybit_rates = await asyncio.gather(
            hl_task, binance_task, bybit_task,
            return_exceptions=True,
        )

        if isinstance(hl_rates, dict):
            self._hl_funding = hl_rates
        else:
            self.log.warning("Failed to fetch HL funding: %s", hl_rates)
            return

        if isinstance(binance_rates, dict):
            self._binance_funding = binance_rates
        if isinstance(bybit_rates, dict):
            self._bybit_funding = bybit_rates

        # Update histories.
        now = time.time()
        for asset, rate in self._hl_funding.items():
            self._history[asset].append(now, rate)

        # Evaluate every HL asset.
        for asset, hl_rate in self._hl_funding.items():
            await self._evaluate(asset, hl_rate)

    # -- opportunity evaluation ----------------------------------------------

    async def _evaluate(self, asset: str, hl_rate: float) -> None:
        min_diff = settings.funding_arb_min_rate_diff
        history = self._history[asset]

        # --- Cross-exchange arb ---
        for exchange_name, rates in [
            ("binance", self._binance_funding),
            ("bybit", self._bybit_funding),
        ]:
            other_rate = rates.get(asset)
            if other_rate is None:
                continue

            diff = hl_rate - other_rate
            if abs(diff) < min_diff:
                continue

            # Skip if the rate is unstable (about to flip).
            if history.count >= 10 and not history.is_stable():
                continue

            annualised = abs(diff) * _ANNUALISE_FACTOR
            breakeven_h = self._calc_breakeven(abs(diff))

            # Direction on HL side: short if HL rate is higher (longs pay),
            # long if HL rate is lower (shorts pay).
            if diff > 0:
                direction = -1.0
                desc = "Short %s on HL (%+.4f%%), long on %s (%+.4f%%)" % (
                    asset, hl_rate * 100, exchange_name, other_rate * 100,
                )
            else:
                direction = 1.0
                desc = "Long %s on HL (%+.4f%%), short on %s (%+.4f%%)" % (
                    asset, hl_rate * 100, exchange_name, other_rate * 100,
                )

            conviction = self._calc_conviction(
                annualised, history, is_cross_exchange=True,
            )
            if conviction < 0.1:
                continue

            signal = AgentSignal(
                agent_id=self.agent_id,
                asset=asset,
                direction=direction,
                conviction=conviction,
                timeframe=Timeframe.INTRADAY,
                reasoning="Funding arb: %s | Ann. yield %.1f%% | Breakeven %dh" % (
                    desc, annualised * 100, breakeven_h,
                ),
                data_sources=["funding:hyperliquid", "funding:%s" % exchange_name],
                metadata={
                    "strategy": "cross_exchange_arb",
                    "hl_rate": hl_rate,
                    "other_rate": other_rate,
                    "other_exchange": exchange_name,
                    "rate_diff": diff,
                    "annualised_yield": annualised,
                    "breakeven_hours": breakeven_h,
                    "rate_stable": history.is_stable(),
                },
                expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
            )
            await self.emit_signal(signal)
            await self.bus.publish_to(STREAM_SIGNALS_FUNDING_ARB, signal)
            self._signals_emitted += 1

            self.log.info(
                "FUNDING ARB: %s vs %s diff=%.4f%% yield=%.1f%% breakeven=%dh",
                asset, exchange_name, diff * 100, annualised * 100, breakeven_h,
            )

        # --- Single-leg on HL (extreme funding) ---
        if abs(hl_rate) > settings.funding_rate_extreme * 2:
            if history.count >= 10 and not history.is_stable():
                return

            annualised = abs(hl_rate) * _ANNUALISE_FACTOR

            if hl_rate > 0:
                direction = -1.0
                desc = "Single-leg short (longs pay %+.4f%%/8h)" % (hl_rate * 100,)
            else:
                direction = 1.0
                desc = "Single-leg long (shorts pay %+.4f%%/8h)" % (abs(hl_rate) * 100,)

            conviction = self._calc_conviction(
                annualised, history, is_cross_exchange=False,
            )
            if conviction < 0.15:
                return

            # Discount conviction for single-leg (directional risk).
            conviction *= 0.7

            signal = AgentSignal(
                agent_id=self.agent_id,
                asset=asset,
                direction=direction,
                conviction=conviction,
                timeframe=Timeframe.INTRADAY,
                reasoning="Single-leg funding: %s | Ann. yield %.1f%%" % (
                    desc, annualised * 100,
                ),
                data_sources=["funding:hyperliquid"],
                metadata={
                    "strategy": "single_leg_funding",
                    "hl_rate": hl_rate,
                    "annualised_yield": annualised,
                    "rate_trend": history.trend(),
                },
                expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
            )
            await self.emit_signal(signal)
            await self.bus.publish_to(STREAM_SIGNALS_FUNDING_ARB, signal)
            self._signals_emitted += 1

    # -- conviction calculation ----------------------------------------------

    @staticmethod
    def _calc_conviction(
        annualised_yield: float,
        history: _FundingHistory,
        *,
        is_cross_exchange: bool,
    ) -> float:
        """Conviction from yield magnitude, stability, and history depth."""
        # Base: 0–0.5 scaled linearly from 0–25% annualised yield.
        base = min(annualised_yield * 2, 0.5)

        # Stability bonus.
        stability_bonus = 0.2 if history.is_stable() else 0.0

        # Data depth bonus (max 0.2 at ≥50 samples).
        data_bonus = min(history.count / 50, 0.2)

        # Trend penalty: if rate is converging toward zero, reduce conviction.
        trend_penalty = 0.0
        mean_rate = history.mean()
        if mean_rate != 0:
            trend = history.trend()
            if (mean_rate > 0 and trend < 0) or (mean_rate < 0 and trend > 0):
                trend_penalty = min(abs(trend) / abs(mean_rate) * 0.3, 0.3)

        # Cross-exchange gets a safety bonus (delta-neutral).
        cross_bonus = 0.15 if is_cross_exchange else 0.0

        conviction = base + stability_bonus + data_bonus + cross_bonus - trend_penalty
        return max(0.0, min(1.0, conviction))

    @staticmethod
    def _calc_breakeven(rate_diff: float) -> float:
        """Hours until trading costs are recouped by funding income."""
        if rate_diff == 0:
            return float("inf")
        periods = _ROUND_TRIP_COST / rate_diff  # Number of 8-hour periods.
        return periods * 8.0

    # -- data fetching -------------------------------------------------------

    async def _fetch_hl_funding(self) -> dict[str, float]:
        """Fetch current funding rates from Hyperliquid."""
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                self._info_url,
                json={"type": "metaAndAssetCtxs"},
            )
            resp.raise_for_status()
            data = resp.json()

        rates: dict[str, float] = {}

        # data == [meta_dict, [asset_ctx, ...]]
        if isinstance(data, list) and len(data) >= 2:
            meta = data[0]
            asset_ctxs = data[1]
            universe = meta.get("universe", [])
            for i, ctx in enumerate(asset_ctxs):
                if i < len(universe):
                    name = universe[i].get("name", "")
                    funding = ctx.get("funding")
                    if funding is not None:
                        try:
                            rates[name] = float(funding)
                        except (ValueError, TypeError):
                            pass
        return rates

    async def _fetch_ccxt_funding(self, exchange_id: str) -> dict[str, float]:
        """Fetch funding rates from a CEX via CCXT (async)."""
        try:
            import ccxt.async_support as ccxt_async
        except ImportError:
            self.log.debug("ccxt not installed — skipping %s", exchange_id)
            return {}

        rates: dict[str, float] = {}
        exchange = None
        try:
            exchange_class = getattr(ccxt_async, exchange_id, None)
            if exchange_class is None:
                return rates

            exchange = exchange_class({"enableRateLimit": True})
            funding_rates = await exchange.fetch_funding_rates()

            for symbol, info in funding_rates.items():
                # Map "BTC/USDT:USDT" → "BTC"
                base = symbol.split("/")[0] if "/" in symbol else symbol
                rate = info.get("fundingRate")
                if rate is not None:
                    rates[base] = float(rate)
        except Exception:
            self.log.warning(
                "Failed to fetch funding from %s", exchange_id, exc_info=True,
            )
        finally:
            if exchange:
                await exchange.close()
        return rates

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "hl_assets_tracked": len(self._hl_funding),
            "binance_assets": len(self._binance_funding),
            "bybit_assets": len(self._bybit_funding),
            "signals_emitted": self._signals_emitted,
            "histories_tracked": len(self._history),
        })
        return base
