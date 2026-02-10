"""
HedgingEngine — automatic portfolio hedging and tail-risk protection.

Monitors ``portfolio:state`` for directional imbalances.  When net
exposure exceeds the configured threshold, the engine identifies
negatively-correlated assets available on Hyperliquid and publishes
hedge suggestions to ``decisions:pending`` (they still pass through
RiskGuardian before execution).

Tail-risk mode:  When a VIX-equivalent proxy spikes (cross-asset
volatility surge), the engine pre-emptively hedges a configurable
fraction of the portfolio.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import numpy as np

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_DECISIONS_PENDING,
    STREAM_MARKET_PRICES,
    STREAM_PORTFOLIO_STATE,
    STREAM_RISK_ANOMALY,
)
from core.models import (
    AlertSeverity,
    AnomalyAlert,
    AnomalyType,
    Direction,
    HedgeSuggestion,
    PortfolioState,
    PriceUpdate,
    TradeAction,
    TradeDecision,
)

# ---------------------------------------------------------------------------
# Candidate hedge assets on Hyperliquid by asset class.
#
# When the portfolio is long crypto, we want to hedge with assets that
# historically move inversely (equities as risk-on proxy can be used for
# macro hedges, gold as safe haven, etc.).
# ---------------------------------------------------------------------------

_HEDGE_CANDIDATES: dict[str, list[str]] = {
    # If long crypto → hedge with gold / short equity index.
    "long_crypto": ["GOLD", "SPY"],
    # If short crypto → hedge with BTC or ETH (revert-to-mean).
    "short_crypto": ["BTC", "ETH"],
    # If long equities → hedge with gold.
    "long_equity": ["GOLD"],
    # Generic diversifiers available on HL perps.
    "diversifiers": ["GOLD", "SILVER", "SPY", "EUR"],
}

# Number of recent hourly samples used for cross-asset correlation.
_CORR_SAMPLES = 168  # 7 days hourly.

# VIX proxy: when rolling 24h volatility of BTC + ETH exceeds this
# threshold (annualised), enter tail-risk hedging mode.
_TAIL_RISK_VOL_THRESHOLD = 1.20  # 120 % annualised vol.


class HedgingEngine(BaseAgent):
    """Monitors portfolio tilt and proposes hedging trades."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="hedging_engine",
            agent_type="risk",
            bus=bus,
            **kw,
        )
        self._portfolio = PortfolioState()

        # Hourly price samples for correlation.
        self._hourly_prices: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=_CORR_SAMPLES),
        )
        self._last_hourly_sample: float = 0.0

        # Latest mid-prices.
        self._mid_prices: dict[str, float] = {}

        # Track whether tail-risk mode is active.
        self._tail_risk_active = False

        # Cooldown: don't spam hedge suggestions.
        self._last_hedge_time: float = 0.0
        self._hedge_cooldown = 300.0  # 5 minutes.

        self._suggestions_count = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._price_listener(), name="he:prices"),
            asyncio.create_task(self._hedge_evaluation_loop(), name="he:eval"),
            asyncio.create_task(self._tail_risk_loop(), name="he:tail"),
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

    # -- main loop: consume portfolio state ----------------------------------

    async def process(self) -> None:
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE,
            group="hedging_engine",
            consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    # -- periodic hedge evaluation -------------------------------------------

    async def _hedge_evaluation_loop(self) -> None:
        """Every 30 seconds, check if a hedge is needed."""
        while True:
            await asyncio.sleep(30)
            try:
                await self._evaluate_hedge()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Hedge evaluation failed.")

    async def _evaluate_hedge(self) -> None:
        import time
        now = time.monotonic()
        if now - self._last_hedge_time < self._hedge_cooldown:
            return

        nav = self._portfolio.total_nav
        net_exp = self._portfolio.net_exposure
        if nav <= 0:
            return

        threshold = settings.hedge_net_exposure_threshold

        # No hedge needed if exposure is within bounds.
        if abs(net_exp) <= threshold:
            return

        # Determine the hedge direction.
        is_long_heavy = net_exp > threshold
        excess = abs(net_exp) - threshold

        self.log.info(
            "Net exposure %.2f exceeds ±%.2f threshold — seeking hedge (excess=%.2f).",
            net_exp, threshold, excess,
        )

        # Pick the best hedge asset (most negatively correlated).
        hedge_asset, corr = self._select_hedge_asset(is_long_heavy)
        if hedge_asset is None:
            self.log.warning("No suitable hedge asset found.")
            return

        # Calculate hedge size.
        # We want to reduce net exposure back to the threshold.
        hedge_size_pct = min(excess, settings.max_position_pct)
        # Hedge action: if portfolio is long-heavy, we sell (short) the hedge asset.
        # If short-heavy, we buy (long).
        if is_long_heavy:
            hedge_action = TradeAction.OPEN_SHORT
        else:
            hedge_action = TradeAction.OPEN_LONG

        suggestion = HedgeSuggestion(
            reason="Net exposure %.2f exceeds threshold ±%.2f" % (net_exp, threshold),
            hedge_asset=hedge_asset,
            hedge_action=hedge_action,
            hedge_size_pct=hedge_size_pct,
            current_net_exposure=net_exp,
            target_net_exposure=threshold if is_long_heavy else -threshold,
            correlation_to_portfolio=corr,
        )

        self.log.info(
            "HEDGE SUGGESTION: %s %s size=%.4f (corr=%.2f to portfolio)",
            hedge_action, hedge_asset, hedge_size_pct, corr,
        )

        # Publish as a TradeDecision to decisions:pending (full risk pipeline).
        decision = TradeDecision(
            asset=hedge_asset,
            action=hedge_action,
            size_pct=hedge_size_pct,
            consensus_score=0.0,
            entry_price=self._mid_prices.get(hedge_asset),
            metadata={
                "source": "hedging_engine",
                "suggestion_id": suggestion.suggestion_id,
                "reason": suggestion.reason,
                "correlation": corr,
                "execution_mode": "twap",
                "twap_splits": 3,
                "twap_duration": 180,
            },
        )
        await self.bus.publish_to(STREAM_DECISIONS_PENDING, decision)
        self._last_hedge_time = now
        self._suggestions_count += 1

    # -- hedge asset selection -----------------------------------------------

    def _select_hedge_asset(self, is_long_heavy: bool) -> tuple[str | None, float]:
        """Find the most negatively correlated tradeable asset.

        Returns (asset, correlation) or (None, 0) if nothing suitable.
        """
        # Build return series for the portfolio composite.
        portfolio_assets = [p.asset for p in self._portfolio.positions]
        if not portfolio_assets:
            return None, 0.0

        # Weighted composite returns.
        composite_returns = self._composite_returns(portfolio_assets)
        if composite_returns is None or len(composite_returns) < 10:
            # Fallback to pre-defined candidates.
            return self._fallback_hedge(is_long_heavy), 0.0

        # Candidate assets = everything we have data for MINUS existing positions.
        existing = set(portfolio_assets)
        candidates = [
            a for a in self._hourly_prices
            if a not in existing and len(self._hourly_prices[a]) >= 24
        ]

        # Also include pre-configured diversifiers.
        for pool in _HEDGE_CANDIDATES.values():
            for c in pool:
                if c not in existing and c not in candidates and c in self._mid_prices:
                    candidates.append(c)

        best_asset: str | None = None
        best_corr: float = 0.0  # We want the most negative.

        for candidate in candidates:
            prices = list(self._hourly_prices[candidate])
            n = min(len(prices), len(composite_returns) + 1)
            if n < 10:
                continue
            returns = [
                (prices[i] - prices[i - 1]) / prices[i - 1]
                for i in range(1, n)
                if prices[i - 1] != 0
            ]
            overlap = min(len(returns), len(composite_returns))
            if overlap < 10:
                continue
            corr = float(np.corrcoef(
                composite_returns[-overlap:],
                returns[-overlap:],
            )[0, 1])

            # We want the most negative correlation for hedging.
            if corr < best_corr:
                best_corr = corr
                best_asset = candidate

        if best_asset is None:
            return self._fallback_hedge(is_long_heavy), 0.0

        return best_asset, best_corr

    def _composite_returns(self, assets: list[str]) -> list[float] | None:
        """Compute equal-weighted composite return series for given assets."""
        series_list: list[list[float]] = []
        min_len = float("inf")

        for asset in assets:
            prices = list(self._hourly_prices.get(asset, []))
            if len(prices) < 10:
                continue
            returns = [
                (prices[i] - prices[i - 1]) / prices[i - 1]
                for i in range(1, len(prices))
                if prices[i - 1] != 0
            ]
            series_list.append(returns)
            min_len = min(min_len, len(returns))

        if not series_list or min_len < 10:
            return None

        n = int(min_len)
        composite = [0.0] * n
        for s in series_list:
            for i in range(n):
                composite[i] += s[-(n - i)] / len(series_list)
        return composite

    def _fallback_hedge(self, is_long_heavy: bool) -> str | None:
        """Use pre-configured candidates when correlation data is sparse."""
        pool_key = "long_crypto" if is_long_heavy else "short_crypto"
        for asset in _HEDGE_CANDIDATES.get(pool_key, []):
            if asset in self._mid_prices:
                return asset
        for asset in _HEDGE_CANDIDATES.get("diversifiers", []):
            if asset in self._mid_prices:
                return asset
        return None

    # -- tail-risk monitoring ------------------------------------------------

    async def _tail_risk_loop(self) -> None:
        """Every 60 seconds, compute a VIX-like proxy and hedge if spiking."""
        while True:
            await asyncio.sleep(60)
            try:
                await self._check_tail_risk()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Tail risk check failed.")

    async def _check_tail_risk(self) -> None:
        """VIX proxy: annualised rolling vol of BTC+ETH returns."""
        vol = self._compute_vol_proxy()
        if vol is None:
            return

        if vol > _TAIL_RISK_VOL_THRESHOLD and not self._tail_risk_active:
            self._tail_risk_active = True
            self.log.critical(
                "TAIL RISK MODE: vol proxy %.1f%% exceeds %.1f%% threshold.",
                vol * 100, _TAIL_RISK_VOL_THRESHOLD * 100,
            )
            await self._execute_tail_hedge()

        elif vol <= _TAIL_RISK_VOL_THRESHOLD * 0.8 and self._tail_risk_active:
            self._tail_risk_active = False
            self.log.info(
                "Tail risk mode deactivated: vol proxy %.1f%% back within bounds.",
                vol * 100,
            )

    def _compute_vol_proxy(self) -> float | None:
        """Annualised 24h realised vol from BTC + ETH hourly returns."""
        assets = ["BTC", "ETH"]
        all_returns: list[float] = []

        for asset in assets:
            prices = list(self._hourly_prices.get(asset, []))
            if len(prices) < 25:
                return None
            # Last 24 hourly returns.
            recent = prices[-25:]
            returns = [
                (recent[i] - recent[i - 1]) / recent[i - 1]
                for i in range(1, len(recent))
                if recent[i - 1] != 0
            ]
            all_returns.extend(returns)

        if len(all_returns) < 20:
            return None

        hourly_std = float(np.std(all_returns))
        # Annualise: sqrt(24 * 365) ≈ 93.6.
        annualised = hourly_std * (24 * 365) ** 0.5
        return annualised

    async def _execute_tail_hedge(self) -> None:
        """In tail-risk mode, hedge a fixed percentage of portfolio with safe havens."""
        nav = self._portfolio.total_nav
        if nav <= 0:
            return

        hedge_pct = settings.hedge_tail_risk_pct

        # Try GOLD first, then SPY short, then any available diversifier.
        hedge_asset = None
        for candidate in ["GOLD", "SILVER", "SPY"]:
            if candidate in self._mid_prices:
                hedge_asset = candidate
                break

        if hedge_asset is None:
            self.log.warning("No tail-risk hedge asset available on exchange.")
            return

        # For GOLD/SILVER, go long (safe haven). For SPY, go short (risk-off).
        if hedge_asset in ("GOLD", "SILVER"):
            action = TradeAction.OPEN_LONG
        else:
            action = TradeAction.OPEN_SHORT

        decision = TradeDecision(
            asset=hedge_asset,
            action=action,
            size_pct=min(hedge_pct, settings.max_position_pct),
            consensus_score=0.0,
            entry_price=self._mid_prices.get(hedge_asset),
            metadata={
                "source": "hedging_engine",
                "reason": "tail_risk_hedge",
                "vol_proxy": self._compute_vol_proxy(),
                "execution_mode": "twap",
                "twap_splits": 5,
                "twap_duration": 300,
            },
        )
        await self.bus.publish_to(STREAM_DECISIONS_PENDING, decision)
        self._suggestions_count += 1

        self.log.critical(
            "TAIL RISK HEDGE: %s %s size=%.4f",
            action, hedge_asset, decision.size_pct,
        )

    # -- price listener ------------------------------------------------------

    async def _price_listener(self) -> None:
        """Sample hourly prices and track mid-prices."""
        import time as _time
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="hedging_engine_prices",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                self._mid_prices.update(update.prices)

                # Sample hourly.
                now = _time.monotonic()
                if now - self._last_hourly_sample >= 3600 or self._last_hourly_sample == 0:
                    self._last_hourly_sample = now
                    for asset, price in update.prices.items():
                        self._hourly_prices[asset].append(price)
            except Exception:
                pass

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "portfolio_net_exposure": self._portfolio.net_exposure,
            "tail_risk_active": self._tail_risk_active,
            "suggestions_sent": self._suggestions_count,
            "assets_with_history": len(self._hourly_prices),
        })
        return base
