"""
RegimeClassifier — HMM-based market regime detection.

Uses a Gaussian Hidden Markov Model (hmmlearn) trained on rolling market
features (returns, volatility, volume, correlations) to classify the
current market into one of 5 regimes:

  1. trending_up      — sustained bullish momentum
  2. trending_down    — sustained bearish pressure
  3. mean_reverting   — range-bound, oscillating
  4. high_volatility  — large swings, unclear direction
  5. low_volatility   — calm, low-range environment

Publishes ``RegimeState`` to ``data:regime`` every ``regime_classifier_interval``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any

import numpy as np

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_DATA_REGIME,
    STREAM_MARKET_PRICES,
)
from core.models import MarketRegime, PriceUpdate, RegimeState


# Map HMM state index → regime (ordered by typical volatility).
_STATE_MAP: dict[int, MarketRegime] = {
    0: MarketRegime.LOW_VOLATILITY,
    1: MarketRegime.MEAN_REVERTING,
    2: MarketRegime.TRENDING_UP,
    3: MarketRegime.TRENDING_DOWN,
    4: MarketRegime.HIGH_VOLATILITY,
}

# Minimum observations before fitting HMM.
_MIN_OBS = 50


class RegimeClassifier(BaseAgent):
    """Classifies market regime using a Gaussian HMM."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="regime_classifier",
            agent_type="decision",
            bus=bus,
            **kw,
        )
        self._btc_prices: deque[float] = deque(maxlen=500)
        self._eth_prices: deque[float] = deque(maxlen=500)
        self._current_regime = MarketRegime.LOW_VOLATILITY
        self._regime_confidence = 0.0
        self._regime_start: datetime = datetime.now(timezone.utc)
        self._hmm: Any = None  # GaussianHMM instance, lazy-init.
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._price_listener(), name="rc:px"),
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
        """Periodically classify the regime and publish."""
        await asyncio.sleep(settings.regime_classifier_interval)
        if len(self._btc_prices) < _MIN_OBS:
            return

        features = self._build_features()
        if features is None:
            return

        regime, confidence = await asyncio.get_running_loop().run_in_executor(
            None, self._fit_and_predict, features,
        )

        previous = self._current_regime
        if regime != self._current_regime:
            self._current_regime = regime
            self._regime_start = datetime.now(timezone.utc)
            self.log.info(
                "REGIME CHANGE: %s → %s (confidence=%.2f)",
                previous, regime, confidence,
            )

        now = datetime.now(timezone.utc)
        duration_h = (now - self._regime_start).total_seconds() / 3600

        state = RegimeState(
            regime=self._current_regime,
            confidence=confidence,
            previous_regime=previous if previous != self._current_regime else None,
            regime_duration_hours=round(duration_h, 2),
            features={
                "btc_return_1h": self._pct_return(self._btc_prices, 12),
                "btc_vol_1h": self._rolling_vol(self._btc_prices, 12),
                "obs_count": len(self._btc_prices),
            },
        )
        self._regime_confidence = confidence
        await self.bus.publish_to(STREAM_DATA_REGIME, state)

    def _build_features(self) -> np.ndarray | None:
        """Build feature matrix from price history: [returns, vol, spread]."""
        btc = np.array(self._btc_prices)
        if len(btc) < _MIN_OBS:
            return None

        returns = np.diff(np.log(btc))
        if len(returns) < _MIN_OBS - 1:
            return None

        # Rolling 12-period (≈1h at 5min ticks) volatility.
        vol = np.array([
            np.std(returns[max(0, i - 12):i + 1])
            for i in range(len(returns))
        ])

        # BTC-ETH spread if available.
        if len(self._eth_prices) >= len(btc):
            eth = np.array(list(self._eth_prices)[-len(btc):])
            eth_returns = np.diff(np.log(eth))
            spread = returns - eth_returns[:len(returns)]
        else:
            spread = np.zeros_like(returns)

        features = np.column_stack([returns, vol, spread])
        return features

    def _fit_and_predict(self, features: np.ndarray) -> tuple[MarketRegime, float]:
        """Fit HMM and predict current state (sync — runs in executor)."""
        try:
            from hmmlearn.hmm import GaussianHMM
        except ImportError:
            return MarketRegime.LOW_VOLATILITY, 0.0

        n_states = min(settings.regime_hmm_n_states, len(_STATE_MAP))

        model = GaussianHMM(
            n_components=n_states,
            covariance_type="full",
            n_iter=100,
            random_state=42,
        )

        try:
            model.fit(features)
            states = model.predict(features)
            proba = model.predict_proba(features)

            current_state = int(states[-1])
            confidence = float(proba[-1, current_state])
            regime = _STATE_MAP.get(current_state, MarketRegime.LOW_VOLATILITY)

            # Heuristic: remap based on mean return per state.
            state_returns: dict[int, float] = {}
            for s in range(n_states):
                mask = states == s
                if mask.any():
                    state_returns[s] = float(np.mean(features[mask, 0]))

            # Sort states by mean return to assign regimes more accurately.
            sorted_states = sorted(state_returns.items(), key=lambda x: x[1])
            remapped: dict[int, MarketRegime] = {}
            regimes_ordered = [
                MarketRegime.TRENDING_DOWN,
                MarketRegime.HIGH_VOLATILITY,
                MarketRegime.MEAN_REVERTING,
                MarketRegime.LOW_VOLATILITY,
                MarketRegime.TRENDING_UP,
            ]
            for i, (state_idx, _) in enumerate(sorted_states):
                remapped[state_idx] = regimes_ordered[min(i, len(regimes_ordered) - 1)]

            regime = remapped.get(current_state, MarketRegime.LOW_VOLATILITY)
            self._hmm = model
            return regime, confidence

        except Exception:
            return self._current_regime, 0.0

    @staticmethod
    def _pct_return(prices: deque[float], lookback: int) -> float:
        if len(prices) <= lookback:
            return 0.0
        return (prices[-1] / prices[-lookback - 1]) - 1.0

    @staticmethod
    def _rolling_vol(prices: deque[float], lookback: int) -> float:
        if len(prices) <= lookback:
            return 0.0
        arr = np.array(list(prices)[-lookback - 1:])
        returns = np.diff(np.log(arr))
        return float(np.std(returns)) if len(returns) > 0 else 0.0

    async def _price_listener(self) -> None:
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="regime_classifier",
            consumer=self.agent_id,
        ):
            try:
                update = PriceUpdate.model_validate(payload)
                if "BTC" in update.prices:
                    self._btc_prices.append(update.prices["BTC"])
                if "ETH" in update.prices:
                    self._eth_prices.append(update.prices["ETH"])
            except Exception:
                pass

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "current_regime": self._current_regime,
            "regime_confidence": round(self._regime_confidence, 3),
            "btc_observations": len(self._btc_prices),
        })
        return base
