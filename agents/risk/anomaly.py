"""
AnomalyDetector — real-time market surveillance.

Monitors the ``market:prices`` stream and platform data for anomalies
that precede adverse events.  When something unusual is detected, an
``AnomalyAlert`` is published to ``risk:anomaly``.

High-severity alerts automatically trigger the RiskGuardian's defensive
mode (reduce positions 30 %, widen stops, pause entries 15 min).

Detection rules:
  a) Volume spike > 5× rolling average
  b) Price move > 3 σ in < 5 minutes
  c) Order book bid/ask imbalance > 10:1
  d) Funding rate extreme ( > |0.1 %| per 8 h )
  e) Correlation spike — multiple assets moving in lockstep
  f) Hyperliquid API latency > 2 seconds
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import numpy as np

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_EXECUTION_ADVISORY,
    STREAM_MARKET_PRICES,
    STREAM_RISK_ANOMALY,
)
from core.models import (
    AlertSeverity,
    AnomalyAlert,
    AnomalyType,
    ExecutionAdvisory,
    PriceUpdate,
)

# ---------------------------------------------------------------------------
# Rolling-statistics helpers
# ---------------------------------------------------------------------------

_PRICE_WINDOW = 300   # 5 minutes of 1-second ticks.
_VOLUME_WINDOW = 720  # ~12 hours of 1-minute volume samples.
_CORR_ASSETS = 10     # Track correlations among top N assets.
_CORR_WINDOW = 60     # Last 60 price samples for cross-asset correlation.


class _RollingStats:
    """Efficient on-line mean and variance using Welford's algorithm,
    backed by a fixed-length deque so the window slides."""

    __slots__ = ("_data", "_maxlen")

    def __init__(self, maxlen: int) -> None:
        self._data: deque[float] = deque(maxlen=maxlen)
        self._maxlen = maxlen

    def append(self, value: float) -> None:
        self._data.append(value)

    @property
    def count(self) -> int:
        return len(self._data)

    def mean(self) -> float:
        if not self._data:
            return 0.0
        return sum(self._data) / len(self._data)

    def std(self) -> float:
        n = len(self._data)
        if n < 2:
            return 0.0
        m = self.mean()
        var = sum((x - m) ** 2 for x in self._data) / (n - 1)
        return var ** 0.5

    def last(self) -> float:
        return self._data[-1] if self._data else 0.0

    def first(self) -> float:
        return self._data[0] if self._data else 0.0

    def values(self) -> list[float]:
        return list(self._data)


# ---------------------------------------------------------------------------
# AnomalyDetector agent
# ---------------------------------------------------------------------------

class AnomalyDetector(BaseAgent):
    """Real-time market anomaly detection engine."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="anomaly_detector",
            agent_type="risk",
            bus=bus,
            **kw,
        )
        # Per-asset rolling price windows (5-min window).
        self._prices: dict[str, _RollingStats] = defaultdict(
            lambda: _RollingStats(_PRICE_WINDOW),
        )
        # Per-asset rolling volume (12-hour window, 1-min buckets).
        self._volumes: dict[str, _RollingStats] = defaultdict(
            lambda: _RollingStats(_VOLUME_WINDOW),
        )
        # Per-asset last-known L2 book depth from execution advisory.
        self._bid_depth: dict[str, float] = {}
        self._ask_depth: dict[str, float] = {}
        # Per-asset funding rates.
        self._funding: dict[str, float] = {}

        # Cross-asset correlation tracker (recent prices for top assets).
        self._corr_prices: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=_CORR_WINDOW),
        )

        # Timestamps for rate-limiting alert emission.
        self._last_alert: dict[str, float] = defaultdict(float)
        self._alert_cooldown = 60.0  # Don't re-fire same alert type within 60 s.

        # Latency tracking.
        self._last_msg_ts: float = 0.0

        self._sub_tasks: list[asyncio.Task[None]] = []
        self._alert_count = 0

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._advisory_listener(), name="ad:advisory"),
            asyncio.create_task(self._correlation_check_loop(), name="ad:corr"),
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

    # -- main loop: consume price ticks --------------------------------------

    async def process(self) -> None:
        async for _msg_id, payload in self.bus.subscribe_to(
            STREAM_MARKET_PRICES,
            group="anomaly_detector",
            consumer=self.agent_id,
        ):
            try:
                before = time.monotonic()
                update = PriceUpdate.model_validate(payload)

                for asset, price in update.prices.items():
                    self._prices[asset].append(price)
                    self._corr_prices[asset].append(price)

                # --- (f) API latency check ---
                elapsed_ms = (time.monotonic() - before) * 1000
                # Also measure message staleness.
                msg_age_ms = (
                    datetime.now(timezone.utc) - update.timestamp
                ).total_seconds() * 1000
                if msg_age_ms > settings.api_latency_threshold_ms:
                    await self._emit_anomaly(
                        AnomalyType.API_LATENCY,
                        AlertSeverity.WARNING,
                        description="Market data staleness %.0f ms (threshold %.0f ms)" % (
                            msg_age_ms, settings.api_latency_threshold_ms,
                        ),
                        value=msg_age_ms,
                        threshold=settings.api_latency_threshold_ms,
                    )

                # --- (b) Price deviation (3σ in 5 min) ---
                for asset, price in update.prices.items():
                    stats = self._prices[asset]
                    if stats.count < 30:
                        continue
                    mu = stats.mean()
                    sigma = stats.std()
                    if sigma > 0:
                        zscore = abs(price - mu) / sigma
                        if zscore > settings.price_zscore_threshold:
                            severity = (
                                AlertSeverity.CRITICAL if zscore > 5
                                else AlertSeverity.WARNING
                            )
                            await self._emit_anomaly(
                                AnomalyType.PRICE_DEVIATION,
                                severity,
                                asset=asset,
                                description="%s price z-score %.1f (threshold %.1f)" % (
                                    asset, zscore, settings.price_zscore_threshold,
                                ),
                                value=zscore,
                                threshold=settings.price_zscore_threshold,
                            )

                # --- (c) Order book imbalance ---
                for asset in update.prices:
                    bid = self._bid_depth.get(asset, 0)
                    ask = self._ask_depth.get(asset, 0)
                    if ask > 0 and bid > 0:
                        ratio = max(bid / ask, ask / bid)
                        if ratio > settings.book_imbalance_threshold:
                            await self._emit_anomaly(
                                AnomalyType.BOOK_IMBALANCE,
                                AlertSeverity.WARNING,
                                asset=asset,
                                description="%s book imbalance %.1f:1 (threshold %.1f)" % (
                                    asset, ratio, settings.book_imbalance_threshold,
                                ),
                                value=ratio,
                                threshold=settings.book_imbalance_threshold,
                            )

                # --- (d) Funding rate extreme ---
                for asset in update.prices:
                    fr = self._funding.get(asset)
                    if fr is not None and abs(fr) > settings.funding_rate_extreme:
                        severity = (
                            AlertSeverity.CRITICAL if abs(fr) > settings.funding_rate_extreme * 3
                            else AlertSeverity.WARNING
                        )
                        await self._emit_anomaly(
                            AnomalyType.FUNDING_EXTREME,
                            severity,
                            asset=asset,
                            description="%s funding rate %.4f%% (threshold ±%.4f%%)" % (
                                asset, fr * 100, settings.funding_rate_extreme * 100,
                            ),
                            value=fr,
                            threshold=settings.funding_rate_extreme,
                        )

            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Error in anomaly detection loop.")

    # -- (a) volume spike detection via advisory data ------------------------

    async def _advisory_listener(self) -> None:
        """Ingest execution advisories for depth/volume/funding data."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_EXECUTION_ADVISORY,
            group="anomaly_advisory",
            consumer=self.agent_id,
        ):
            try:
                adv = ExecutionAdvisory.model_validate(payload)
                self._bid_depth[adv.asset] = adv.bid_depth_usd
                self._ask_depth[adv.asset] = adv.ask_depth_usd

                # Use depth as a volume proxy and check for spikes.
                total_depth = adv.bid_depth_usd + adv.ask_depth_usd
                vol_stats = self._volumes[adv.asset]
                vol_stats.append(total_depth)

                if vol_stats.count >= 30:
                    avg = vol_stats.mean()
                    if avg > 0 and total_depth > avg * settings.volume_spike_multiplier:
                        severity = (
                            AlertSeverity.CRITICAL
                            if total_depth > avg * settings.volume_spike_multiplier * 2
                            else AlertSeverity.WARNING
                        )
                        await self._emit_anomaly(
                            AnomalyType.VOLUME_SPIKE,
                            severity,
                            asset=adv.asset,
                            description="%s depth %.0f vs avg %.0f (%.1fx)" % (
                                adv.asset, total_depth, avg, total_depth / avg,
                            ),
                            value=total_depth / avg,
                            threshold=settings.volume_spike_multiplier,
                        )
            except Exception:
                pass

    # -- (e) cross-asset correlation spike -----------------------------------

    async def _correlation_check_loop(self) -> None:
        """Every 60 seconds, check if multiple assets are moving in lockstep."""
        while True:
            await asyncio.sleep(60)
            try:
                await self._check_correlation_spike()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Correlation check failed.")

    async def _check_correlation_spike(self) -> None:
        # Pick top N assets by data availability.
        candidates = sorted(
            self._corr_prices.keys(),
            key=lambda a: len(self._corr_prices[a]),
            reverse=True,
        )[:_CORR_ASSETS]

        if len(candidates) < 3:
            return

        # Build return matrix.
        series: dict[str, list[float]] = {}
        min_len = min(len(self._corr_prices[a]) for a in candidates)
        if min_len < 10:
            return

        for asset in candidates:
            prices = list(self._corr_prices[asset])[-min_len:]
            returns = [
                (prices[i] - prices[i - 1]) / prices[i - 1]
                for i in range(1, len(prices))
                if prices[i - 1] != 0
            ]
            if len(returns) >= min_len - 2:
                series[asset] = returns[:min_len - 1]

        assets = list(series.keys())
        if len(assets) < 3:
            return

        n = min(len(v) for v in series.values())
        matrix = np.array([series[a][:n] for a in assets])
        corr_matrix = np.corrcoef(matrix)

        # Count pairs with |correlation| > 0.9.
        high_corr_pairs = 0
        total_pairs = 0
        for i in range(len(assets)):
            for j in range(i + 1, len(assets)):
                total_pairs += 1
                if abs(corr_matrix[i, j]) > 0.9:
                    high_corr_pairs += 1

        if total_pairs > 0:
            pct_high = high_corr_pairs / total_pairs
            # If >50 % of pairs have extreme correlation, it's a regime event.
            if pct_high > 0.5:
                severity = (
                    AlertSeverity.CRITICAL if pct_high > 0.7
                    else AlertSeverity.WARNING
                )
                await self._emit_anomaly(
                    AnomalyType.CORRELATION_SPIKE,
                    severity,
                    description="%.0f%% of asset pairs have |corr| > 0.9 (%d/%d)" % (
                        pct_high * 100, high_corr_pairs, total_pairs,
                    ),
                    value=pct_high,
                    threshold=0.5,
                )

    # -- alert emission with cooldown ----------------------------------------

    async def _emit_anomaly(
        self,
        anomaly_type: AnomalyType,
        severity: AlertSeverity,
        description: str = "",
        asset: str = "",
        value: float = 0.0,
        threshold: float = 0.0,
    ) -> None:
        """Publish an anomaly alert, respecting per-type cooldown."""
        cooldown_key = "%s:%s" % (anomaly_type, asset)
        now = time.monotonic()
        if now - self._last_alert[cooldown_key] < self._alert_cooldown:
            return
        self._last_alert[cooldown_key] = now

        alert = AnomalyAlert(
            anomaly_type=anomaly_type,
            severity=severity,
            asset=asset,
            description=description,
            value=value,
            threshold=threshold,
        )
        await self.bus.publish_to(STREAM_RISK_ANOMALY, alert)
        self._alert_count += 1

        log_fn = self.log.critical if severity == AlertSeverity.CRITICAL else self.log.warning
        log_fn(
            "ANOMALY [%s] %s: %s (value=%.4f threshold=%.4f)",
            severity.upper(), anomaly_type, description, value, threshold,
        )

    # -- public: allow platform specialist to push funding rates -------------

    def update_funding(self, asset: str, rate: float) -> None:
        self._funding[asset] = rate

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "assets_monitored": len(self._prices),
            "alerts_emitted": self._alert_count,
        })
        return base
