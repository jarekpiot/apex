"""
MacroRegime — macro-economic regime detection for directional bias.

Data sources:
  - FRED API: DXY (dollar index), 10Y/2Y Treasury yields, VIX
  - Derived: yield-curve slope, dollar momentum, risk-on/off classification

Regime logic:
  - Risk-on:  falling DXY + steepening yield curve + low VIX → bullish crypto
  - Risk-off: rising DXY + inverting yield curve + high VIX → bearish crypto
  - Neutral:  mixed signals → low conviction

Applies a broad directional bias to all assets (crypto benefits in risk-on,
suffers in risk-off). Equity and commodity perps get regime-specific biases.

Runs every 30 minutes (FRED data updates slowly).
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus
from core.models import AgentSignal, Timeframe

# FRED series IDs.
_FRED_SERIES = {
    "dxy": "DTWEXBGS",      # Trade-weighted dollar index (broad).
    "us10y": "DGS10",       # 10-Year Treasury yield.
    "us2y": "DGS2",         # 2-Year Treasury yield.
    "vix": "VIXCLS",        # VIX close.
}

_FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Assets that get macro signals.
_CRYPTO_MAJORS = ["BTC", "ETH"]
_RISK_ON_ASSETS = ["SOL", "AVAX", "ARB", "OP", "LINK", "DOGE"]
_EQUITY_ASSETS = ["SPY"]
_SAFE_HAVEN = ["GOLD"]


class MacroRegime(BaseAgent):
    """Macro-economic regime detection → directional bias."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="macro_regime",
            agent_type="macro",
            bus=bus,
            **kw,
        )
        # Rolling history for trend detection.
        self._dxy_history: deque[float] = deque(maxlen=30)
        self._us10y_history: deque[float] = deque(maxlen=30)
        self._us2y_history: deque[float] = deque(maxlen=30)
        self._vix_history: deque[float] = deque(maxlen=30)

        self._current_regime: str = "neutral"  # risk_on, risk_off, neutral
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="mr:scan"),
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
        await asyncio.sleep(3600)

    # -- scan ----------------------------------------------------------------

    async def _scan_loop(self) -> None:
        await asyncio.sleep(10)
        while True:
            try:
                await self._scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Macro scan failed.")
            await asyncio.sleep(settings.macro_scan_interval)

    async def _scan(self) -> None:
        fred_key = settings.fred_api_key.get_secret_value()
        if not fred_key:
            self.log.debug("No FRED API key — macro agent idle.")
            return

        # Fetch all series in parallel.
        results = await asyncio.gather(
            *[self._fetch_fred(series_id, fred_key) for series_id in _FRED_SERIES.values()],
            return_exceptions=True,
        )

        values: dict[str, float] = {}
        for key, result in zip(_FRED_SERIES.keys(), results):
            if isinstance(result, (int, float)):
                values[key] = float(result)

        if not values:
            return

        # Update histories.
        if "dxy" in values:
            self._dxy_history.append(values["dxy"])
        if "us10y" in values:
            self._us10y_history.append(values["us10y"])
        if "us2y" in values:
            self._us2y_history.append(values["us2y"])
        if "vix" in values:
            self._vix_history.append(values["vix"])

        # Classify regime.
        regime, regime_score, reasoning = self._classify_regime(values)
        self._current_regime = regime

        self.log.info("Macro regime: %s (score=%.2f) — %s", regime, regime_score, reasoning)

        if abs(regime_score) < 0.1:
            return

        # Emit signals for crypto assets.
        for asset in _CRYPTO_MAJORS + _RISK_ON_ASSETS:
            conviction = min(abs(regime_score) * 0.8, 0.6)
            if asset in _RISK_ON_ASSETS:
                conviction *= 0.8  # Lower conviction for alts.
            sig = AgentSignal(
                agent_id=self.agent_id, asset=asset,
                direction=regime_score,
                conviction=conviction,
                timeframe=Timeframe.SWING,
                reasoning="Macro %s: %s" % (regime, reasoning),
                data_sources=["fred"],
                metadata={"regime": regime, **{k: round(v, 4) for k, v in values.items()}},
                expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
            )
            await self.emit_signal(sig)
            self._signals_emitted += 1

        # Equities: risk-on = bullish SPY.
        for asset in _EQUITY_ASSETS:
            sig = AgentSignal(
                agent_id=self.agent_id, asset=asset,
                direction=regime_score * 0.8,
                conviction=min(abs(regime_score) * 0.7, 0.5),
                timeframe=Timeframe.SWING,
                reasoning="Macro %s: %s" % (regime, reasoning),
                data_sources=["fred"],
                expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
            )
            await self.emit_signal(sig)
            self._signals_emitted += 1

        # Gold: inverse of risk-on (safe haven).
        for asset in _SAFE_HAVEN:
            sig = AgentSignal(
                agent_id=self.agent_id, asset=asset,
                direction=-regime_score * 0.6,
                conviction=min(abs(regime_score) * 0.5, 0.4),
                timeframe=Timeframe.SWING,
                reasoning="Macro %s (safe haven inverse): %s" % (regime, reasoning),
                data_sources=["fred"],
                expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
            )
            await self.emit_signal(sig)
            self._signals_emitted += 1

    # -- regime classification -----------------------------------------------

    def _classify_regime(
        self, values: dict[str, float],
    ) -> tuple[str, float, str]:
        """Returns (regime_name, score [-1..+1], reasoning)."""
        scores: list[float] = []
        reasons: list[str] = []

        # DXY trend: falling = risk-on, rising = risk-off.
        if len(self._dxy_history) >= 3:
            recent = list(self._dxy_history)
            dxy_change = (recent[-1] - recent[-3]) / recent[-3] if recent[-3] else 0
            if dxy_change < -0.005:
                scores.append(0.4)
                reasons.append("DXY falling (%.2f%%)" % (dxy_change * 100))
            elif dxy_change > 0.005:
                scores.append(-0.4)
                reasons.append("DXY rising (%.2f%%)" % (dxy_change * 100))

        # Yield curve: 10Y - 2Y. Positive = normal, negative = inverted.
        us10y = values.get("us10y")
        us2y = values.get("us2y")
        if us10y is not None and us2y is not None:
            spread = us10y - us2y
            if spread > 0.5:
                scores.append(0.3)
                reasons.append("Yield curve normal (%.2f%%)" % spread)
            elif spread < -0.2:
                scores.append(-0.4)
                reasons.append("Yield curve inverted (%.2f%%)" % spread)
            else:
                reasons.append("Yield curve flat (%.2f%%)" % spread)

        # VIX level.
        vix = values.get("vix")
        if vix is not None:
            if vix < 15:
                scores.append(0.3)
                reasons.append("Low VIX (%.1f)" % vix)
            elif vix > 25:
                scores.append(-0.4)
                reasons.append("High VIX (%.1f)" % vix)
            elif vix > 30:
                scores.append(-0.6)
                reasons.append("Extreme VIX (%.1f)" % vix)

        if not scores:
            return "neutral", 0.0, "Insufficient data"

        avg = sum(scores) / len(scores)
        if avg > 0.15:
            regime = "risk_on"
        elif avg < -0.15:
            regime = "risk_off"
        else:
            regime = "neutral"

        return regime, max(-1.0, min(1.0, avg)), " | ".join(reasons)

    # -- FRED API ------------------------------------------------------------

    async def _fetch_fred(self, series_id: str, api_key: str) -> float:
        """Fetch the latest observation for a FRED series."""
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(_FRED_BASE, params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 5,
                })
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            self.log.debug("FRED fetch failed for %s", series_id, exc_info=True)
            raise

        for obs in data.get("observations", []):
            val = obs.get("value", ".")
            if val != ".":
                return float(val)
        raise ValueError("No valid observation for %s" % series_id)

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"regime": self._current_regime,
                     "signals_emitted": self._signals_emitted})
        return base
