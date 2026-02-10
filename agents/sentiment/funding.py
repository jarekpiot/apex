"""
SentimentFunding — funding rate and open-interest as crowd-positioning proxy.

Different from FundingRateArb (which seeks arb opportunities):
this agent uses aggregate funding and OI as a *sentiment* indicator.

Logic:
  - Very positive aggregate funding → crowd is long → contrarian bearish
  - Very negative aggregate funding → crowd is short → contrarian bullish
  - Rapidly rising OI + positive funding → euphoria → bearish signal
  - Rapidly falling OI → capitulation → potential bottom → bullish

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
from core.message_bus import MessageBus
from core.models import AgentSignal, Timeframe

_HL_INFO_URL = "https://api.hyperliquid.xyz/info"
_HL_TESTNET_INFO_URL = "https://api.hyperliquid-testnet.xyz/info"


class SentimentFunding(BaseAgent):
    """Funding rate + OI as crowd-positioning sentiment proxy."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="sentiment_funding",
            agent_type="sentiment",
            bus=bus,
            **kw,
        )
        self._funding_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=144),
        )
        self._oi_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=144),
        )
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []
        self._info_url = (
            _HL_TESTNET_INFO_URL if settings.hyperliquid_testnet
            else _HL_INFO_URL
        )

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="sf:scan"),
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
        await asyncio.sleep(15)
        while True:
            try:
                await self._scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Sentiment-funding scan failed.")
            await asyncio.sleep(settings.sentiment_scan_interval)

    async def _scan(self) -> None:
        data = await self._fetch_hl_meta()
        if not data:
            return

        for asset, funding, oi in data:
            self._funding_history[asset].append(funding)
            self._oi_history[asset].append(oi)

            direction, conviction, reasoning = self._evaluate(asset)
            if conviction < 0.1:
                continue

            signal = AgentSignal(
                agent_id=self.agent_id,
                asset=asset,
                direction=direction,
                conviction=conviction,
                timeframe=Timeframe.INTRADAY,
                reasoning=reasoning,
                data_sources=["hyperliquid:funding", "hyperliquid:oi"],
                metadata={"funding": round(funding, 6), "oi": round(oi, 2)},
                expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            await self.emit_signal(signal)
            self._signals_emitted += 1

    def _evaluate(self, asset: str) -> tuple[float, float, str]:
        funding_hist = list(self._funding_history.get(asset, []))
        oi_hist = list(self._oi_history.get(asset, []))

        if len(funding_hist) < 6 or len(oi_hist) < 6:
            return 0.0, 0.0, ""

        avg_funding = sum(funding_hist[-6:]) / 6
        current_funding = funding_hist[-1]

        # OI momentum: compare recent vs older.
        oi_recent = sum(oi_hist[-3:]) / 3
        oi_older = sum(oi_hist[-6:-3]) / 3 if len(oi_hist) >= 6 else oi_recent
        oi_change = (oi_recent - oi_older) / oi_older if oi_older > 0 else 0

        scores: list[float] = []
        reasons: list[str] = []

        # Contrarian funding signal.
        extreme = settings.funding_rate_extreme
        if avg_funding > extreme:
            # Crowd is long → contrarian bearish.
            intensity = min(abs(avg_funding) / extreme, 3.0)
            scores.append(-0.3 * intensity)
            reasons.append("Crowd long (funding %+.4f%%)" % (avg_funding * 100))
        elif avg_funding < -extreme:
            # Crowd is short → contrarian bullish.
            intensity = min(abs(avg_funding) / extreme, 3.0)
            scores.append(0.3 * intensity)
            reasons.append("Crowd short (funding %+.4f%%)" % (avg_funding * 100))

        # OI momentum.
        if oi_change > 0.05 and avg_funding > extreme:
            # Rising OI + positive funding → euphoria → bearish.
            scores.append(-0.3)
            reasons.append("Euphoria: OI +%.1f%% + positive funding" % (oi_change * 100))
        elif oi_change < -0.05:
            # Falling OI → capitulation → potential bottom.
            scores.append(0.2)
            reasons.append("Capitulation: OI %.1f%%" % (oi_change * 100))

        if not scores:
            return 0.0, 0.0, ""

        direction = max(-1.0, min(1.0, sum(scores)))
        conviction = min(abs(direction) * 1.2, 0.7)
        return direction, conviction, " | ".join(reasons[:3])

    # -- data fetching -------------------------------------------------------

    async def _fetch_hl_meta(self) -> list[tuple[str, float, float]]:
        """Returns [(asset, funding_rate, open_interest), ...]"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    self._info_url, json={"type": "metaAndAssetCtxs"},
                )
                resp.raise_for_status()
                raw = resp.json()
        except Exception:
            self.log.warning("HL meta fetch failed.", exc_info=True)
            return []

        results: list[tuple[str, float, float]] = []
        if isinstance(raw, list) and len(raw) >= 2:
            universe = raw[0].get("universe", [])
            ctxs = raw[1]
            for i, ctx in enumerate(ctxs):
                if i >= len(universe):
                    break
                name = universe[i].get("name", "")
                funding = float(ctx.get("funding", 0) or 0)
                oi = float(ctx.get("openInterest", 0) or 0)
                if name:
                    results.append((name, funding, oi))
        return results

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"signals_emitted": self._signals_emitted,
                     "assets_tracked": len(self._funding_history)})
        return base
