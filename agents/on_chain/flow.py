"""
OnChainFlow — exchange flows, stablecoin supply, and TVL momentum.

Uses DefiLlama public API for:
  - Stablecoin aggregate market-cap changes (capital inflows/outflows)
  - Protocol TVL changes (money flowing into/out of DeFi)
  - Chain-level TVL for ecosystem health

Rising stablecoins + rising TVL = capital entering crypto → bullish.
Falling stablecoins + falling TVL = capital leaving → bearish.

Runs every 10 minutes.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus
from core.models import AgentSignal, Timeframe

_DEFILLAMA = settings.defillama_base_url

# Map HL tickers to DefiLlama chain slugs for chain-level TVL.
_TICKER_TO_CHAIN: dict[str, str] = {
    "ETH": "Ethereum", "SOL": "Solana", "AVAX": "Avalanche",
    "ARB": "Arbitrum", "OP": "Optimism", "MATIC": "Polygon",
    "FTM": "Fantom", "NEAR": "Near", "SUI": "Sui", "APT": "Aptos",
    "SEI": "Sei", "INJ": "Injective",
}


class OnChainFlow(BaseAgent):
    """Capital-flow signals from on-chain data."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="on_chain_flow",
            agent_type="on_chain",
            bus=bus,
            **kw,
        )
        # Rolling TVL snapshots: chain → deque of (timestamp, tvl).
        self._tvl_history: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=144),
        )
        self._stablecoin_mcap_history: deque[tuple[float, float]] = deque(maxlen=144)
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="ocf:scan"),
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
                self.log.exception("On-chain scan failed.")
            await asyncio.sleep(settings.on_chain_scan_interval)

    async def _scan(self) -> None:
        import time
        now = time.time()

        chain_tvls, stable_mcap = await asyncio.gather(
            self._fetch_chain_tvls(),
            self._fetch_stablecoin_mcap(),
            return_exceptions=True,
        )

        if isinstance(chain_tvls, dict):
            for chain, tvl in chain_tvls.items():
                self._tvl_history[chain].append((now, tvl))

        if isinstance(stable_mcap, (int, float)):
            self._stablecoin_mcap_history.append((now, float(stable_mcap)))

        # Stablecoin flow → broad market signal.
        stable_dir, stable_conv, stable_reason = self._stablecoin_signal()
        if stable_conv > 0.1:
            for asset in ("BTC", "ETH"):
                sig = AgentSignal(
                    agent_id=self.agent_id, asset=asset,
                    direction=stable_dir, conviction=stable_conv * 0.8,
                    timeframe=Timeframe.SWING,
                    reasoning="Stablecoin flow: %s" % stable_reason,
                    data_sources=["defillama:stablecoins"],
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                )
                await self.emit_signal(sig)
                self._signals_emitted += 1

        # Per-chain TVL momentum → ecosystem token signal.
        if isinstance(chain_tvls, dict):
            for ticker, chain in _TICKER_TO_CHAIN.items():
                direction, conviction, reason = self._tvl_momentum(chain)
                if conviction < 0.15:
                    continue
                sig = AgentSignal(
                    agent_id=self.agent_id, asset=ticker,
                    direction=direction, conviction=conviction,
                    timeframe=Timeframe.SWING,
                    reasoning="TVL flow: %s" % reason,
                    data_sources=["defillama:chains"],
                    metadata={"chain": chain},
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                )
                await self.emit_signal(sig)
                self._signals_emitted += 1

    # -- signal computation --------------------------------------------------

    def _stablecoin_signal(self) -> tuple[float, float, str]:
        hist = list(self._stablecoin_mcap_history)
        if len(hist) < 6:
            return 0.0, 0.0, ""
        recent = hist[-1][1]
        older = hist[-6][1]  # ~1 hour ago at 10-min intervals
        if older <= 0:
            return 0.0, 0.0, ""
        pct_change = (recent - older) / older
        if pct_change > 0.001:
            return 0.5, min(abs(pct_change) * 100, 0.7), "Stablecoins +%.3f%%" % (pct_change * 100)
        elif pct_change < -0.001:
            return -0.5, min(abs(pct_change) * 100, 0.7), "Stablecoins %.3f%%" % (pct_change * 100)
        return 0.0, 0.0, "Stablecoins flat"

    def _tvl_momentum(self, chain: str) -> tuple[float, float, str]:
        hist = list(self._tvl_history.get(chain, []))
        if len(hist) < 6:
            return 0.0, 0.0, ""
        recent = hist[-1][1]
        older = hist[-6][1]
        if older <= 0:
            return 0.0, 0.0, ""
        pct = (recent - older) / older
        if abs(pct) < 0.005:
            return 0.0, 0.0, "%s TVL flat" % chain
        direction = 0.6 if pct > 0 else -0.6
        conviction = min(abs(pct) * 20, 0.7)
        return direction, conviction, "%s TVL %+.2f%%" % (chain, pct * 100)

    # -- data fetching -------------------------------------------------------

    async def _fetch_chain_tvls(self) -> dict[str, float]:
        url = "%s/v2/chains" % _DEFILLAMA
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            self.log.warning("DefiLlama chains fetch failed.", exc_info=True)
            return {}

        result: dict[str, float] = {}
        if isinstance(data, list):
            for chain in data:
                name = chain.get("name", "")
                tvl = chain.get("tvl", 0)
                if name and tvl:
                    result[name] = float(tvl)
        return result

    async def _fetch_stablecoin_mcap(self) -> float:
        url = "%s/stablecoins" % _DEFILLAMA
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            self.log.warning("DefiLlama stablecoins fetch failed.", exc_info=True)
            return 0.0

        total = 0.0
        for coin in data.get("peggedAssets", []):
            chains = coin.get("chainCirculating", {})
            for chain_data in chains.values():
                current = chain_data.get("current", {})
                total += current.get("peggedUSD", 0)
        return total

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"signals_emitted": self._signals_emitted,
                     "chains_tracked": len(self._tvl_history)})
        return base
