"""
FundamentalValuation — token valuation using on-chain and market metrics.

Fetches data from CoinGecko (market caps, volumes) and DefiLlama (TVL) to
compute valuation ratios for crypto assets tradeable on Hyperliquid.

Metrics:
  - NVT proxy: market_cap / 24h_volume (high = overvalued, low = undervalued)
  - TVL ratio: market_cap / TVL (high = overvalued for DeFi tokens)
  - Volume trend: 24h volume change vs 7d average
  - Market-cap momentum: 24h and 7d price changes

Signal: assets with low NVT + rising volume + healthy TVL = bullish.
Assets with high NVT + falling volume = bearish.
Runs every 15 minutes.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus
from core.models import AgentSignal, Timeframe

# ---------------------------------------------------------------------------
# CoinGecko ticker mapping (HL ticker → CoinGecko id)
# ---------------------------------------------------------------------------

_TICKER_TO_CG: dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "AVAX": "avalanche-2", "DOGE": "dogecoin", "ADA": "cardano",
    "DOT": "polkadot", "LINK": "chainlink", "UNI": "uniswap",
    "AAVE": "aave", "ARB": "arbitrum", "OP": "optimism",
    "MATIC": "matic-network", "ATOM": "cosmos", "NEAR": "near",
    "FTM": "fantom", "INJ": "injective-protocol", "SUI": "sui",
    "APT": "aptos", "SEI": "sei-network", "TIA": "celestia",
    "DYDX": "dydx-chain", "MKR": "maker", "LDO": "lido-dao",
    "SNX": "havven", "CRV": "curve-dao-token", "PENDLE": "pendle",
    "WIF": "dogwifcoin", "PEPE": "pepe", "BONK": "bonk",
    "JUP": "jupiter-exchange-solana", "W": "wormhole",
    "ONDO": "ondo-finance", "ENA": "ethena",
}

# DefiLlama protocol slugs for TVL.
_TICKER_TO_PROTOCOL: dict[str, str] = {
    "UNI": "uniswap", "AAVE": "aave", "MKR": "makerdao",
    "CRV": "curve-dex", "LDO": "lido", "SNX": "synthetix",
    "DYDX": "dydx", "PENDLE": "pendle", "INJ": "injective",
    "JUP": "jupiter", "ENA": "ethena",
}

_CG_BASE = "https://api.coingecko.com/api/v3"

# NVT thresholds (approximate — real values depend on the asset).
_NVT_LOW = 15.0    # Undervalued territory.
_NVT_HIGH = 100.0  # Overvalued territory.


class FundamentalValuation(BaseAgent):
    """Token valuation via CoinGecko + DefiLlama fundamentals."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="fundamental_valuation",
            agent_type="fundamental",
            bus=bus,
            **kw,
        )
        self._metrics: dict[str, dict[str, float]] = {}
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="fv:scan"),
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

    # -- scan loop -----------------------------------------------------------

    async def _scan_loop(self) -> None:
        await asyncio.sleep(20)
        while True:
            try:
                await self._scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Fundamental scan failed.")
            await asyncio.sleep(settings.fundamental_scan_interval)

    async def _scan(self) -> None:
        cg_data = await self._fetch_coingecko()
        tvl_data = await self._fetch_defillama_tvl()

        for ticker, cg_id in _TICKER_TO_CG.items():
            data = cg_data.get(cg_id)
            if data is None:
                continue

            mcap = data.get("market_cap", 0)
            vol_24h = data.get("total_volume", 0)
            pct_24h = data.get("price_change_percentage_24h", 0) or 0
            pct_7d = data.get("price_change_percentage_7d_in_currency", 0) or 0

            if mcap <= 0 or vol_24h <= 0:
                continue

            # NVT proxy.
            nvt = mcap / vol_24h

            # TVL ratio (if available).
            tvl = tvl_data.get(ticker, 0)
            tvl_ratio = mcap / tvl if tvl > 0 else 0.0

            direction, conviction, reasoning = self._evaluate(
                ticker, nvt, tvl_ratio, pct_24h, pct_7d,
            )
            if conviction < 0.1:
                continue

            signal = AgentSignal(
                agent_id=self.agent_id,
                asset=ticker,
                direction=direction,
                conviction=conviction,
                timeframe=Timeframe.SWING,
                reasoning=reasoning,
                data_sources=["coingecko", "defillama"],
                metadata={"nvt": round(nvt, 2), "tvl_ratio": round(tvl_ratio, 2),
                          "pct_24h": round(pct_24h, 2), "pct_7d": round(pct_7d, 2)},
                expires_at=datetime.now(timezone.utc) + timedelta(hours=4),
            )
            await self.emit_signal(signal)
            self._signals_emitted += 1

    # -- evaluation ----------------------------------------------------------

    @staticmethod
    def _evaluate(
        ticker: str, nvt: float, tvl_ratio: float,
        pct_24h: float, pct_7d: float,
    ) -> tuple[float, float, str]:
        scores: list[float] = []
        reasons: list[str] = []

        # NVT score: low = undervalued (bullish), high = overvalued (bearish).
        if nvt < _NVT_LOW:
            scores.append(0.6)
            reasons.append("Low NVT %.1f (undervalued)" % nvt)
        elif nvt > _NVT_HIGH:
            scores.append(-0.6)
            reasons.append("High NVT %.1f (overvalued)" % nvt)
        else:
            scores.append(0.0)

        # TVL ratio (DeFi only).
        if tvl_ratio > 0:
            if tvl_ratio < 3.0:
                scores.append(0.4)
                reasons.append("Low mcap/TVL %.1f" % tvl_ratio)
            elif tvl_ratio > 20.0:
                scores.append(-0.4)
                reasons.append("High mcap/TVL %.1f" % tvl_ratio)
            else:
                scores.append(0.0)

        # Volume / momentum.
        if pct_24h > 5:
            scores.append(0.3)
            reasons.append("24h +%.1f%%" % pct_24h)
        elif pct_24h < -5:
            scores.append(-0.3)
            reasons.append("24h %.1f%%" % pct_24h)
        else:
            scores.append(pct_24h / 20.0)

        if not scores:
            return 0.0, 0.0, ""
        direction = max(-1.0, min(1.0, sum(scores) / len(scores)))
        conviction = min(abs(direction) * 1.2, 0.8)
        return direction, conviction, " | ".join(reasons[:4])

    # -- data fetching -------------------------------------------------------

    async def _fetch_coingecko(self) -> dict[str, dict]:
        ids = ",".join(_TICKER_TO_CG.values())
        url = "%s/coins/markets" % _CG_BASE
        params = {
            "vs_currency": "usd", "ids": ids,
            "order": "market_cap_desc", "per_page": 100,
            "price_change_percentage": "7d",
        }
        headers = {}
        key = settings.coingecko_api_key.get_secret_value()
        if key:
            headers["x-cg-demo-api-key"] = key

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, params=params, headers=headers)
                resp.raise_for_status()
                data = resp.json()
            return {item["id"]: item for item in data} if isinstance(data, list) else {}
        except Exception:
            self.log.warning("CoinGecko fetch failed.", exc_info=True)
            return {}

    async def _fetch_defillama_tvl(self) -> dict[str, float]:
        """Fetch TVL for known DeFi protocols."""
        url = "%s/protocols" % settings.defillama_base_url
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            self.log.warning("DefiLlama fetch failed.", exc_info=True)
            return {}

        slug_to_tvl: dict[str, float] = {}
        if isinstance(data, list):
            for proto in data:
                slug = proto.get("slug", "")
                tvl = proto.get("tvl", 0)
                if slug and tvl:
                    slug_to_tvl[slug] = float(tvl)

        result: dict[str, float] = {}
        for ticker, slug in _TICKER_TO_PROTOCOL.items():
            if slug in slug_to_tvl:
                result[ticker] = slug_to_tvl[slug]
        return result

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"signals_emitted": self._signals_emitted,
                     "assets_with_metrics": len(self._metrics)})
        return base
