"""
OnChainIntelligence — centralised on-chain data ingestion agent.

6 concurrent sub-tasks:
  a) TVL — DefiLlama /v2/chains + /protocols (top 100)        every 15 min
  b) DEX Volume — DefiLlama /overview/dexs                     every 10 min
  c) Stablecoin — DefiLlama /stablecoins                       every 1 hr
  d) Trending — CoinGecko /search/trending + /coins/markets    every 5 min
  e) Fear & Greed — alternative.me /fng                        every 1 hr
  f) Token Unlocks — token.unlocks.app /api/v2/token           every 1 hr

Publishes to ``data:onchain``, persists to ``onchain_metrics``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import OnChainMetricRow, async_session_factory
from core.message_bus import MessageBus, STREAM_DATA_ONCHAIN
from core.models import OnChainDataPoint


# ---------------------------------------------------------------------------
# Hardcoded token unlock schedule (fallback when API is unavailable).
# ---------------------------------------------------------------------------
_HARDCODED_UNLOCKS: list[dict[str, Any]] = [
    {"token": "ARB", "date": "2025-03-16", "amount_pct": 3.49, "description": "Team & advisor vesting"},
    {"token": "APT", "date": "2025-04-12", "amount_pct": 2.64, "description": "Foundation unlock"},
    {"token": "OP", "date": "2025-05-31", "amount_pct": 2.15, "description": "Core contributor vesting"},
    {"token": "SUI", "date": "2025-06-01", "amount_pct": 1.82, "description": "Series A vesting"},
]


class OnChainIntelligence(BaseAgent):
    """Centralised on-chain data ingestion."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="onchain_intel",
            agent_type="ingestion",
            bus=bus,
            **kw,
        )
        self._http: httpx.AsyncClient | None = None
        self._sub_tasks: list[asyncio.Task[None]] = []

        # Accumulated state — latest snapshot.
        self._data = OnChainDataPoint()

        # Per-source backoff state: source -> current_delay (seconds).
        self._backoff: dict[str, float] = {}

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=30.0)
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._tvl_loop(), name="oc:tvl"),
            asyncio.create_task(self._dex_volume_loop(), name="oc:dex"),
            asyncio.create_task(self._stablecoin_loop(), name="oc:stablecoin"),
            asyncio.create_task(self._trending_loop(), name="oc:trending"),
            asyncio.create_task(self._fear_greed_loop(), name="oc:fear_greed"),
            asyncio.create_task(self._token_unlocks_loop(), name="oc:unlocks"),
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
        """Main loop publishes the latest aggregated snapshot."""
        await asyncio.sleep(30)
        self._data.timestamp = datetime.now(timezone.utc)
        await self.bus.publish_to(STREAM_DATA_ONCHAIN, self._data)

    # -- helpers -------------------------------------------------------------

    async def _get_json(self, url: str, source: str) -> Any:
        """GET *url*, return parsed JSON. Applies exponential backoff on failure."""
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

    async def _persist_metrics(self, rows: list[OnChainMetricRow]) -> None:
        if not rows:
            return
        async with async_session_factory() as session:
            session.add_all(rows)
            await session.commit()

    # -- (a) TVL -------------------------------------------------------------

    async def _tvl_loop(self) -> None:
        while True:
            try:
                await self._fetch_tvl()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("TVL fetch failed.")
            await asyncio.sleep(settings.onchain_tvl_interval)

    async def _fetch_tvl(self) -> None:
        now = datetime.now(timezone.utc)
        db_rows: list[OnChainMetricRow] = []

        # Chain TVLs.
        chains = await self._get_json("https://api.llama.fi/v2/chains", "defillama_chains")
        if chains and isinstance(chains, list):
            chain_tvls: dict[str, float] = {}
            for c in chains:
                name = c.get("name", "")
                tvl = float(c.get("tvl", 0) or 0)
                if name and tvl > 0:
                    chain_tvls[name] = tvl
                    db_rows.append(OnChainMetricRow(
                        ts=now, source="defillama", metric_name="chain_tvl",
                        metric_value=tvl, asset=name,
                    ))
            self._data.chain_tvls = chain_tvls

        # Top 100 protocols by TVL.
        protocols = await self._get_json("https://api.llama.fi/protocols", "defillama_protocols")
        if protocols and isinstance(protocols, list):
            protocol_tvls: dict[str, float] = {}
            for p in protocols[:100]:
                name = p.get("name", "")
                tvl = float(p.get("tvl", 0) or 0)
                if name and tvl > 0:
                    protocol_tvls[name] = tvl
                    db_rows.append(OnChainMetricRow(
                        ts=now, source="defillama", metric_name="protocol_tvl",
                        metric_value=tvl, asset=name,
                    ))
            self._data.protocol_tvls = protocol_tvls

        await self._persist_metrics(db_rows)
        self.log.info("TVL updated: %d chains, %d protocols",
                       len(self._data.chain_tvls), len(self._data.protocol_tvls))

    # -- (b) DEX Volume ------------------------------------------------------

    async def _dex_volume_loop(self) -> None:
        while True:
            try:
                await self._fetch_dex_volume()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("DEX volume fetch failed.")
            await asyncio.sleep(settings.onchain_dex_interval)

    async def _fetch_dex_volume(self) -> None:
        data = await self._get_json("https://api.llama.fi/overview/dexs", "defillama_dexs")
        if not data:
            return
        now = datetime.now(timezone.utc)
        db_rows: list[OnChainMetricRow] = []

        total = float(data.get("totalDataChart", [{}])[-1].get("totalVolume", 0) if data.get("totalDataChart") else 0)
        self._data.total_dex_volume_24h = total

        protocols = data.get("protocols", [])
        dex_vols: dict[str, float] = {}
        for p in protocols[:50]:
            name = p.get("name", "")
            vol = float(p.get("total24h", 0) or 0)
            if name and vol > 0:
                dex_vols[name] = vol
                db_rows.append(OnChainMetricRow(
                    ts=now, source="defillama", metric_name="dex_volume_24h",
                    metric_value=vol, asset=name,
                ))
        self._data.dex_volumes_24h = dex_vols
        await self._persist_metrics(db_rows)
        self.log.info("DEX volume updated: total=$%.0f, %d DEXs", total, len(dex_vols))

    # -- (c) Stablecoins -----------------------------------------------------

    async def _stablecoin_loop(self) -> None:
        while True:
            try:
                await self._fetch_stablecoins()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Stablecoin fetch failed.")
            await asyncio.sleep(settings.onchain_stablecoin_interval)

    async def _fetch_stablecoins(self) -> None:
        data = await self._get_json("https://stablecoins.llama.fi/stablecoins", "defillama_stables")
        if not data:
            return
        now = datetime.now(timezone.utc)
        db_rows: list[OnChainMetricRow] = []

        peggedAssets = data.get("peggedAssets", [])
        breakdown: dict[str, float] = {}
        total_mcap = 0.0
        for s in peggedAssets:
            symbol = s.get("symbol", "")
            mcap = float(s.get("circulating", {}).get("peggedUSD", 0) or 0)
            if symbol and mcap > 0:
                breakdown[symbol] = mcap
                total_mcap += mcap
                db_rows.append(OnChainMetricRow(
                    ts=now, source="defillama", metric_name="stablecoin_mcap",
                    metric_value=mcap, asset=symbol,
                ))

        self._data.stablecoin_breakdown = breakdown
        self._data.total_stablecoin_mcap = total_mcap
        await self._persist_metrics(db_rows)
        self.log.info("Stablecoins updated: total mcap=$%.0f, %d assets", total_mcap, len(breakdown))

    # -- (d) Trending --------------------------------------------------------

    async def _trending_loop(self) -> None:
        while True:
            try:
                await self._fetch_trending()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Trending fetch failed.")
            await asyncio.sleep(settings.onchain_trending_interval)

    async def _fetch_trending(self) -> None:
        base = "https://api.coingecko.com/api/v3"
        headers: dict[str, str] = {}
        api_key = settings.coingecko_api_key.get_secret_value()
        if api_key:
            headers["x-cg-demo-api-key"] = api_key

        data = await self._get_json(f"{base}/search/trending", "coingecko_trending")
        if not data:
            return

        coins = data.get("coins", [])
        trending: list[dict[str, Any]] = []
        coin_ids: list[str] = []
        for c in coins[:15]:
            item = c.get("item", {})
            coin_id = item.get("id", "")
            trending.append({
                "id": coin_id,
                "symbol": item.get("symbol", ""),
                "name": item.get("name", ""),
                "market_cap_rank": item.get("market_cap_rank"),
                "score": item.get("score", 0),
            })
            if coin_id:
                coin_ids.append(coin_id)

        # Enrich with market data.
        if coin_ids:
            ids_str = ",".join(coin_ids[:15])
            url = f"{base}/coins/markets?vs_currency=usd&ids={ids_str}&sparkline=false"
            market_data = await self._get_json(url, "coingecko_markets")
            if market_data and isinstance(market_data, list):
                mcap_changes: dict[str, float] = {}
                vol_spikes: dict[str, float] = {}
                for m in market_data:
                    sym = (m.get("symbol") or "").upper()
                    pct = m.get("price_change_percentage_24h")
                    if sym and pct is not None:
                        mcap_changes[sym] = float(pct)
                    vol = float(m.get("total_volume", 0) or 0)
                    mcap = float(m.get("market_cap", 0) or 0)
                    if mcap > 0 and vol / mcap > 0.3:
                        vol_spikes[sym] = vol / mcap
                self._data.market_cap_changes = mcap_changes
                self._data.volume_spikes = vol_spikes

        self._data.trending_tokens = trending
        self.log.info("Trending updated: %d tokens", len(trending))

    # -- (e) Fear & Greed ----------------------------------------------------

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
            entry = entries[0]
            idx = int(entry.get("value", 0))
            label = entry.get("value_classification", "")
            self._data.fear_greed_index = idx
            self._data.fear_greed_label = label

            now = datetime.now(timezone.utc)
            await self._persist_metrics([OnChainMetricRow(
                ts=now, source="alternative.me", metric_name="fear_greed_index",
                metric_value=float(idx),
            )])
            self.log.info("Fear & Greed: %d (%s)", idx, label)

    # -- (f) Token Unlocks ---------------------------------------------------

    async def _token_unlocks_loop(self) -> None:
        while True:
            try:
                await self._fetch_token_unlocks()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Token unlocks fetch failed.")
            await asyncio.sleep(settings.onchain_unlocks_interval)

    async def _fetch_token_unlocks(self) -> None:
        # Try live API first, fall back to hardcoded schedule.
        unlocks = await self._try_unlocks_api()
        if unlocks is None:
            unlocks = _HARDCODED_UNLOCKS

        now = datetime.now(timezone.utc)
        upcoming: list[dict[str, Any]] = []
        for u in unlocks:
            unlock_date_str = u.get("date", "")
            try:
                unlock_date = datetime.fromisoformat(unlock_date_str).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue

            delta = unlock_date - now
            if timedelta(0) < delta < timedelta(days=30):
                upcoming.append(u)
                # Warn 24h before large unlocks (> 1% circulating supply).
                if delta < timedelta(hours=24) and float(u.get("amount_pct", 0)) > 1.0:
                    self.log.warning(
                        "TOKEN UNLOCK in <24h: %s — %.2f%% of supply (%s)",
                        u.get("token"), u.get("amount_pct"), u.get("description"),
                    )

        self._data.upcoming_unlocks = upcoming
        self.log.info("Token unlocks: %d upcoming in next 30d", len(upcoming))

    async def _try_unlocks_api(self) -> list[dict[str, Any]] | None:
        """Attempt to fetch from token.unlocks.app; return None on failure."""
        try:
            data = await self._get_json(
                "https://token.unlocks.app/api/v2/token",
                "token_unlocks",
            )
            if not data or not isinstance(data, list):
                return None
            result: list[dict[str, Any]] = []
            for item in data[:50]:
                events = item.get("unlock_events", item.get("events", []))
                symbol = item.get("symbol", item.get("name", ""))
                for ev in events:
                    result.append({
                        "token": symbol,
                        "date": ev.get("date", ""),
                        "amount_pct": float(ev.get("percent", 0) or 0),
                        "description": ev.get("description", ""),
                    })
            return result if result else None
        except Exception:
            return None
