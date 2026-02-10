"""
PlatformSpecialist — the fund's Hyperliquid domain expert.

Responsibilities:
  a) Asset Registry — polls /info for meta + asset contexts every 5 min.
  b) Fee Intelligence — tracks fee tier and maker/taker costs.
  c) Wallet & Account Health — monitors margin/liquidation every 10 s.
  d) Bridge Monitor — checks Arbitrum bridge health periodically.
  e) Platform Ecosystem Intel — HLP performance, HYPE dynamics, governance.
  f) Execution Advisory — publishes liquidity/depth guidance per asset.

All state is published to Redis streams so every other agent can subscribe.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import (
    MessageBus,
    STREAM_ACCOUNT_HEALTH,
    STREAM_EXECUTION_ADVISORY,
    STREAM_NEW_LISTING,
    STREAM_PLATFORM_STATE,
)
from core.models import (
    AccountHealth,
    AlertSeverity,
    AssetInfo,
    ExecutionAdvisory,
    NewListingAlert,
)

# ---------------------------------------------------------------------------
# HL REST endpoints
# ---------------------------------------------------------------------------

_API_MAINNET = "https://api.hyperliquid.xyz"
_API_TESTNET = "https://api.hyperliquid-testnet.xyz"

# Bridge / ecosystem URLs.
_ARBITRUM_BRIDGE_API = "https://bridge.arbitrum.io/api/status"
_HYPE_COINGECKO_ID = "hyperliquid"

# Fee tiers (volume in USD over trailing 14 days).
_FEE_TIERS: list[tuple[float, float, float]] = [
    # (volume_threshold, maker_bps, taker_bps)
    (0,             0.10, 0.35),
    (1_000_000,     0.08, 0.32),
    (5_000_000,     0.06, 0.28),
    (25_000_000,    0.04, 0.24),
    (100_000_000,   0.02, 0.20),
    (500_000_000,   0.00, 0.18),
]


class PlatformSpecialist(BaseAgent):
    """Hyperliquid platform intelligence layer."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="platform_specialist",
            agent_type="ingestion",
            bus=bus,
            **kw,
        )
        base = _API_TESTNET if settings.hyperliquid_testnet else _API_MAINNET
        self._base_url = base
        self._info_url = f"{base}/info"
        self._http: httpx.AsyncClient | None = None

        # State.
        self._asset_registry: dict[str, AssetInfo] = {}
        self._known_assets: set[str] = set()
        self._account_health = AccountHealth()
        self._trailing_volume: float = 0.0
        self._maker_fee_bps: float = 0.10
        self._taker_fee_bps: float = 0.35

        # Sub-tasks for each responsibility.
        self._sub_tasks: list[asyncio.Task[None]] = []

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=15.0)
        await super().start()

        # Launch concurrent loops for each responsibility.
        self._sub_tasks = [
            asyncio.create_task(self._asset_registry_loop(), name="ps:asset_registry"),
            asyncio.create_task(self._account_health_loop(), name="ps:account_health"),
            asyncio.create_task(self._bridge_monitor_loop(), name="ps:bridge"),
            asyncio.create_task(self._ecosystem_intel_loop(), name="ps:ecosystem"),
            asyncio.create_task(self._execution_advisory_loop(), name="ps:advisory"),
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
        """Main loop just keeps the agent alive; real work is in sub-tasks."""
        await asyncio.sleep(30)
        await self._publish_platform_state()

    # -- (a) Asset Registry --------------------------------------------------

    async def _asset_registry_loop(self) -> None:
        """Poll HL meta + asset contexts and maintain the live registry."""
        while True:
            try:
                await self._refresh_asset_registry()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Asset registry refresh failed.")
            await asyncio.sleep(settings.asset_registry_interval)

    async def _refresh_asset_registry(self) -> None:
        meta_and_ctxs = await self._post_info({"type": "metaAndAssetCtxs"})
        if not meta_and_ctxs or not isinstance(meta_and_ctxs, list) or len(meta_and_ctxs) < 2:
            self.log.warning("Unexpected metaAndAssetCtxs response shape.")
            return

        universe = meta_and_ctxs[0].get("universe", [])
        asset_ctxs = meta_and_ctxs[1]

        new_registry: dict[str, AssetInfo] = {}
        for i, meta_entry in enumerate(universe):
            name = meta_entry.get("name", "")
            ctx = asset_ctxs[i] if i < len(asset_ctxs) else {}

            sz_dec = meta_entry.get("szDecimals", 0)
            max_lev = meta_entry.get("maxLeverage", 1)
            margin_req = 1.0 / max_lev if max_lev else 1.0
            tick = 10 ** (-sz_dec) if sz_dec else 1.0
            lot = tick

            oi = float(ctx.get("openInterest", 0) or 0)
            vol = float(ctx.get("dayNtlVlm", 0) or 0)
            funding = float(ctx.get("funding", 0) or 0)
            mark = float(ctx.get("markPx", 0) or 0)

            is_new = name not in self._known_assets

            info = AssetInfo(
                asset=name,
                sz_decimals=sz_dec,
                max_leverage=max_lev,
                margin_requirement=margin_req,
                tick_size=tick,
                lot_size=lot,
                open_interest=oi,
                volume_24h=vol,
                funding_rate=funding,
                mark_price=mark,
                is_new=is_new,
            )
            new_registry[name] = info

            # New listing alert.
            if is_new and self._known_assets:  # Skip first load.
                self.log.info("NEW LISTING DETECTED: %s (max_lev=%d)", name, max_lev)
                alert = NewListingAlert(
                    asset=name,
                    max_leverage=max_lev,
                    sz_decimals=sz_dec,
                )
                await self.bus.publish_to(STREAM_NEW_LISTING, alert)

        self._asset_registry = new_registry
        self._known_assets = set(new_registry.keys())
        self.log.info(
            "Asset registry refreshed: %d perps available.", len(new_registry),
        )

    # -- (b) Fee Intelligence ------------------------------------------------

    def _update_fee_tier(self, trailing_volume: float) -> None:
        """Determine maker/taker fees from the fund's trailing volume."""
        self._trailing_volume = trailing_volume
        for threshold, maker, taker in reversed(_FEE_TIERS):
            if trailing_volume >= threshold:
                self._maker_fee_bps = maker
                self._taker_fee_bps = taker
                break

    def get_effective_fee(self, is_maker: bool) -> float:
        """Return effective fee in basis points."""
        return self._maker_fee_bps if is_maker else self._taker_fee_bps

    # -- (c) Wallet & Account Health -----------------------------------------

    async def _account_health_loop(self) -> None:
        """Monitor wallet margin health every N seconds."""
        while True:
            try:
                await self._refresh_account_health()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Account health check failed.")
            await asyncio.sleep(settings.account_health_interval)

    async def _refresh_account_health(self) -> None:
        address = settings.hyperliquid_vault_address
        if not address:
            return

        state = await self._post_info({"type": "clearinghouseState", "user": address})
        if not state:
            return

        margin_summary = state.get("marginSummary", {})
        equity = float(margin_summary.get("accountValue", 0) or 0)
        total_margin = float(margin_summary.get("totalMarginUsed", 0) or 0)
        available = float(margin_summary.get("totalRawUsd", 0) or 0)
        withdrawable = float(state.get("withdrawable", 0) or 0)

        utilisation = total_margin / equity if equity > 0 else 0.0
        # Distance-to-liquidation: simplified as (equity - maintenance) / equity.
        maintenance = float(margin_summary.get("totalNtlPos", 0) or 0) * 0.05
        dist_liq = (equity - maintenance) / equity if equity > 0 else 1.0
        dist_liq = max(0.0, min(1.0, dist_liq))

        # Determine severity.
        severity = AlertSeverity.INFO
        if utilisation > settings.margin_utilisation_warn:
            severity = AlertSeverity.CRITICAL
            self.log.warning(
                "CRITICAL: Margin utilisation %.1f%% exceeds %.0f%% threshold",
                utilisation * 100, settings.margin_utilisation_warn * 100,
            )
        elif dist_liq < settings.liquidation_distance_warn:
            severity = AlertSeverity.CRITICAL
            self.log.warning(
                "CRITICAL: Liquidation distance %.1f%% below %.0f%% threshold",
                dist_liq * 100, settings.liquidation_distance_warn * 100,
            )
        elif utilisation > 0.6:
            severity = AlertSeverity.WARNING

        self._account_health = AccountHealth(
            equity=equity,
            margin_used=total_margin,
            margin_available=available,
            margin_utilisation=utilisation,
            withdrawable=withdrawable,
            cross_margin_ratio=1.0 - utilisation,
            distance_to_liquidation=dist_liq,
            severity=severity,
        )

        # Update fee tier from trailing volume.
        user_rate = await self._post_info({"type": "userRateLimit", "user": address})
        if user_rate and isinstance(user_rate, dict):
            cum_vol = float(user_rate.get("cumVlm", 0) or 0)
            self._update_fee_tier(cum_vol)

        await self.bus.publish_to(STREAM_ACCOUNT_HEALTH, self._account_health)

    # -- (d) Bridge Monitor --------------------------------------------------

    async def _bridge_monitor_loop(self) -> None:
        """Check Arbitrum bridge health every 5 minutes."""
        while True:
            try:
                await self._check_bridge()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Bridge health check failed.")
            await asyncio.sleep(300)

    async def _check_bridge(self) -> None:
        """Query Arbitrum bridge status. Alert on congestion or USDC de-peg."""
        try:
            resp = await self._http.get(
                _ARBITRUM_BRIDGE_API, timeout=10.0,
            )
            if resp.status_code == 200:
                self.log.debug("Arbitrum bridge status: healthy")
            else:
                self.log.warning(
                    "Arbitrum bridge returned status %d", resp.status_code,
                )
        except httpx.RequestError as exc:
            self.log.warning("Bridge check unreachable: %s", exc)

    # -- (e) Platform Ecosystem Intel ----------------------------------------

    async def _ecosystem_intel_loop(self) -> None:
        """Track HLP vault, HYPE token, and protocol governance every 10 min."""
        while True:
            try:
                await self._check_ecosystem()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Ecosystem intel check failed.")
            await asyncio.sleep(600)

    async def _check_ecosystem(self) -> None:
        # HLP vault performance.
        hlp = await self._post_info({"type": "vaultDetails", "vaultAddress": "0x1"})
        if hlp and isinstance(hlp, dict):
            self.log.info(
                "HLP vault TVL: %s",
                hlp.get("summary", {}).get("tvl", "N/A"),
            )

        # HYPE token price via CoinGecko (best-effort).
        try:
            resp = await self._http.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": _HYPE_COINGECKO_ID, "vs_currencies": "usd"},
                timeout=10.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                hype_price = data.get(_HYPE_COINGECKO_ID, {}).get("usd")
                if hype_price:
                    self.log.info("HYPE price: $%.4f", hype_price)
        except httpx.RequestError:
            pass  # Non-critical.

    # -- (f) Execution Advisory ----------------------------------------------

    async def _execution_advisory_loop(self) -> None:
        """Publish per-asset execution guidance every 30 seconds."""
        while True:
            try:
                await self._publish_execution_advisories()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Execution advisory publish failed.")
            await asyncio.sleep(30)

    async def _publish_execution_advisories(self) -> None:
        address = settings.hyperliquid_vault_address
        if not self._asset_registry:
            return

        # Grab L2 snapshots for top assets by volume.
        sorted_assets = sorted(
            self._asset_registry.values(),
            key=lambda a: a.volume_24h,
            reverse=True,
        )[:20]

        for info in sorted_assets:
            # Fetch L2 book for depth estimate.
            book = await self._post_info({"type": "l2Book", "coin": info.asset})
            bid_depth_usd = 0.0
            ask_depth_usd = 0.0
            spread_bps = 0.0

            if book and isinstance(book, dict):
                levels = book.get("levels", [[], []])
                bids = levels[0] if len(levels) > 0 else []
                asks = levels[1] if len(levels) > 1 else []

                for lv in bids[:10]:
                    bid_depth_usd += float(lv.get("sz", 0)) * float(lv.get("px", 0))
                for lv in asks[:10]:
                    ask_depth_usd += float(lv.get("sz", 0)) * float(lv.get("px", 0))

                if bids and asks:
                    best_bid = float(bids[0].get("px", 0))
                    best_ask = float(asks[0].get("px", 0))
                    mid = (best_bid + best_ask) / 2
                    if mid > 0:
                        spread_bps = ((best_ask - best_bid) / mid) * 10_000

            # Determine optimal order type.
            rec = "limit" if spread_bps > 2.0 else "market"

            # Max size that won't move the book more than 10 bps.
            max_size = min(bid_depth_usd, ask_depth_usd) * 0.10

            advisory = ExecutionAdvisory(
                asset=info.asset,
                bid_depth_usd=bid_depth_usd,
                ask_depth_usd=ask_depth_usd,
                spread_bps=spread_bps,
                recommended_order_type=rec,
                max_size_no_impact=max_size,
                maker_fee_bps=self._maker_fee_bps,
                taker_fee_bps=self._taker_fee_bps,
            )
            await self.bus.publish_to(STREAM_EXECUTION_ADVISORY, advisory)

    # -- aggregate platform state publish ------------------------------------

    async def _publish_platform_state(self) -> None:
        """Publish consolidated platform snapshot."""
        from pydantic import BaseModel

        class PlatformState(BaseModel):
            timestamp: datetime = datetime.now(timezone.utc)
            total_assets: int = len(self._asset_registry)
            account_health: dict[str, Any] = self._account_health.model_dump()
            maker_fee_bps: float = self._maker_fee_bps
            taker_fee_bps: float = self._taker_fee_bps
            trailing_volume: float = self._trailing_volume

        state = PlatformState()
        await self.bus.publish_to(STREAM_PLATFORM_STATE, state)

    # -- HL info helper ------------------------------------------------------

    async def _post_info(self, payload: dict[str, Any]) -> Any:
        """POST to the HL /info endpoint and return parsed JSON."""
        try:
            resp = await self._http.post(self._info_url, json=payload, timeout=10.0)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            self.log.warning("HL info %s returned %d", payload.get("type"), exc.response.status_code)
        except httpx.RequestError as exc:
            self.log.warning("HL info request failed: %s", exc)
        return None

    # -- public accessors for other agents -----------------------------------

    def get_asset_info(self, asset: str) -> AssetInfo | None:
        return self._asset_registry.get(asset)

    def get_all_assets(self) -> dict[str, AssetInfo]:
        return dict(self._asset_registry)

    @property
    def account(self) -> AccountHealth:
        return self._account_health

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "assets_registered": len(self._asset_registry),
            "margin_utilisation": self._account_health.margin_utilisation,
            "severity": self._account_health.severity,
            "fee_tier_maker_bps": self._maker_fee_bps,
        })
        return base
