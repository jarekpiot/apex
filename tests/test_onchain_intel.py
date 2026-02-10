"""Tests for OnChainIntelligence — data parsing, backoff, and fetch logic."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from agents.ingestion.onchain_intel import OnChainIntelligence, _HARDCODED_UNLOCKS
from core.models import OnChainDataPoint
from tests.conftest import MockMessageBus


def _make_agent() -> OnChainIntelligence:
    agent = OnChainIntelligence(bus=MockMessageBus())
    agent._http = httpx.AsyncClient()  # Will be mocked per-test.
    agent._persist_metrics = AsyncMock()  # No real DB.
    return agent


# ---------------------------------------------------------------------------
# _get_json backoff
# ---------------------------------------------------------------------------

class TestGetJsonBackoff:
    async def test_429_returns_none_and_sets_backoff(self):
        agent = _make_agent()
        mock_resp = AsyncMock()
        mock_resp.status_code = 429
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        result = await agent._get_json("http://example.com", "test_source")
        assert result is None
        assert "test_source" in agent._backoff
        assert agent._backoff["test_source"] == 2.0  # 1.0 * 2

    async def test_500_returns_none_and_sets_backoff(self):
        agent = _make_agent()
        mock_resp = AsyncMock()
        mock_resp.status_code = 500
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        result = await agent._get_json("http://example.com", "srv_err")
        assert result is None
        assert "srv_err" in agent._backoff

    async def test_backoff_doubles_on_repeated_failure(self):
        agent = _make_agent()
        agent._backoff["src"] = 4.0
        mock_resp = AsyncMock()
        mock_resp.status_code = 429
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        await agent._get_json("http://example.com", "src")
        assert agent._backoff["src"] == 8.0

    async def test_backoff_capped_at_60(self):
        agent = _make_agent()
        agent._backoff["src"] = 50.0
        mock_resp = AsyncMock()
        mock_resp.status_code = 429
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        await agent._get_json("http://example.com", "src")
        assert agent._backoff["src"] == 60.0

    async def test_success_clears_backoff(self):
        agent = _make_agent()
        agent._backoff["src"] = 8.0
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ok": True}
        mock_resp.raise_for_status = MagicMock()
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        result = await agent._get_json("http://example.com", "src")
        assert result == {"ok": True}
        assert "src" not in agent._backoff


# ---------------------------------------------------------------------------
# _fetch_tvl
# ---------------------------------------------------------------------------

class TestFetchTvl:
    async def test_populates_chain_and_protocol_tvls(self):
        agent = _make_agent()
        chains_resp = [
            {"name": "Ethereum", "tvl": 50_000_000_000},
            {"name": "Solana", "tvl": 5_000_000_000},
        ]
        protocols_resp = [
            {"name": "Aave", "tvl": 10_000_000_000},
            {"name": "Uniswap", "tvl": 5_000_000_000},
        ]

        call_count = 0

        async def mock_get_json(url: str, source: str):
            nonlocal call_count
            call_count += 1
            if "chains" in url:
                return chains_resp
            return protocols_resp

        agent._get_json = mock_get_json

        await agent._fetch_tvl()

        assert agent._data.chain_tvls["Ethereum"] == 50_000_000_000
        assert agent._data.chain_tvls["Solana"] == 5_000_000_000
        assert agent._data.protocol_tvls["Aave"] == 10_000_000_000
        assert len(agent._data.protocol_tvls) == 2

    async def test_handles_empty_response(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)

        await agent._fetch_tvl()
        # Should not crash; tvls stay at defaults.
        assert agent._data.chain_tvls == {}

    async def test_skips_zero_tvl_entries(self):
        agent = _make_agent()
        chains_resp = [
            {"name": "Ethereum", "tvl": 50_000},
            {"name": "Dead", "tvl": 0},
        ]

        async def mock_get_json(url: str, source: str):
            if "chains" in url:
                return chains_resp
            return []

        agent._get_json = mock_get_json
        await agent._fetch_tvl()

        assert "Ethereum" in agent._data.chain_tvls
        assert "Dead" not in agent._data.chain_tvls


# ---------------------------------------------------------------------------
# _fetch_dex_volume
# ---------------------------------------------------------------------------

class TestFetchDexVolume:
    async def test_populates_dex_volumes(self):
        agent = _make_agent()
        resp = {
            "totalDataChart": [{"totalVolume": 3_000_000_000}],
            "protocols": [
                {"name": "Uniswap", "total24h": 1_200_000_000},
                {"name": "PancakeSwap", "total24h": 800_000_000},
            ],
        }
        agent._get_json = AsyncMock(return_value=resp)

        await agent._fetch_dex_volume()

        assert agent._data.total_dex_volume_24h == 3_000_000_000
        assert agent._data.dex_volumes_24h["Uniswap"] == 1_200_000_000
        assert len(agent._data.dex_volumes_24h) == 2

    async def test_handles_none_response(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        await agent._fetch_dex_volume()
        assert agent._data.total_dex_volume_24h == 0.0

    async def test_empty_total_data_chart(self):
        agent = _make_agent()
        resp = {"totalDataChart": [], "protocols": []}
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_dex_volume()
        assert agent._data.total_dex_volume_24h == 0.0


# ---------------------------------------------------------------------------
# _fetch_stablecoins
# ---------------------------------------------------------------------------

class TestFetchStablecoins:
    async def test_populates_stablecoin_breakdown(self):
        agent = _make_agent()
        resp = {
            "peggedAssets": [
                {"symbol": "USDT", "circulating": {"peggedUSD": 80_000_000_000}},
                {"symbol": "USDC", "circulating": {"peggedUSD": 30_000_000_000}},
            ],
        }
        agent._get_json = AsyncMock(return_value=resp)

        await agent._fetch_stablecoins()

        assert agent._data.stablecoin_breakdown["USDT"] == 80_000_000_000
        assert agent._data.stablecoin_breakdown["USDC"] == 30_000_000_000
        assert agent._data.total_stablecoin_mcap == pytest.approx(110_000_000_000)

    async def test_skips_zero_mcap(self):
        agent = _make_agent()
        resp = {
            "peggedAssets": [
                {"symbol": "USDT", "circulating": {"peggedUSD": 80_000}},
                {"symbol": "DEAD", "circulating": {"peggedUSD": 0}},
            ],
        }
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_stablecoins()
        assert "DEAD" not in agent._data.stablecoin_breakdown


# ---------------------------------------------------------------------------
# _fetch_trending
# ---------------------------------------------------------------------------

class TestFetchTrending:
    async def test_populates_trending_tokens(self):
        agent = _make_agent()
        trending_resp = {
            "coins": [
                {"item": {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "score": 0}},
                {"item": {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 5, "score": 1}},
            ],
        }
        market_resp = [
            {"symbol": "btc", "price_change_percentage_24h": 5.2, "total_volume": 30e9, "market_cap": 800e9},
            {"symbol": "sol", "price_change_percentage_24h": -3.1, "total_volume": 2e9, "market_cap": 40e9},
        ]

        call_count = 0

        async def mock_get_json(url: str, source: str):
            nonlocal call_count
            call_count += 1
            if "trending" in url:
                return trending_resp
            return market_resp

        agent._get_json = mock_get_json
        await agent._fetch_trending()

        assert len(agent._data.trending_tokens) == 2
        assert agent._data.trending_tokens[0]["id"] == "bitcoin"
        assert agent._data.market_cap_changes["BTC"] == pytest.approx(5.2)
        assert agent._data.market_cap_changes["SOL"] == pytest.approx(-3.1)

    async def test_handles_no_trending_data(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        await agent._fetch_trending()
        assert agent._data.trending_tokens == []


# ---------------------------------------------------------------------------
# _fetch_fear_greed
# ---------------------------------------------------------------------------

class TestFetchFearGreed:
    async def test_populates_fear_greed(self):
        agent = _make_agent()
        resp = {"data": [{"value": "72", "value_classification": "Greed"}]}
        agent._get_json = AsyncMock(return_value=resp)

        await agent._fetch_fear_greed()

        assert agent._data.fear_greed_index == 72
        assert agent._data.fear_greed_label == "Greed"

    async def test_handles_empty_data(self):
        agent = _make_agent()
        resp = {"data": []}
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_fear_greed()
        assert agent._data.fear_greed_index == 0

    async def test_handles_none_response(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        await agent._fetch_fear_greed()
        assert agent._data.fear_greed_index == 0


# ---------------------------------------------------------------------------
# _fetch_token_unlocks
# ---------------------------------------------------------------------------

class TestFetchTokenUnlocks:
    async def test_uses_hardcoded_when_api_fails(self):
        agent = _make_agent()
        agent._try_unlocks_api = AsyncMock(return_value=None)

        # Patch "now" so hardcoded dates are in the future.
        future_unlocks = [
            {"token": "TEST", "date": (datetime.now(timezone.utc) + timedelta(days=5)).strftime("%Y-%m-%d"),
             "amount_pct": 2.0, "description": "Test unlock"},
        ]
        with patch.object(agent, "_try_unlocks_api", return_value=None):
            # Override _HARDCODED_UNLOCKS for deterministic test.
            import agents.ingestion.onchain_intel as mod
            original = mod._HARDCODED_UNLOCKS
            mod._HARDCODED_UNLOCKS = future_unlocks
            try:
                await agent._fetch_token_unlocks()
                assert len(agent._data.upcoming_unlocks) == 1
                assert agent._data.upcoming_unlocks[0]["token"] == "TEST"
            finally:
                mod._HARDCODED_UNLOCKS = original

    async def test_filters_past_unlocks(self):
        agent = _make_agent()
        past_unlock = [
            {"token": "OLD", "date": "2020-01-01", "amount_pct": 5.0, "description": "old"},
        ]
        agent._try_unlocks_api = AsyncMock(return_value=past_unlock)
        await agent._fetch_token_unlocks()
        assert len(agent._data.upcoming_unlocks) == 0

    async def test_filters_far_future_unlocks(self):
        agent = _make_agent()
        far_future = [
            {"token": "FAR", "date": "2030-01-01", "amount_pct": 1.0, "description": "way off"},
        ]
        agent._try_unlocks_api = AsyncMock(return_value=far_future)
        await agent._fetch_token_unlocks()
        assert len(agent._data.upcoming_unlocks) == 0

    async def test_invalid_date_skipped(self):
        agent = _make_agent()
        bad = [{"token": "BAD", "date": "not-a-date", "amount_pct": 1.0, "description": "bad"}]
        agent._try_unlocks_api = AsyncMock(return_value=bad)
        await agent._fetch_token_unlocks()
        assert len(agent._data.upcoming_unlocks) == 0


# ---------------------------------------------------------------------------
# _try_unlocks_api
# ---------------------------------------------------------------------------

class TestTryUnlocksApi:
    async def test_returns_none_on_non_list(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value={"error": "not a list"})
        result = await agent._try_unlocks_api()
        assert result is None

    async def test_returns_none_on_none(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        result = await agent._try_unlocks_api()
        assert result is None

    async def test_parses_unlock_events(self):
        agent = _make_agent()
        api_resp = [
            {
                "symbol": "ARB",
                "unlock_events": [
                    {"date": "2025-06-01", "percent": 2.5, "description": "Team vesting"},
                ],
            },
        ]
        agent._get_json = AsyncMock(return_value=api_resp)
        result = await agent._try_unlocks_api()
        assert result is not None
        assert len(result) == 1
        assert result[0]["token"] == "ARB"
        assert result[0]["amount_pct"] == 2.5

    async def test_returns_none_on_empty_events(self):
        agent = _make_agent()
        api_resp = [{"symbol": "X", "unlock_events": []}]
        agent._get_json = AsyncMock(return_value=api_resp)
        result = await agent._try_unlocks_api()
        assert result is None


# ---------------------------------------------------------------------------
# Model defaults
# ---------------------------------------------------------------------------

class TestOnChainDataPointDefaults:
    def test_all_fields_have_defaults(self):
        """OnChainDataPoint should be constructable with no args."""
        dp = OnChainDataPoint()
        assert dp.source == "onchain_intel"
        assert dp.protocol_tvls == {}
        assert dp.chain_tvls == {}
        assert dp.total_stablecoin_mcap == 0.0
        assert dp.fear_greed_index == 0
        assert dp.trending_tokens == []
        assert dp.upcoming_unlocks == []
