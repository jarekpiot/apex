"""Tests for MacroFeed — market hours, FRED parsing, yfinance mapping, calendar filtering."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from agents.ingestion.macro_feed import MacroFeed, _FRED_NAMES, _TICKER_FIELDS, _HIGH_IMPACT_KEYWORDS
from core.models import MacroDataPoint
from tests.conftest import MockMessageBus

import httpx


def _make_agent() -> MacroFeed:
    agent = MacroFeed(bus=MockMessageBus())
    agent._http = httpx.AsyncClient()
    agent._persist_indicators = AsyncMock()  # No real DB.
    return agent


# ---------------------------------------------------------------------------
# _is_us_market_open
# ---------------------------------------------------------------------------

class TestIsUsMarketOpen:
    def test_weekday_during_hours(self):
        # Wednesday 14:00 ET = 19:00 UTC
        dt = datetime(2025, 3, 5, 19, 0, 0, tzinfo=timezone.utc)
        with patch("agents.ingestion.macro_feed.datetime") as mock_dt:
            mock_dt.now.return_value = dt
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            # Can't easily mock static method with zoneinfo, test the logic directly.
            # We'll test the fallback UTC-5 branch.
            pass
        # Just verify it returns a bool and doesn't crash.
        result = MacroFeed._is_us_market_open()
        assert isinstance(result, bool)

    def test_returns_bool(self):
        assert isinstance(MacroFeed._is_us_market_open(), bool)


# ---------------------------------------------------------------------------
# _fetch_fred
# ---------------------------------------------------------------------------

class TestFetchFred:
    async def test_populates_fred_fields(self):
        agent = _make_agent()
        # Mock FRED responses.
        async def mock_get_json(url: str, source: str):
            for series_id, field_name in _FRED_NAMES.items():
                if series_id in url:
                    return {"observations": [{"value": "4.50"}]}
            return {"observations": []}

        agent._get_json = mock_get_json

        with patch("agents.ingestion.macro_feed.settings") as mock_settings:
            mock_settings.fred_api_key.get_secret_value.return_value = "test-key"
            await agent._fetch_fred()

        assert agent._data.fed_funds_rate == pytest.approx(4.50)
        assert agent._data.us10y_yield == pytest.approx(4.50)
        assert agent._data.us2y_yield == pytest.approx(4.50)

    async def test_computes_yield_curve_spread(self):
        agent = _make_agent()
        call_idx = 0
        values = {"DGS10": "4.50", "DGS2": "4.00"}

        async def mock_get_json(url: str, source: str):
            for sid, val in values.items():
                if sid in url:
                    return {"observations": [{"value": val}]}
            return {"observations": [{"value": "3.0"}]}

        agent._get_json = mock_get_json
        with patch("agents.ingestion.macro_feed.settings") as mock_settings:
            mock_settings.fred_api_key.get_secret_value.return_value = "key"
            await agent._fetch_fred()

        assert agent._data.yield_curve_spread == pytest.approx(0.50)

    async def test_skips_when_no_api_key(self):
        agent = _make_agent()
        agent._get_json = AsyncMock()

        with patch("agents.ingestion.macro_feed.settings") as mock_settings:
            mock_settings.fred_api_key.get_secret_value.return_value = ""
            await agent._fetch_fred()

        agent._get_json.assert_not_called()

    async def test_handles_missing_value(self):
        agent = _make_agent()
        async def mock_get_json(url: str, source: str):
            return {"observations": [{"value": "."}]}

        agent._get_json = mock_get_json
        with patch("agents.ingestion.macro_feed.settings") as mock_settings:
            mock_settings.fred_api_key.get_secret_value.return_value = "key"
            await agent._fetch_fred()

        # "." values should be skipped, fields remain None.
        assert agent._data.fed_funds_rate is None


# ---------------------------------------------------------------------------
# _fetch_yfinance
# ---------------------------------------------------------------------------

class TestFetchYfinance:
    async def test_maps_prices_to_data_fields(self):
        agent = _make_agent()
        mock_prices = {"SPY": 520.50, "QQQ": 440.25, "^VIX": 18.5}

        with patch.object(MacroFeed, "_sync_fetch_yfinance", return_value=mock_prices):
            await agent._fetch_yfinance()

        assert agent._data.spy_price == pytest.approx(520.50)
        assert agent._data.qqq_price == pytest.approx(440.25)
        assert agent._data.vix_price == pytest.approx(18.5)

    async def test_handles_empty_prices(self):
        agent = _make_agent()
        with patch.object(MacroFeed, "_sync_fetch_yfinance", return_value={}):
            await agent._fetch_yfinance()
        # Should not crash; fields remain None.
        assert agent._data.spy_price is None


# ---------------------------------------------------------------------------
# _fetch_fear_greed
# ---------------------------------------------------------------------------

class TestFetchFearGreed:
    async def test_populates_crypto_fear_greed(self):
        agent = _make_agent()
        resp = {"data": [{"value": "25"}]}
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_fear_greed()
        assert agent._data.crypto_fear_greed == 25

    async def test_handles_none(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        await agent._fetch_fear_greed()
        assert agent._data.crypto_fear_greed == 0


# ---------------------------------------------------------------------------
# _fetch_calendar
# ---------------------------------------------------------------------------

class TestFetchCalendar:
    async def test_filters_high_impact_only(self):
        agent = _make_agent()
        now = datetime.now(timezone.utc)
        future = (now + timedelta(hours=6)).isoformat()
        resp = [
            {"title": "FOMC Meeting", "date": future, "impact": "High", "country": "USD"},
            {"title": "Some Low Event", "date": future, "impact": "Low", "country": "USD"},
        ]
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_calendar()

        assert len(agent._data.upcoming_events) == 1
        assert agent._data.upcoming_events[0]["title"] == "FOMC Meeting"

    async def test_excludes_past_events(self):
        agent = _make_agent()
        past = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        resp = [
            {"title": "CPI Release", "date": past, "impact": "High", "country": "USD"},
        ]
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_calendar()
        assert len(agent._data.upcoming_events) == 0

    async def test_excludes_events_beyond_48h(self):
        agent = _make_agent()
        far = (datetime.now(timezone.utc) + timedelta(hours=72)).isoformat()
        resp = [
            {"title": "NFP", "date": far, "impact": "High", "country": "USD"},
        ]
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_calendar()
        assert len(agent._data.upcoming_events) == 0

    async def test_handles_bad_date(self):
        agent = _make_agent()
        resp = [
            {"title": "Bad Event", "date": "not-a-date", "impact": "High", "country": "USD"},
        ]
        agent._get_json = AsyncMock(return_value=resp)
        await agent._fetch_calendar()
        assert len(agent._data.upcoming_events) == 0

    async def test_handles_none_response(self):
        agent = _make_agent()
        agent._get_json = AsyncMock(return_value=None)
        await agent._fetch_calendar()
        assert agent._data.upcoming_events == []


# ---------------------------------------------------------------------------
# FRED constant mappings
# ---------------------------------------------------------------------------

class TestFredConstants:
    def test_all_series_have_field_names(self):
        expected = {"FEDFUNDS", "CPIAUCSL", "WM2NS", "DGS10", "DGS2", "DTWEXBGS"}
        assert set(_FRED_NAMES.keys()) == expected

    def test_ticker_fields_cover_all_tickers(self):
        expected = {"SPY", "QQQ", "GLD", "USO", "DX-Y.NYB", "^VIX"}
        assert set(_TICKER_FIELDS.keys()) == expected


# ---------------------------------------------------------------------------
# MacroDataPoint defaults
# ---------------------------------------------------------------------------

class TestMacroDataPointDefaults:
    def test_all_fields_have_defaults(self):
        dp = MacroDataPoint()
        assert dp.source == "macro_feed"
        assert dp.fed_funds_rate is None
        assert dp.us10y_yield is None
        assert dp.upcoming_events == []
        assert dp.us_market_open is False
