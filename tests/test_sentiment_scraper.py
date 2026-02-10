"""Tests for SentimentScraper — asset detection, VADER scoring, composite calculation."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agents.ingestion.sentiment import (
    SentimentScraper,
    _ASSET_KEYWORDS,
    _TRACKED_ASSETS,
)
from core.models import SentimentDataPoint
from tests.conftest import MockMessageBus

import httpx


def _make_agent(with_vader: bool = False) -> SentimentScraper:
    agent = SentimentScraper(bus=MockMessageBus())
    agent._http = httpx.AsyncClient()
    agent._persist_scores = AsyncMock()  # No real DB.
    if with_vader:
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            agent._vader = SentimentIntensityAnalyzer()
        except ImportError:
            pass
    return agent


# ---------------------------------------------------------------------------
# _detect_assets
# ---------------------------------------------------------------------------

class TestDetectAssets:
    def test_bitcoin_keyword(self):
        agent = _make_agent()
        assets = agent._detect_assets("I think bitcoin will moon soon")
        assert "BTC" in assets

    def test_btc_ticker(self):
        agent = _make_agent()
        assets = agent._detect_assets("BTC is looking bullish today")
        assert "BTC" in assets

    def test_ethereum_keyword(self):
        agent = _make_agent()
        assets = agent._detect_assets("Ethereum 2.0 is finally here")
        assert "ETH" in assets

    def test_multiple_assets(self):
        agent = _make_agent()
        assets = agent._detect_assets("I'm buying bitcoin and solana today, also looking at cardano")
        assert "BTC" in assets
        assert "SOL" in assets
        assert "ADA" in assets

    def test_no_match(self):
        agent = _make_agent()
        assets = agent._detect_assets("the weather is nice today")
        assert assets == []

    def test_case_insensitive(self):
        agent = _make_agent()
        assets = agent._detect_assets("BITCOIN and ETHEREUM are the top two")
        assert "BTC" in assets
        assert "ETH" in assets

    def test_word_boundary(self):
        agent = _make_agent()
        # "link" should match as a word boundary, not as part of another word
        assets = agent._detect_assets("chainlink integration is great")
        assert "LINK" in assets

    def test_polygon_matic(self):
        agent = _make_agent()
        assets = agent._detect_assets("polygon matic bridge is fast")
        assert "MATIC" in assets

    def test_all_keywords_map_to_tracked_assets(self):
        for keyword, asset in _ASSET_KEYWORDS.items():
            assert asset in _TRACKED_ASSETS, f"Keyword '{keyword}' maps to '{asset}' which is not tracked"


# ---------------------------------------------------------------------------
# _score_text (VADER)
# ---------------------------------------------------------------------------

class TestScoreText:
    def test_returns_zero_without_vader(self):
        agent = _make_agent(with_vader=False)
        assert agent._score_text("This is amazing bullish news!") == 0.0

    def test_positive_text_with_vader(self):
        agent = _make_agent(with_vader=True)
        if agent._vader is None:
            pytest.skip("VADER not installed")
        score = agent._score_text("This is amazing, wonderful, fantastic news!")
        assert score > 0

    def test_negative_text_with_vader(self):
        agent = _make_agent(with_vader=True)
        if agent._vader is None:
            pytest.skip("VADER not installed")
        score = agent._score_text("This is terrible, horrible, awful crash")
        assert score < 0

    def test_neutral_text_with_vader(self):
        agent = _make_agent(with_vader=True)
        if agent._vader is None:
            pytest.skip("VADER not installed")
        score = agent._score_text("The price is 50000")
        # Should be close to 0.
        assert abs(score) < 0.5

    def test_empty_string(self):
        agent = _make_agent(with_vader=True)
        score = agent._score_text("")
        assert score == 0.0


# ---------------------------------------------------------------------------
# _compute_composite
# ---------------------------------------------------------------------------

class TestComputeComposite:
    def test_reddit_only(self):
        sp = SentimentDataPoint(
            asset="BTC",
            reddit_post_count=10,
            reddit_sentiment_score=0.5,
        )
        result = SentimentScraper._compute_composite(sp)
        # Only reddit: 0.5 * 0.40 / 0.40 = 0.5
        assert result == pytest.approx(0.5)

    def test_news_only(self):
        sp = SentimentDataPoint(
            asset="ETH",
            news_count=5,
            news_sentiment_score=-0.3,
        )
        result = SentimentScraper._compute_composite(sp)
        # Only news: -0.3 * 0.30 / 0.30 = -0.3
        assert result == pytest.approx(-0.3)

    def test_reddit_and_news(self):
        sp = SentimentDataPoint(
            asset="BTC",
            reddit_post_count=10,
            reddit_sentiment_score=0.8,
            news_count=5,
            news_sentiment_score=0.4,
        )
        result = SentimentScraper._compute_composite(sp)
        # (0.8*0.40 + 0.4*0.30) / (0.40 + 0.30) = (0.32 + 0.12) / 0.70 = 0.6286
        assert result == pytest.approx(0.6286, rel=0.01)

    def test_all_sources(self):
        sp = SentimentDataPoint(
            asset="SOL",
            reddit_post_count=10,
            reddit_sentiment_score=0.6,
            news_count=5,
            news_sentiment_score=0.4,
            google_trends_interest=80.0,  # Normalizes to (80-50)/50 = 0.6
            social_sentiment=0.3,         # From LunarCrush
        )
        result = SentimentScraper._compute_composite(sp)
        # (0.6*0.40 + 0.4*0.30 + 0.6*0.15 + 0.3*0.15) / (0.40+0.30+0.15+0.15)
        # = (0.24 + 0.12 + 0.09 + 0.045) / 1.0 = 0.495
        assert result == pytest.approx(0.495, rel=0.01)

    def test_no_data_returns_zero(self):
        sp = SentimentDataPoint(asset="BTC")
        result = SentimentScraper._compute_composite(sp)
        assert result == 0.0

    def test_trends_normalization(self):
        sp = SentimentDataPoint(
            asset="BTC",
            google_trends_interest=100.0,  # (100-50)/50 = 1.0
        )
        result = SentimentScraper._compute_composite(sp)
        # 1.0 * 0.15 / 0.15 = 1.0
        assert result == pytest.approx(1.0)

    def test_trends_below_50_negative(self):
        sp = SentimentDataPoint(
            asset="BTC",
            google_trends_interest=20.0,  # (20-50)/50 = -0.6
        )
        result = SentimentScraper._compute_composite(sp)
        assert result == pytest.approx(-0.6)


# ---------------------------------------------------------------------------
# _fetch_cryptopanic
# ---------------------------------------------------------------------------

class TestFetchCryptopanic:
    async def test_skips_without_api_key(self):
        agent = _make_agent()
        agent._get_json = AsyncMock()

        with patch("agents.ingestion.sentiment.settings") as mock_settings:
            mock_settings.cryptopanic_api_key.get_secret_value.return_value = ""
            await agent._fetch_cryptopanic()

        agent._get_json.assert_not_called()

    async def test_processes_results_with_currency_tags(self):
        agent = _make_agent(with_vader=True)
        if agent._vader is None:
            pytest.skip("VADER not installed")

        resp = {
            "results": [
                {
                    "title": "Bitcoin surges to new highs",
                    "currencies": [{"code": "BTC"}],
                },
                {
                    "title": "Ethereum upgrade complete",
                    "currencies": [{"code": "ETH"}],
                },
            ],
        }
        agent._get_json = AsyncMock(return_value=resp)

        with patch("agents.ingestion.sentiment.settings") as mock_settings:
            mock_settings.cryptopanic_api_key.get_secret_value.return_value = "test-key"
            await agent._fetch_cryptopanic()

        assert agent._asset_sentiment["BTC"].news_count > 0
        assert agent._asset_sentiment["ETH"].news_count > 0


# ---------------------------------------------------------------------------
# _get_json backoff (shared pattern)
# ---------------------------------------------------------------------------

class TestGetJsonBackoff:
    async def test_429_triggers_backoff(self):
        agent = _make_agent()
        mock_resp = AsyncMock()
        mock_resp.status_code = 429
        agent._http = AsyncMock()
        agent._http.get = AsyncMock(return_value=mock_resp)

        result = await agent._get_json("http://example.com", "test")
        assert result is None
        assert agent._backoff["test"] == 2.0

    async def test_success_clears_backoff(self):
        agent = _make_agent()
        agent._backoff["src"] = 4.0
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
# Tracked assets
# ---------------------------------------------------------------------------

class TestTrackedAssets:
    def test_14_assets_tracked(self):
        assert len(_TRACKED_ASSETS) == 14

    def test_major_assets_present(self):
        for asset in ["BTC", "ETH", "SOL", "AVAX", "DOGE"]:
            assert asset in _TRACKED_ASSETS

    def test_asset_sentiment_init(self):
        """Agent initialises SentimentDataPoint for each tracked asset."""
        agent = _make_agent()
        assert set(agent._asset_sentiment.keys()) == set(_TRACKED_ASSETS)
        for asset, sp in agent._asset_sentiment.items():
            assert sp.asset == asset


# ---------------------------------------------------------------------------
# SentimentDataPoint defaults
# ---------------------------------------------------------------------------

class TestSentimentDataPointDefaults:
    def test_all_fields_have_defaults(self):
        dp = SentimentDataPoint()
        assert dp.source == "sentiment_scraper"
        assert dp.asset == ""
        assert dp.reddit_post_count == 0
        assert dp.reddit_sentiment_score == 0.0
        assert dp.news_count == 0
        assert dp.composite_sentiment == 0.0
