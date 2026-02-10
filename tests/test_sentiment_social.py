"""Tests for SentimentSocial — keyword text scoring."""

import pytest

from agents.sentiment.social import SentimentSocial


class TestScoreText:
    def test_all_bullish(self):
        text = "bullish moon rally breakout surge pump"
        score = SentimentSocial._score_text(text)
        assert score > 0

    def test_all_bearish(self):
        text = "bearish crash dump sell plunge hack"
        score = SentimentSocial._score_text(text)
        assert score < 0

    def test_mixed(self):
        text = "bullish crash rally dump"
        score = SentimentSocial._score_text(text)
        # 2 bull, 2 bear → 0
        assert score == pytest.approx(0.0)

    def test_no_keywords(self):
        text = "the quick brown fox jumps over the lazy dog"
        score = SentimentSocial._score_text(text)
        assert score == 0.0

    def test_empty(self):
        score = SentimentSocial._score_text("")
        assert score == 0.0

    def test_exact_ratio(self):
        # 2 bullish + 1 bearish = (2-1)/3 = 0.333
        text = "bullish moon crash"
        score = SentimentSocial._score_text(text)
        assert score == pytest.approx(1 / 3, rel=0.01)
