"""
SentimentSocial — social-media and news sentiment analysis.

Data sources:
  - CoinGecko community stats (reddit subscribers, twitter followers)
  - NewsAPI headlines for crypto keywords (if API key provided)

Computes a composite sentiment score per asset based on social momentum
and news tone.  Positive sentiment + rising social engagement = bullish.

Runs every 5 minutes.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus
from core.models import AgentSignal, Timeframe

# Simple keyword sentiment scoring.
_BULLISH_WORDS = frozenset({
    "bullish", "moon", "rally", "breakout", "surge", "pump", "buy",
    "outperform", "upgrade", "adoption", "partnership", "launch", "ath",
    "growth", "soar", "gain", "higher", "recovery", "strong", "positive",
})
_BEARISH_WORDS = frozenset({
    "bearish", "crash", "dump", "sell", "plunge", "hack", "exploit",
    "downgrade", "ban", "regulation", "sec", "lawsuit", "fraud", "rug",
    "decline", "drop", "lower", "weak", "negative", "fear", "capitulation",
})

_CG_BASE = "https://api.coingecko.com/api/v3"

# CoinGecko IDs for top assets.
_TICKER_TO_CG: dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "AVAX": "avalanche-2", "DOGE": "dogecoin", "ADA": "cardano",
    "LINK": "chainlink", "ARB": "arbitrum", "OP": "optimism",
    "MATIC": "matic-network", "NEAR": "near", "SUI": "sui",
    "APT": "aptos", "INJ": "injective-protocol",
}


class SentimentSocial(BaseAgent):
    """Social media + news headline sentiment engine."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="sentiment_social",
            agent_type="sentiment",
            bus=bus,
            **kw,
        )
        # Per-asset rolling sentiment scores.
        self._sentiment_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=72),  # ~6 hours at 5-min intervals.
        )
        self._signals_emitted = 0
        self._sub_tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._scan_loop(), name="ss:scan"),
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
        await asyncio.sleep(20)
        while True:
            try:
                await self._scan()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Sentiment scan failed.")
            await asyncio.sleep(settings.sentiment_scan_interval)

    async def _scan(self) -> None:
        # Fetch CoinGecko community stats.
        cg_sentiment = await self._fetch_coingecko_sentiment()
        # Fetch news headlines.
        news_sentiment = await self._fetch_news_sentiment()

        for ticker in _TICKER_TO_CG:
            scores: list[float] = []

            cg_score = cg_sentiment.get(ticker, 0.0)
            if cg_score != 0:
                scores.append(cg_score)

            news_score = news_sentiment.get(ticker, 0.0)
            if news_score != 0:
                scores.append(news_score)

            if not scores:
                continue

            composite = sum(scores) / len(scores)
            self._sentiment_history[ticker].append(composite)

            # Require sufficient history for a signal.
            hist = list(self._sentiment_history[ticker])
            if len(hist) < 3:
                continue

            avg = sum(hist[-6:]) / min(len(hist), 6)
            if abs(avg) < 0.1:
                continue

            direction = max(-1.0, min(1.0, avg))
            conviction = min(abs(avg) * 1.5, 0.7)

            signal = AgentSignal(
                agent_id=self.agent_id,
                asset=ticker,
                direction=direction,
                conviction=conviction,
                timeframe=Timeframe.INTRADAY,
                reasoning="Social sentiment: %.2f (cg=%.2f news=%.2f)" % (
                    avg, cg_score, news_score),
                data_sources=["coingecko:community", "newsapi"],
                metadata={"composite": round(avg, 3),
                          "cg_score": round(cg_score, 3),
                          "news_score": round(news_score, 3)},
                expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            await self.emit_signal(signal)
            self._signals_emitted += 1

    # -- CoinGecko community data -------------------------------------------

    async def _fetch_coingecko_sentiment(self) -> dict[str, float]:
        """Fetch sentiment_votes_up/down percentage from CoinGecko."""
        result: dict[str, float] = {}
        headers = {}
        key = settings.coingecko_api_key.get_secret_value()
        if key:
            headers["x-cg-demo-api-key"] = key

        for ticker, cg_id in _TICKER_TO_CG.items():
            url = "%s/coins/%s" % (_CG_BASE, cg_id)
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        url, headers=headers,
                        params={"localization": "false", "tickers": "false",
                                "market_data": "false", "community_data": "true",
                                "developer_data": "false"},
                    )
                    if resp.status_code == 429:
                        self.log.debug("CoinGecko rate limited.")
                        break
                    resp.raise_for_status()
                    data = resp.json()

                sent = data.get("sentiment_votes_up_percentage", 50)
                # Normalise: 50% = neutral, 0% = -1, 100% = +1.
                score = (sent - 50) / 50 if sent is not None else 0.0
                result[ticker] = score
            except Exception:
                pass
            await asyncio.sleep(0.5)  # Respect rate limits.
        return result

    # -- News headline sentiment --------------------------------------------

    async def _fetch_news_sentiment(self) -> dict[str, float]:
        api_key = settings.newsapi_key.get_secret_value()
        if not api_key:
            return {}

        result: dict[str, float] = {}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q": "crypto OR bitcoin OR ethereum",
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 50,
                        "apiKey": api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            self.log.debug("NewsAPI fetch failed.", exc_info=True)
            return {}

        articles = data.get("articles", [])
        for ticker in _TICKER_TO_CG:
            keyword = ticker.lower()
            score_sum = 0.0
            count = 0
            for article in articles:
                title = (article.get("title") or "").lower()
                desc = (article.get("description") or "").lower()
                text = title + " " + desc
                if keyword not in text and _TICKER_TO_CG[ticker] not in text:
                    continue
                score_sum += self._score_text(text)
                count += 1
            if count > 0:
                result[ticker] = score_sum / count
        return result

    @staticmethod
    def _score_text(text: str) -> float:
        words = set(re.findall(r"\w+", text.lower()))
        bull = len(words & _BULLISH_WORDS)
        bear = len(words & _BEARISH_WORDS)
        total = bull + bear
        if total == 0:
            return 0.0
        return (bull - bear) / total

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({"signals_emitted": self._signals_emitted,
                     "assets_tracked": len(self._sentiment_history)})
        return base
