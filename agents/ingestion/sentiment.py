"""
SentimentScraper — centralised sentiment data ingestion agent.

4 concurrent sub-tasks:
  a) Reddit — asyncpraw on r/cryptocurrency, r/bitcoin, r/ethereum   every 5 min
  b) CryptoPanic — news headlines with VADER scoring                  every 5 min
  c) Google Trends — pytrends interest over time                      every 30 min
  d) LunarCrush — social metrics (galaxy score, social volume)        every 10 min

Publishes per-asset SentimentDataPoint to ``data:sentiment``, persists to ``sentiment_scores``.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from agents.base_agent import BaseAgent
from config.settings import settings
from core.database import SentimentScoreRow, async_session_factory
from core.message_bus import MessageBus, STREAM_DATA_SENTIMENT
from core.models import SentimentDataPoint


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TRACKED_ASSETS = [
    "BTC", "ETH", "SOL", "AVAX", "DOGE", "ADA",
    "LINK", "ARB", "OP", "SUI", "APT", "INJ", "NEAR", "MATIC",
]

# Keyword → asset mapping for mention detection.
_ASSET_KEYWORDS: dict[str, str] = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "avalanche": "AVAX", "avax": "AVAX",
    "dogecoin": "DOGE", "doge": "DOGE",
    "cardano": "ADA", "ada": "ADA",
    "chainlink": "LINK", "link": "LINK",
    "arbitrum": "ARB", "arb": "ARB",
    "optimism": "OP",
    "sui": "SUI",
    "aptos": "APT", "apt": "APT",
    "injective": "INJ", "inj": "INJ",
    "near": "NEAR",
    "polygon": "MATIC", "matic": "MATIC",
}

_SUBREDDITS = ["cryptocurrency", "bitcoin", "ethereum"]

_TRENDS_KEYWORDS = ["buy bitcoin", "bitcoin", "ethereum", "crypto", "solana"]


class SentimentScraper(BaseAgent):
    """Centralised sentiment data ingestion."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="sentiment_scraper",
            agent_type="ingestion",
            bus=bus,
            **kw,
        )
        self._http: httpx.AsyncClient | None = None
        self._sub_tasks: list[asyncio.Task[None]] = []

        # Per-asset accumulated sentiment.
        self._asset_sentiment: dict[str, SentimentDataPoint] = {
            a: SentimentDataPoint(asset=a) for a in _TRACKED_ASSETS
        }
        self._reddit: Any = None  # asyncpraw.Reddit instance
        self._vader: Any = None   # SentimentIntensityAnalyzer instance
        self._backoff: dict[str, float] = {}

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._http = httpx.AsyncClient(timeout=30.0)

        # Initialise VADER.
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self._vader = SentimentIntensityAnalyzer()
        except ImportError:
            self.log.warning("vaderSentiment not installed, NLP scoring disabled.")

        # Initialise Reddit (asyncpraw).
        client_id = settings.reddit_client_id.get_secret_value()
        client_secret = settings.reddit_client_secret.get_secret_value()
        if client_id and client_secret:
            try:
                import asyncpraw
                self._reddit = asyncpraw.Reddit(
                    client_id=client_id,
                    client_secret=client_secret,
                    user_agent="APEX-SentimentScraper/1.0",
                )
            except ImportError:
                self.log.warning("asyncpraw not installed, Reddit scraping disabled.")
        else:
            self.log.info("Reddit credentials not configured, skipping Reddit loop.")

        await super().start()
        self._sub_tasks = [
            asyncio.create_task(self._reddit_loop(), name="ss:reddit"),
            asyncio.create_task(self._cryptopanic_loop(), name="ss:cryptopanic"),
            asyncio.create_task(self._trends_loop(), name="ss:trends"),
            asyncio.create_task(self._lunarcrush_loop(), name="ss:lunarcrush"),
        ]

    async def stop(self) -> None:
        for t in self._sub_tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._sub_tasks.clear()
        if self._reddit:
            try:
                await self._reddit.close()
            except Exception:
                pass
            self._reddit = None
        if self._http:
            await self._http.aclose()
            self._http = None
        await super().stop()

    async def process(self) -> None:
        """Main loop publishes per-asset sentiment snapshots."""
        await asyncio.sleep(30)
        now = datetime.now(timezone.utc)
        for asset, sp in self._asset_sentiment.items():
            sp.timestamp = now
            sp.composite_sentiment = self._compute_composite(sp)
            await self.bus.publish_to(STREAM_DATA_SENTIMENT, sp)

    # -- helpers -------------------------------------------------------------

    def _detect_assets(self, text: str) -> list[str]:
        """Find which tracked assets are mentioned in *text*."""
        lower = text.lower()
        found: set[str] = set()
        for keyword, asset in _ASSET_KEYWORDS.items():
            if re.search(rf"\b{re.escape(keyword)}\b", lower):
                found.add(asset)
        return list(found)

    def _score_text(self, text: str) -> float:
        """VADER compound score (-1 to +1), or 0 if VADER unavailable."""
        if not self._vader:
            return 0.0
        return self._vader.polarity_scores(text)["compound"]

    @staticmethod
    def _compute_composite(sp: SentimentDataPoint) -> float:
        """Weighted composite: reddit 40%, news 30%, trends 15%, lunarcrush 15%."""
        total = 0.0
        weight_sum = 0.0

        if sp.reddit_post_count > 0:
            total += sp.reddit_sentiment_score * 0.40
            weight_sum += 0.40
        if sp.news_count > 0:
            total += sp.news_sentiment_score * 0.30
            weight_sum += 0.30
        if sp.google_trends_interest > 0:
            # Normalize 0-100 to -1..+1 centered at 50.
            norm = (sp.google_trends_interest - 50.0) / 50.0
            total += norm * 0.15
            weight_sum += 0.15
        if sp.social_sentiment != 0:
            total += sp.social_sentiment * 0.15
            weight_sum += 0.15

        return total / weight_sum if weight_sum > 0 else 0.0

    async def _get_json(self, url: str, source: str) -> Any:
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

    async def _persist_scores(self, rows: list[SentimentScoreRow]) -> None:
        if not rows:
            return
        async with async_session_factory() as session:
            session.add_all(rows)
            await session.commit()

    # -- (a) Reddit ----------------------------------------------------------

    async def _reddit_loop(self) -> None:
        while True:
            try:
                if self._reddit:
                    await self._fetch_reddit()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Reddit fetch failed.")
            await asyncio.sleep(settings.sentiment_reddit_interval)

    async def _fetch_reddit(self) -> None:
        now = datetime.now(timezone.utc)
        # Reset reddit counts before accumulating.
        for sp in self._asset_sentiment.values():
            sp.reddit_post_count = 0
            sp.reddit_sentiment_score = 0.0
            sp.reddit_bullish_pct = 0.0

        asset_scores: dict[str, list[float]] = {a: [] for a in _TRACKED_ASSETS}
        db_rows: list[SentimentScoreRow] = []

        for sub_name in _SUBREDDITS:
            try:
                subreddit = await self._reddit.subreddit(sub_name)
                count = 0
                async for post in subreddit.hot(limit=25):
                    text = f"{post.title} {post.selftext or ''}"
                    score = self._score_text(text)
                    assets = self._detect_assets(text)
                    for asset in assets:
                        if asset in asset_scores:
                            asset_scores[asset].append(score)
                    count += 1
                self.log.debug("Reddit r/%s: processed %d posts", sub_name, count)
            except Exception:
                self.log.exception("Error fetching r/%s", sub_name)
            await asyncio.sleep(2)  # Respect rate limits between subreddits.

        for asset, scores in asset_scores.items():
            if not scores:
                continue
            sp = self._asset_sentiment[asset]
            sp.reddit_post_count = len(scores)
            sp.reddit_sentiment_score = sum(scores) / len(scores)
            sp.reddit_bullish_pct = sum(1 for s in scores if s > 0.05) / len(scores)

            db_rows.append(SentimentScoreRow(
                ts=now, source="reddit", asset=asset,
                score=sp.reddit_sentiment_score,
                post_count=sp.reddit_post_count,
            ))

        await self._persist_scores(db_rows)
        total_mentions = sum(len(s) for s in asset_scores.values())
        self.log.info("Reddit updated: %d total mentions across %d subs", total_mentions, len(_SUBREDDITS))

    # -- (b) CryptoPanic ----------------------------------------------------

    async def _cryptopanic_loop(self) -> None:
        while True:
            try:
                await self._fetch_cryptopanic()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("CryptoPanic fetch failed.")
            await asyncio.sleep(settings.sentiment_cryptopanic_interval)

    async def _fetch_cryptopanic(self) -> None:
        api_key = settings.cryptopanic_api_key.get_secret_value()
        if not api_key:
            self.log.debug("CryptoPanic API key not configured, skipping.")
            return

        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={api_key}&kind=news&public=true"
        data = await self._get_json(url, "cryptopanic")
        if not data:
            return

        now = datetime.now(timezone.utc)
        results = data.get("results", [])
        asset_scores: dict[str, list[float]] = {a: [] for a in _TRACKED_ASSETS}
        db_rows: list[SentimentScoreRow] = []

        for item in results:
            title = item.get("title", "")
            score = self._score_text(title)
            assets = self._detect_assets(title)

            # Also check CryptoPanic's own currency tags.
            currencies = item.get("currencies", [])
            for c in currencies:
                code = (c.get("code") or "").upper()
                if code in self._asset_sentiment:
                    assets.append(code)

            for asset in set(assets):
                if asset in asset_scores:
                    asset_scores[asset].append(score)

        for asset, scores in asset_scores.items():
            if not scores:
                continue
            sp = self._asset_sentiment[asset]
            sp.news_count = len(scores)
            sp.news_sentiment_score = sum(scores) / len(scores)
            sp.news_positive_pct = sum(1 for s in scores if s > 0.05) / len(scores)
            sp.news_negative_pct = sum(1 for s in scores if s < -0.05) / len(scores)

            db_rows.append(SentimentScoreRow(
                ts=now, source="cryptopanic", asset=asset,
                score=sp.news_sentiment_score,
                post_count=sp.news_count,
            ))

        await self._persist_scores(db_rows)
        self.log.info("CryptoPanic updated: %d articles processed", len(results))

    # -- (c) Google Trends ---------------------------------------------------

    async def _trends_loop(self) -> None:
        while True:
            try:
                await self._fetch_trends()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("Google Trends fetch failed.")
            await asyncio.sleep(settings.sentiment_trends_interval)

    async def _fetch_trends(self) -> None:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, self._sync_fetch_trends)
        if not result:
            return

        now = datetime.now(timezone.utc)
        db_rows: list[SentimentScoreRow] = []

        for keyword, interest in result.items():
            assets = self._detect_assets(keyword)
            for asset in assets:
                if asset in self._asset_sentiment:
                    sp = self._asset_sentiment[asset]
                    sp.google_trends_interest = float(interest)
                    db_rows.append(SentimentScoreRow(
                        ts=now, source="google_trends", asset=asset,
                        score=float(interest) / 100.0,  # Normalize 0-100 → 0-1
                        metadata_={"keyword": keyword},
                    ))

        await self._persist_scores(db_rows)
        self.log.info("Google Trends updated: %d keywords", len(result))

    @staticmethod
    def _sync_fetch_trends() -> dict[str, float]:
        """Synchronous pytrends call — run via executor."""
        try:
            from pytrends.request import TrendReq
        except ImportError:
            return {}

        try:
            pytrends = TrendReq(hl="en-US", tz=360)
            pytrends.build_payload(_TRENDS_KEYWORDS, timeframe="now 7-d")
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                return {}
            # Get latest values.
            latest = df.iloc[-1]
            return {kw: float(latest.get(kw, 0)) for kw in _TRENDS_KEYWORDS if kw in df.columns}
        except Exception:
            return {}

    # -- (d) LunarCrush ------------------------------------------------------

    async def _lunarcrush_loop(self) -> None:
        while True:
            try:
                await self._fetch_lunarcrush()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("LunarCrush fetch failed.")
            await asyncio.sleep(settings.sentiment_lunarcrush_interval)

    async def _fetch_lunarcrush(self) -> None:
        api_key = settings.lunarcrush_api_key.get_secret_value()
        if not api_key:
            self.log.debug("LunarCrush API key not configured, skipping.")
            return

        now = datetime.now(timezone.utc)
        db_rows: list[SentimentScoreRow] = []

        for asset in _TRACKED_ASSETS:
            url = f"https://lunarcrush.com/api4/public/coins/{asset.lower()}/v1"
            headers = {"Authorization": f"Bearer {api_key}"}
            try:
                assert self._http is not None
                resp = await self._http.get(url, headers=headers)
                if resp.status_code == 429 or resp.status_code >= 500:
                    delay = self._backoff.get("lunarcrush", 1.0)
                    self._backoff["lunarcrush"] = min(delay * 2, 60.0)
                    await asyncio.sleep(delay)
                    continue
                if resp.status_code != 200:
                    continue
                self._backoff.pop("lunarcrush", None)
                data = resp.json()
            except Exception:
                continue

            coin_data = data.get("data", data)
            if not coin_data:
                continue

            sp = self._asset_sentiment[asset]
            sp.galaxy_score = float(coin_data.get("galaxy_score", 0) or 0)
            sp.social_volume = float(coin_data.get("social_volume", 0) or 0)
            # Normalize galaxy_score (0-100) to -1..+1.
            sp.social_sentiment = (sp.galaxy_score - 50.0) / 50.0 if sp.galaxy_score else 0.0

            db_rows.append(SentimentScoreRow(
                ts=now, source="lunarcrush", asset=asset,
                score=sp.social_sentiment,
                metadata_={"galaxy_score": sp.galaxy_score, "social_volume": sp.social_volume},
            ))

        await self._persist_scores(db_rows)
        self.log.info("LunarCrush updated: %d assets", len(_TRACKED_ASSETS))
