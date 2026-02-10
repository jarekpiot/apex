"""
Redis Streams message bus for inter-agent communication.

Each logical channel (signals, decisions, risk_checks, red_team) maps to a
separate Redis Stream.  Consumer groups allow multiple workers to process
the same stream in parallel without duplicating work.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from pydantic import BaseModel

from config.settings import settings

logger = logging.getLogger(__name__)

# Stream names — single source of truth.
STREAM_SIGNALS = "apex:signals"
STREAM_DECISIONS = "apex:decisions"
STREAM_RISK_CHECKS = "apex:risk_checks"
STREAM_RED_TEAM = "apex:red_team"

# Sprint 2 — market data & platform streams.
STREAM_MARKET_PRICES = "market:prices"
STREAM_PLATFORM_STATE = "platform:state"
STREAM_NEW_LISTING = "platform:new_listing"
STREAM_ACCOUNT_HEALTH = "platform:account_health"
STREAM_EXECUTION_ADVISORY = "platform:execution_advisory"
STREAM_DECISIONS_PENDING = "decisions:pending"
STREAM_DECISIONS_APPROVED = "decisions:approved"
STREAM_DECISIONS_REJECTED = "decisions:rejected"
STREAM_TRADES_EXECUTED = "trades:executed"
STREAM_PORTFOLIO_STATE = "portfolio:state"

# Sprint 3 — risk layer streams.
STREAM_RISK_ANOMALY = "risk:anomaly"

# Sprint 4 — analysis agent streams.
STREAM_SIGNALS_TECHNICAL = "signals:technical"
STREAM_SIGNALS_FUNDING_ARB = "signals:funding_arb"

# Data ingestion streams.
STREAM_DATA_ONCHAIN = "data:onchain"
STREAM_DATA_MACRO = "data:macro"
STREAM_DATA_SENTIMENT = "data:sentiment"

# How long to block on XREADGROUP when no new messages arrive (ms).
_DEFAULT_BLOCK_MS = 5_000
_DEFAULT_BATCH = 10


def _serialize(model: BaseModel) -> dict[str, str]:
    """Flatten a Pydantic model into a Redis-friendly string dict."""
    return {"payload": model.model_dump_json()}


def _deserialize(raw: dict[bytes, bytes]) -> dict[str, Any]:
    """Parse a single Redis stream entry back into a Python dict."""
    payload = raw.get(b"payload", b"{}")
    return json.loads(payload)


class MessageBus:
    """Thin async wrapper around Redis Streams."""

    def __init__(self, redis_url: str | None = None) -> None:
        self._url = redis_url or settings.redis_url
        self._redis: aioredis.Redis | None = None

    # -- lifecycle -----------------------------------------------------------

    async def connect(self) -> None:
        """Open the Redis connection pool."""
        self._redis = aioredis.from_url(
            self._url,
            decode_responses=False,  # We handle decoding ourselves.
        )
        await self._redis.ping()
        logger.info("MessageBus connected to Redis at %s", self._url)

    async def close(self) -> None:
        """Drain and close the connection pool."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("MessageBus connection closed.")

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("MessageBus is not connected. Call connect() first.")
        return self._redis

    # -- publishing ----------------------------------------------------------

    async def _publish(self, stream: str, model: BaseModel) -> str:
        """Publish a Pydantic model to *stream*. Returns the message id."""
        msg_id: bytes = await self.redis.xadd(stream, _serialize(model))
        decoded = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        logger.debug("Published to %s: %s", stream, decoded)
        return decoded

    async def publish_signal(self, signal: BaseModel) -> str:
        return await self._publish(STREAM_SIGNALS, signal)

    async def publish_decision(self, decision: BaseModel) -> str:
        return await self._publish(STREAM_DECISIONS, decision)

    async def publish_risk_check(self, check: BaseModel) -> str:
        return await self._publish(STREAM_RISK_CHECKS, check)

    async def publish_red_team(self, challenge: BaseModel) -> str:
        return await self._publish(STREAM_RED_TEAM, challenge)

    # -- consumer groups -----------------------------------------------------

    async def ensure_group(self, stream: str, group: str) -> None:
        """Idempotently create a consumer group starting from the latest id."""
        try:
            await self.redis.xgroup_create(
                stream, group, id="0", mkstream=True,
            )
            logger.info("Created consumer group %s on %s", group, stream)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                pass  # Group already exists — that's fine.
            else:
                raise

    # -- subscribing ---------------------------------------------------------

    async def _subscribe(
        self,
        stream: str,
        group: str,
        consumer: str,
        batch: int = _DEFAULT_BATCH,
        block_ms: int = _DEFAULT_BLOCK_MS,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Yield ``(message_id, payload_dict)`` from a consumer group.

        This is an infinite async generator — call ``async for`` on it.
        Messages are automatically ACK'd after yielding.
        """
        await self.ensure_group(stream, group)
        while True:
            entries = await self.redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=batch,
                block=block_ms,
            )
            if not entries:
                continue
            for _stream_key, messages in entries:
                for msg_id, raw in messages:
                    decoded_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    try:
                        payload = _deserialize(raw)
                        yield decoded_id, payload
                        await self.redis.xack(stream, group, msg_id)
                    except Exception:
                        logger.exception(
                            "Failed to process message %s from %s",
                            decoded_id,
                            stream,
                        )

    def subscribe_signals(
        self, group: str, consumer: str, **kw: Any,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        return self._subscribe(STREAM_SIGNALS, group, consumer, **kw)

    def subscribe_decisions(
        self, group: str, consumer: str, **kw: Any,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        return self._subscribe(STREAM_DECISIONS, group, consumer, **kw)

    def subscribe_risk_checks(
        self, group: str, consumer: str, **kw: Any,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        return self._subscribe(STREAM_RISK_CHECKS, group, consumer, **kw)

    def subscribe_red_team(
        self, group: str, consumer: str, **kw: Any,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        return self._subscribe(STREAM_RED_TEAM, group, consumer, **kw)

    # -- generic publish / subscribe -----------------------------------------

    async def publish_to(self, stream: str, model: BaseModel) -> str:
        """Publish a Pydantic model to an arbitrary stream."""
        return await self._publish(stream, model)

    def subscribe_to(
        self, stream: str, group: str, consumer: str, **kw: Any,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Subscribe to an arbitrary stream via consumer group."""
        return self._subscribe(stream, group, consumer, **kw)

    # -- utilities -----------------------------------------------------------

    async def stream_length(self, stream: str) -> int:
        return await self.redis.xlen(stream)

    async def health_check(self) -> bool:
        """Return True if Redis is reachable."""
        try:
            await self.redis.ping()
            return True
        except Exception:
            return False
