"""
Abstract base class for every APEX agent.

Subclass ``BaseAgent``, implement :meth:`process`, and you get:

* Auto-connection to the message bus and database.
* A structured logger bound to your ``agent_id``.
* Graceful start / stop lifecycle with health-check support.
* A convenience :meth:`emit_signal` that publishes + persists in one call.
"""

from __future__ import annotations

import abc
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from core.database import AgentSignalRow, async_session_factory
from core.logger import get_logger
from core.message_bus import MessageBus
from core.models import AgentSignal


class BaseAgent(abc.ABC):
    """Skeleton that every APEX agent inherits from."""

    def __init__(
        self,
        agent_id: str,
        agent_type: str,
        bus: MessageBus,
        **kwargs: Any,
    ) -> None:
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.bus = bus
        self.log: logging.LoggerAdapter = get_logger(agent_id)
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._last_heartbeat: datetime | None = None

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Boot the agent's processing loop."""
        if self._running:
            self.log.warning("Agent already running.")
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name=f"agent:{self.agent_id}")
        self.log.info("Agent started.")

    async def stop(self) -> None:
        """Signal the agent to shut down gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.log.info("Agent stopped.")

    async def _run_loop(self) -> None:
        """Internal loop that calls :meth:`process` repeatedly."""
        try:
            while self._running:
                self._last_heartbeat = datetime.now(timezone.utc)
                await self.process()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("Unhandled error in agent loop — exiting.")
            self._running = False

    # -- abstract interface --------------------------------------------------

    @abc.abstractmethod
    async def process(self) -> None:
        """Execute one iteration of the agent's core logic.

        Implementations should:
        1. Fetch whatever data they need (market data, signals, etc.).
        2. Run analysis.
        3. Call :meth:`emit_signal` when they have a new opinion.
        4. Sleep or await new messages before the next iteration.
        """
        ...

    # -- helpers -------------------------------------------------------------

    async def emit_signal(self, signal: AgentSignal) -> str:
        """Publish a signal to the bus *and* persist it to the database.

        Returns the Redis stream message id.
        """
        # Persist to Postgres.
        async with async_session_factory() as session:
            row = AgentSignalRow(
                signal_id=signal.signal_id,
                agent_id=signal.agent_id,
                ts=signal.timestamp,
                asset=signal.asset,
                direction=signal.direction,
                conviction=signal.conviction,
                timeframe=signal.timeframe.value,
                reasoning=signal.reasoning,
                data_sources=signal.data_sources,
                metadata_=signal.metadata,
                expires_at=signal.expires_at,
            )
            session.add(row)
            await session.commit()

        # Publish to Redis Streams.
        msg_id = await self.bus.publish_signal(signal)
        self.log.info(
            "Signal emitted",
            extra={
                "signal_id": signal.signal_id,
                "asset": signal.asset,
                "direction": signal.direction,
                "conviction": signal.conviction,
            },
        )
        return msg_id

    # -- health --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Return a JSON-serialisable health snapshot."""
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "running": self._running,
            "last_heartbeat": (
                self._last_heartbeat.isoformat() if self._last_heartbeat else None
            ),
        }
