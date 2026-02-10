"""
Structured JSON logging with optional ``agent_id`` context.

Usage::

    from core.logger import get_logger
    log = get_logger("ta_trend")
    log.info("Signal emitted", extra={"asset": "BTC", "direction": 0.8})

Every log record is emitted as a single JSON line so it can be ingested by
ELK / Loki / Datadog without parsing gymnastics.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from config.settings import settings


class _JSONFormatter(logging.Formatter):
    """Format each log record as a compact JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject agent_id when present.
        agent_id = getattr(record, "agent_id", None)
        if agent_id:
            payload["agent_id"] = agent_id

        # Merge anything the caller passed via ``extra={…}``.
        for key in ("asset", "direction", "conviction", "decision_id",
                     "signal_id", "error", "duration_ms"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

        if record.exc_info and record.exc_info[1] is not None:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


class _AgentAdapter(logging.LoggerAdapter):
    """Injects ``agent_id`` into every record automatically."""

    def process(
        self, msg: str, kwargs: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("agent_id", self.extra.get("agent_id"))
        return msg, kwargs


def _configure_root_logger() -> None:
    """One-time setup: attach the JSON formatter to stderr."""
    root = logging.getLogger()
    if root.handlers:
        return  # Already configured (e.g. during tests).

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JSONFormatter())
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())


# Run once on import.
_configure_root_logger()


def get_logger(agent_id: str | None = None, name: str | None = None) -> logging.LoggerAdapter:
    """Return a structured logger, optionally bound to an *agent_id*.

    Parameters
    ----------
    agent_id:
        Logical agent name (e.g. ``"ta_trend"``).  Attached to every record.
    name:
        Python logger name.  Defaults to ``apex.<agent_id>`` when *agent_id*
        is provided, otherwise ``"apex"``.
    """
    logger_name = name or (f"apex.{agent_id}" if agent_id else "apex")
    base = logging.getLogger(logger_name)
    return _AgentAdapter(base, extra={"agent_id": agent_id or ""})
