"""
Decision Journal — CIO's institutional memory system.

Records every investment decision with full context (regime, signal matrix,
agent trust levels, red team challenges). After a trade closes, records the
outcome and optionally generates a lesson learned via Claude.

The CIO queries the journal before making new decisions to avoid repeating
mistakes and to leverage historical patterns.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

from core.models import Direction, MarketRegime

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    import uuid
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DecisionJournalEntry(BaseModel):
    """A single entry in the CIO's decision journal."""

    entry_id: str = Field(default_factory=_new_id)
    timestamp: datetime = Field(default_factory=_utcnow)

    # What was decided.
    asset: str
    direction: Direction
    thesis_summary: str = ""
    conviction: float = 0.5

    # Context at time of decision.
    regime: MarketRegime = MarketRegime.LOW_VOLATILITY
    signal_matrix_snapshot: dict[str, Any] = Field(default_factory=dict)
    top_signals: list[str] = Field(default_factory=list)
    red_team_challenge_summary: str = ""
    ic_debate_quality: str = ""

    # Outcome (filled in later).
    outcome_pnl_pct: Optional[float] = None
    outcome_correct: Optional[bool] = None
    holding_period_hours: Optional[float] = None
    max_adverse_excursion_pct: Optional[float] = None
    exit_reason: Optional[str] = None

    # Lessons (CIO generates these via Claude after trade closes).
    lesson_learned: Optional[str] = None
    would_repeat: Optional[bool] = None


# ---------------------------------------------------------------------------
# DecisionJournal
# ---------------------------------------------------------------------------

class DecisionJournal:
    """CIO's memory — stores and retrieves decision history for context."""

    def __init__(self) -> None:
        self._entries: dict[str, DecisionJournalEntry] = {}  # entry_id -> entry
        # Secondary indices for fast lookup.
        self._by_asset: dict[str, list[str]] = defaultdict(list)
        self._by_regime: dict[str, list[str]] = defaultdict(list)

    # -- recording -----------------------------------------------------------

    def record_decision(self, entry: DecisionJournalEntry) -> None:
        """Store a new decision."""
        self._entries[entry.entry_id] = entry
        self._by_asset[entry.asset].append(entry.entry_id)
        self._by_regime[entry.regime].append(entry.entry_id)

    def record_outcome(
        self,
        entry_id: str,
        pnl_pct: float,
        correct: bool,
        holding_period_hours: float = 0.0,
        max_adverse_pct: float = 0.0,
        exit_reason: str = "",
    ) -> None:
        """Update a decision with its outcome after trade closes."""
        entry = self._entries.get(entry_id)
        if entry is None:
            logger.warning("Journal entry %s not found for outcome recording.", entry_id)
            return
        entry.outcome_pnl_pct = pnl_pct
        entry.outcome_correct = correct
        entry.holding_period_hours = holding_period_hours
        entry.max_adverse_excursion_pct = max_adverse_pct
        entry.exit_reason = exit_reason

    def set_lesson(self, entry_id: str, lesson: str, would_repeat: bool = True) -> None:
        """Set the lesson learned for an entry."""
        entry = self._entries.get(entry_id)
        if entry:
            entry.lesson_learned = lesson
            entry.would_repeat = would_repeat

    # -- queries -------------------------------------------------------------

    def get_entry(self, entry_id: str) -> DecisionJournalEntry | None:
        """Get a single journal entry."""
        return self._entries.get(entry_id)

    def get_similar_decisions(
        self,
        asset: str,
        direction: Direction,
        regime: MarketRegime,
        limit: int = 10,
    ) -> list[DecisionJournalEntry]:
        """Find past decisions in similar conditions."""
        candidates: list[DecisionJournalEntry] = []

        # Prefer same asset + same regime.
        asset_ids = set(self._by_asset.get(asset, []))
        regime_ids = set(self._by_regime.get(regime, []))
        both = asset_ids & regime_ids
        for eid in both:
            entry = self._entries[eid]
            if entry.direction == direction:
                candidates.append(entry)

        # If not enough, broaden to same asset any regime.
        if len(candidates) < limit:
            for eid in asset_ids - both:
                entry = self._entries[eid]
                if entry.direction == direction:
                    candidates.append(entry)

        # Sort by recency.
        candidates.sort(key=lambda e: e.timestamp, reverse=True)
        return candidates[:limit]

    def get_regime_performance(self, regime: MarketRegime) -> dict[str, Any]:
        """How has the CIO performed in this regime historically?"""
        ids = self._by_regime.get(regime, [])
        entries = [self._entries[eid] for eid in ids if eid in self._entries]
        closed = [e for e in entries if e.outcome_correct is not None]

        if not closed:
            return {"trades": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0}

        wins = sum(1 for e in closed if e.outcome_correct)
        avg_pnl = sum(e.outcome_pnl_pct or 0.0 for e in closed) / len(closed)

        # Best and worst trade.
        best = max(closed, key=lambda e: e.outcome_pnl_pct or 0.0)
        worst = min(closed, key=lambda e: e.outcome_pnl_pct or 0.0)

        return {
            "trades": len(closed),
            "win_rate": wins / len(closed),
            "avg_pnl_pct": round(avg_pnl, 4),
            "best_trade": f"{best.direction} {best.asset} {best.outcome_pnl_pct:+.2f}%"
            if best.outcome_pnl_pct is not None else "",
            "worst_trade": f"{worst.direction} {worst.asset} {worst.outcome_pnl_pct:+.2f}%"
            if worst.outcome_pnl_pct is not None else "",
        }

    def get_agent_reliability(
        self,
        agent_id: str,
        regime: MarketRegime | None = None,
    ) -> dict[str, Any]:
        """How reliable has a specific agent been as a top signal source?"""
        relevant: list[DecisionJournalEntry] = []
        for entry in self._entries.values():
            if agent_id in entry.top_signals:
                if regime is None or entry.regime == regime:
                    relevant.append(entry)

        closed = [e for e in relevant if e.outcome_correct is not None]
        if not closed:
            return {"trades": 0, "win_rate": 0.0}

        wins = sum(1 for e in closed if e.outcome_correct)
        return {
            "trades": len(closed),
            "win_rate": round(wins / len(closed), 4),
        }

    def get_recent_lessons(self, limit: int = 5) -> list[str]:
        """Get the most recent lessons for CIO context injection."""
        entries = [
            e for e in self._entries.values()
            if e.lesson_learned
        ]
        entries.sort(key=lambda e: e.timestamp, reverse=True)
        return [e.lesson_learned for e in entries[:limit]]

    def get_recent_entries(self, limit: int = 20) -> list[DecisionJournalEntry]:
        """Get the most recent journal entries."""
        entries = sorted(self._entries.values(), key=lambda e: e.timestamp, reverse=True)
        return entries[:limit]

    # -- CIO context builder -------------------------------------------------

    def build_cio_context(
        self,
        current_regime: MarketRegime,
        target_asset: str = "",
        agent_profiles: dict[str, Any] | None = None,
    ) -> str:
        """Build a context block for CIO prompt injection.

        Returns a multi-line string summarising decision history, regime
        performance, agent reliability, and recent lessons.
        """
        lines: list[str] = []
        lines.append("DECISION HISTORY CONTEXT:")

        # Overall stats.
        all_closed = [e for e in self._entries.values() if e.outcome_correct is not None]
        total = len(all_closed)
        if total == 0:
            lines.append("No closed trades yet — operating without historical context.")
            return "\n".join(lines)

        wins = sum(1 for e in all_closed if e.outcome_correct)
        avg_pnl = sum(e.outcome_pnl_pct or 0.0 for e in all_closed) / total

        # Asset class breakdown.
        ac_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "wins": 0})
        for e in all_closed:
            # Infer asset class from asset name.
            ac = self._infer_asset_class(e.asset)
            ac_stats[ac]["total"] += 1
            if e.outcome_correct:
                ac_stats[ac]["wins"] += 1

        lines.append(
            f"Last 30 days: {total} closed trades, {wins}/{total} wins "
            f"({wins/total*100:.0f}% WR), avg PnL: {avg_pnl:+.2f}%."
        )

        # Asset class line.
        ac_parts = []
        for ac, stats in sorted(ac_stats.items()):
            wr = stats["wins"] / stats["total"] * 100 if stats["total"] > 0 else 0
            ac_parts.append(f"{ac.title()} {stats['total']} trades ({wr:.0f}% WR)")
        if ac_parts:
            lines.append(f"Asset class breakdown: {', '.join(ac_parts)}.")

        # Current regime performance.
        regime_perf = self.get_regime_performance(current_regime)
        if regime_perf["trades"] > 0:
            lines.append(
                f"\nIn current regime ({current_regime}): "
                f"{regime_perf['trades']} trades, "
                f"{regime_perf['win_rate']*100:.0f}% WR, "
                f"avg PnL: {regime_perf['avg_pnl_pct']:+.2f}%."
            )
            if regime_perf.get("best_trade"):
                lines.append(f"  Best: {regime_perf['best_trade']}")
            if regime_perf.get("worst_trade"):
                lines.append(f"  Worst: {regime_perf['worst_trade']}")

        # Target asset history.
        if target_asset:
            asset_entries = [
                e for e in all_closed if e.asset == target_asset
            ]
            if asset_entries:
                asset_entries.sort(key=lambda e: e.timestamp, reverse=True)
                recent = asset_entries[:5]
                streak = "".join("W" if e.outcome_correct else "L" for e in recent)
                lines.append(f"\nFor {target_asset}: last {len(recent)} trades: {streak}")

        # Recent lessons.
        lessons = self.get_recent_lessons(limit=4)
        if lessons:
            lines.append("\nRecent lessons:")
            for lesson in lessons:
                lines.append(f"  - {lesson}")

        return "\n".join(lines)

    @staticmethod
    def _infer_asset_class(asset: str) -> str:
        """Simple heuristic to infer asset class from ticker."""
        crypto = {"BTC", "ETH", "SOL", "AVAX", "DOGE", "LINK", "ADA", "DOT", "MATIC", "ARB"}
        equities = {"SPY", "QQQ", "AAPL", "MSFT", "TSLA", "NVDA", "AMZN"}
        commodities = {"GOLD", "SILVER", "OIL", "GAS", "COPPER"}
        fx = {"EUR", "GBP", "JPY", "AUD", "CAD", "CHF"}

        upper = asset.upper().replace("-PERP", "")
        if upper in crypto:
            return "crypto"
        if upper in equities:
            return "equity"
        if upper in commodities:
            return "commodity"
        if upper in fx:
            return "fx"
        return "crypto"  # Default assumption.
