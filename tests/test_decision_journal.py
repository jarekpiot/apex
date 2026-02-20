"""
Tests for core.decision_journal — CIO's institutional memory system.

Covers: recording, outcome updates, similarity queries, regime performance,
agent reliability, CIO context builder, lessons, and asset-class inference.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.decision_journal import DecisionJournal, DecisionJournalEntry
from core.models import Direction, MarketRegime


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_entry(
    asset: str = "BTC",
    direction: Direction = Direction.LONG,
    regime: MarketRegime = MarketRegime.TRENDING_UP,
    conviction: float = 0.7,
    **kw,
) -> DecisionJournalEntry:
    """Create a DecisionJournalEntry with sensible defaults."""
    return DecisionJournalEntry(
        asset=asset,
        direction=direction,
        regime=regime,
        conviction=conviction,
        **kw,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDecisionJournal:
    """Unit tests for DecisionJournal."""

    # 1. record_decision ---------------------------------------------------

    def test_record_decision(self):
        """Journal stores an entry and it is retrievable by entry_id."""
        journal = DecisionJournal()
        entry = _make_entry(thesis_summary="BTC breakout thesis")
        journal.record_decision(entry)

        retrieved = journal.get_entry(entry.entry_id)
        assert retrieved is not None
        assert retrieved.entry_id == entry.entry_id
        assert retrieved.asset == "BTC"
        assert retrieved.direction == Direction.LONG
        assert retrieved.regime == MarketRegime.TRENDING_UP
        assert retrieved.conviction == 0.7
        assert retrieved.thesis_summary == "BTC breakout thesis"

    # 2. record_outcome ----------------------------------------------------

    def test_record_outcome(self):
        """Outcome fields are updated on an existing entry."""
        journal = DecisionJournal()
        entry = _make_entry()
        journal.record_decision(entry)

        journal.record_outcome(
            entry_id=entry.entry_id,
            pnl_pct=2.5,
            correct=True,
            holding_period_hours=48.0,
            max_adverse_pct=1.2,
            exit_reason="take_profit",
        )

        updated = journal.get_entry(entry.entry_id)
        assert updated is not None
        assert updated.outcome_pnl_pct == 2.5
        assert updated.outcome_correct is True
        assert updated.holding_period_hours == 48.0
        assert updated.max_adverse_excursion_pct == 1.2
        assert updated.exit_reason == "take_profit"

    def test_record_outcome_missing_entry(self):
        """Recording an outcome for a non-existent entry does not raise."""
        journal = DecisionJournal()
        # Should log a warning but not raise.
        journal.record_outcome(
            entry_id="nonexistent-id",
            pnl_pct=1.0,
            correct=True,
        )

    # 3. similar_decisions_query -------------------------------------------

    def test_similar_decisions_query(self):
        """Finds decisions matching asset + direction + regime."""
        journal = DecisionJournal()

        # Matching entry: BTC LONG TRENDING_UP.
        match = _make_entry(asset="BTC", direction=Direction.LONG, regime=MarketRegime.TRENDING_UP)
        journal.record_decision(match)

        # Non-matching: different asset.
        journal.record_decision(
            _make_entry(asset="ETH", direction=Direction.LONG, regime=MarketRegime.TRENDING_UP)
        )

        # Non-matching: different direction.
        journal.record_decision(
            _make_entry(asset="BTC", direction=Direction.SHORT, regime=MarketRegime.TRENDING_UP)
        )

        # Non-matching: different regime (still BTC LONG, but different regime).
        journal.record_decision(
            _make_entry(asset="BTC", direction=Direction.LONG, regime=MarketRegime.HIGH_VOLATILITY)
        )

        results = journal.get_similar_decisions(
            asset="BTC",
            direction=Direction.LONG,
            regime=MarketRegime.TRENDING_UP,
        )

        # The exact match should appear first; the HIGH_VOLATILITY entry broadens.
        entry_ids = [e.entry_id for e in results]
        assert match.entry_id in entry_ids
        # The ETH entry and the SHORT entry should not appear.
        assert all(e.asset == "BTC" and e.direction == Direction.LONG for e in results)

    # 4. similar_decisions_broadens ----------------------------------------

    def test_similar_decisions_broadens(self):
        """When not enough matches for the specific regime, broadens to any regime."""
        journal = DecisionJournal()

        # Only entry is BTC LONG but in a different regime.
        other_regime = _make_entry(
            asset="BTC",
            direction=Direction.LONG,
            regime=MarketRegime.MEAN_REVERTING,
        )
        journal.record_decision(other_regime)

        results = journal.get_similar_decisions(
            asset="BTC",
            direction=Direction.LONG,
            regime=MarketRegime.TRENDING_UP,
            limit=10,
        )

        # The entry from MEAN_REVERTING should still appear via broadened search.
        assert len(results) == 1
        assert results[0].entry_id == other_regime.entry_id

    # 5. regime_performance ------------------------------------------------

    def test_regime_performance(self):
        """Stats computed correctly for a given regime."""
        journal = DecisionJournal()

        # Two winning trades, one losing trade — all in TRENDING_UP.
        e1 = _make_entry(asset="BTC", regime=MarketRegime.TRENDING_UP)
        e2 = _make_entry(asset="ETH", regime=MarketRegime.TRENDING_UP)
        e3 = _make_entry(asset="SOL", regime=MarketRegime.TRENDING_UP)

        for e in (e1, e2, e3):
            journal.record_decision(e)

        journal.record_outcome(e1.entry_id, pnl_pct=5.0, correct=True)
        journal.record_outcome(e2.entry_id, pnl_pct=3.0, correct=True)
        journal.record_outcome(e3.entry_id, pnl_pct=-2.0, correct=False)

        perf = journal.get_regime_performance(MarketRegime.TRENDING_UP)

        assert perf["trades"] == 3
        assert perf["win_rate"] == pytest.approx(2 / 3, abs=1e-4)
        assert perf["avg_pnl_pct"] == pytest.approx(2.0, abs=1e-4)
        assert "BTC" in perf["best_trade"]
        assert "+5.00%" in perf["best_trade"]
        assert "SOL" in perf["worst_trade"]
        assert "-2.00%" in perf["worst_trade"]

    def test_regime_performance_no_closed(self):
        """Regime with no closed trades returns zero stats."""
        journal = DecisionJournal()
        entry = _make_entry(regime=MarketRegime.HIGH_VOLATILITY)
        journal.record_decision(entry)
        # No outcome recorded.

        perf = journal.get_regime_performance(MarketRegime.HIGH_VOLATILITY)
        assert perf["trades"] == 0
        assert perf["win_rate"] == 0.0
        assert perf["avg_pnl_pct"] == 0.0

    # 6. agent_reliability_per_regime --------------------------------------

    def test_agent_reliability_per_regime(self):
        """Reliability computed for an agent in a specific regime."""
        journal = DecisionJournal()

        # Entry where agent_A is a top signal, in TRENDING_UP, wins.
        e1 = _make_entry(
            asset="BTC",
            regime=MarketRegime.TRENDING_UP,
            top_signals=["agent_A", "agent_B"],
        )
        journal.record_decision(e1)
        journal.record_outcome(e1.entry_id, pnl_pct=3.0, correct=True)

        # Entry where agent_A is a top signal, in TRENDING_UP, loses.
        e2 = _make_entry(
            asset="ETH",
            regime=MarketRegime.TRENDING_UP,
            top_signals=["agent_A"],
        )
        journal.record_decision(e2)
        journal.record_outcome(e2.entry_id, pnl_pct=-1.0, correct=False)

        # Entry where agent_A is a top signal, but in a DIFFERENT regime.
        e3 = _make_entry(
            asset="SOL",
            regime=MarketRegime.HIGH_VOLATILITY,
            top_signals=["agent_A"],
        )
        journal.record_decision(e3)
        journal.record_outcome(e3.entry_id, pnl_pct=2.0, correct=True)

        # Query for agent_A in TRENDING_UP only.
        rel = journal.get_agent_reliability("agent_A", regime=MarketRegime.TRENDING_UP)
        assert rel["trades"] == 2
        assert rel["win_rate"] == pytest.approx(0.5, abs=1e-4)

        # Query for agent_A across all regimes (regime=None).
        rel_all = journal.get_agent_reliability("agent_A", regime=None)
        assert rel_all["trades"] == 3
        assert rel_all["win_rate"] == pytest.approx(2 / 3, abs=1e-4)

    def test_agent_reliability_no_trades(self):
        """Agent with no trades returns zero stats."""
        journal = DecisionJournal()
        rel = journal.get_agent_reliability("unknown_agent")
        assert rel["trades"] == 0
        assert rel["win_rate"] == 0.0

    # 7. build_cio_context -------------------------------------------------

    def test_build_cio_context(self):
        """Returns non-empty context string with stats when closed trades exist."""
        journal = DecisionJournal()

        e1 = _make_entry(asset="BTC", regime=MarketRegime.TRENDING_UP)
        journal.record_decision(e1)
        journal.record_outcome(e1.entry_id, pnl_pct=4.0, correct=True)

        e2 = _make_entry(asset="ETH", regime=MarketRegime.TRENDING_UP)
        journal.record_decision(e2)
        journal.record_outcome(e2.entry_id, pnl_pct=-1.0, correct=False)

        ctx = journal.build_cio_context(
            current_regime=MarketRegime.TRENDING_UP,
            target_asset="BTC",
        )

        assert "DECISION HISTORY CONTEXT:" in ctx
        assert "2 closed trades" in ctx
        assert "1/2 wins" in ctx
        assert "50%" in ctx
        # Regime performance section.
        assert "TRENDING_UP" in ctx or "trending_up" in ctx
        # Target asset section.
        assert "BTC" in ctx

    # 8. build_cio_context_empty -------------------------------------------

    def test_build_cio_context_empty(self):
        """Returns short message when no closed trades exist."""
        journal = DecisionJournal()

        ctx = journal.build_cio_context(current_regime=MarketRegime.LOW_VOLATILITY)

        assert "DECISION HISTORY CONTEXT:" in ctx
        assert "No closed trades yet" in ctx

    def test_build_cio_context_with_lessons(self):
        """Context includes recent lessons when they exist."""
        journal = DecisionJournal()

        entry = _make_entry(asset="BTC")
        journal.record_decision(entry)
        journal.record_outcome(entry.entry_id, pnl_pct=1.0, correct=True)
        journal.set_lesson(entry.entry_id, "Don't chase pumps")

        ctx = journal.build_cio_context(current_regime=MarketRegime.TRENDING_UP)
        assert "Recent lessons:" in ctx
        assert "Don't chase pumps" in ctx

    # 9. recent_lessons ----------------------------------------------------

    def test_recent_lessons(self):
        """Returns lessons sorted by recency (most recent first)."""
        journal = DecisionJournal()
        base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

        for i in range(5):
            entry = _make_entry(
                asset="BTC",
                timestamp=base_time + timedelta(hours=i),
            )
            journal.record_decision(entry)
            journal.set_lesson(entry.entry_id, f"Lesson {i}")

        lessons = journal.get_recent_lessons(limit=3)

        assert len(lessons) == 3
        # Most recent first.
        assert lessons[0] == "Lesson 4"
        assert lessons[1] == "Lesson 3"
        assert lessons[2] == "Lesson 2"

    def test_recent_lessons_empty(self):
        """Returns empty list when no lessons have been set."""
        journal = DecisionJournal()
        assert journal.get_recent_lessons() == []

    # 10. set_lesson -------------------------------------------------------

    def test_set_lesson(self):
        """Lesson and would_repeat fields are set correctly."""
        journal = DecisionJournal()
        entry = _make_entry()
        journal.record_decision(entry)

        journal.set_lesson(entry.entry_id, "Funding divergence was a false signal", would_repeat=False)

        updated = journal.get_entry(entry.entry_id)
        assert updated is not None
        assert updated.lesson_learned == "Funding divergence was a false signal"
        assert updated.would_repeat is False

    def test_set_lesson_defaults_to_would_repeat_true(self):
        """When would_repeat is not specified, defaults to True."""
        journal = DecisionJournal()
        entry = _make_entry()
        journal.record_decision(entry)

        journal.set_lesson(entry.entry_id, "Good thesis execution")

        updated = journal.get_entry(entry.entry_id)
        assert updated is not None
        assert updated.would_repeat is True

    def test_set_lesson_missing_entry(self):
        """Setting a lesson on a non-existent entry does not raise."""
        journal = DecisionJournal()
        journal.set_lesson("nonexistent-id", "Should be a no-op")

    # 11. get_recent_entries -----------------------------------------------

    def test_get_recent_entries(self):
        """Returns entries sorted by recency (most recent first)."""
        journal = DecisionJournal()
        base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
        entry_ids = []

        for i in range(5):
            entry = _make_entry(
                asset=f"ASSET{i}",
                timestamp=base_time + timedelta(hours=i),
            )
            journal.record_decision(entry)
            entry_ids.append(entry.entry_id)

        recent = journal.get_recent_entries(limit=3)

        assert len(recent) == 3
        # Most recent first.
        assert recent[0].asset == "ASSET4"
        assert recent[1].asset == "ASSET3"
        assert recent[2].asset == "ASSET2"

    def test_get_recent_entries_all(self):
        """When limit exceeds count, all entries are returned."""
        journal = DecisionJournal()
        entry = _make_entry()
        journal.record_decision(entry)

        recent = journal.get_recent_entries(limit=20)
        assert len(recent) == 1
        assert recent[0].entry_id == entry.entry_id

    # 12. infer_asset_class ------------------------------------------------

    def test_infer_asset_class(self):
        """BTC=crypto, SPY=equity, GOLD=commodity, EUR=fx."""
        infer = DecisionJournal._infer_asset_class

        assert infer("BTC") == "crypto"
        assert infer("ETH") == "crypto"
        assert infer("SOL") == "crypto"

        assert infer("SPY") == "equity"
        assert infer("QQQ") == "equity"
        assert infer("NVDA") == "equity"

        assert infer("GOLD") == "commodity"
        assert infer("SILVER") == "commodity"
        assert infer("OIL") == "commodity"

        assert infer("EUR") == "fx"
        assert infer("GBP") == "fx"
        assert infer("JPY") == "fx"

    def test_infer_asset_class_strips_perp_suffix(self):
        """Tickers like 'BTC-PERP' are properly classified."""
        assert DecisionJournal._infer_asset_class("BTC-PERP") == "crypto"
        assert DecisionJournal._infer_asset_class("SPY-PERP") == "equity"

    def test_infer_asset_class_unknown_defaults_crypto(self):
        """Unknown ticker defaults to crypto."""
        assert DecisionJournal._infer_asset_class("UNKNOWN_TOKEN") == "crypto"
