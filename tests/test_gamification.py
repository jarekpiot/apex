"""
Tests for core.gamification — GamificationEngine, AgentProfile, ranks, XP, streaks.

No real Redis or DB needed — the gamification engine is entirely in-memory.
"""

from __future__ import annotations

import uuid

import pytest

from core.gamification import (
    LARGE_MOVE_THRESHOLDS,
    AgentProfile,
    AgentRank,
    GamificationEngine,
    SignalOutcome,
    _level_for_xp,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_outcome(
    agent_id: str = "agent_1",
    correct: bool = True,
    conviction: float = 0.5,
    asset_class: str = "crypto",
    actual_move_pct: float = 0.02,
    was_primary_driver: bool = False,
    regime: str = "",
) -> SignalOutcome:
    """Create a SignalOutcome with sensible defaults for testing."""
    return SignalOutcome(
        signal_id=uuid.uuid4().hex,
        agent_id=agent_id,
        asset="BTC",
        asset_class=asset_class,
        predicted_direction=1.0 if correct else -1.0,
        actual_move_pct=actual_move_pct,
        correct=correct,
        conviction=conviction,
        was_primary_driver=was_primary_driver,
        regime_at_signal=regime,
    )


# ---------------------------------------------------------------------------
# 1. XP gain on correct signal
# ---------------------------------------------------------------------------

def test_xp_gain_on_correct_signal():
    """Correct signal grants +100 XP (base)."""
    engine = GamificationEngine()
    outcome = _make_outcome(correct=True, conviction=0.5)
    xp_delta = engine.record_outcome(outcome)
    # Base correct = +100, no high-conviction bonus, no large-move, no cross-asset.
    assert xp_delta == 100
    assert engine.get_profile("agent_1").xp == 100


# ---------------------------------------------------------------------------
# 2. XP loss on incorrect signal
# ---------------------------------------------------------------------------

def test_xp_loss_on_incorrect_signal():
    """Incorrect signal costs -50 XP."""
    engine = GamificationEngine()
    # Give agent some starting XP so we can observe the loss.
    engine.get_profile("agent_1").xp = 200
    outcome = _make_outcome(correct=False, conviction=0.5)
    xp_delta = engine.record_outcome(outcome)
    assert xp_delta == -50
    assert engine.get_profile("agent_1").xp == 150


# ---------------------------------------------------------------------------
# 3. High conviction correct bonus
# ---------------------------------------------------------------------------

def test_high_conviction_correct_bonus():
    """Conviction > 0.7 AND correct grants extra +150 XP on top of base +100."""
    engine = GamificationEngine()
    outcome = _make_outcome(correct=True, conviction=0.9)
    xp_delta = engine.record_outcome(outcome)
    # Base +100 + high-conviction bonus +150 = 250.
    assert xp_delta == 250
    profile = engine.get_profile("agent_1")
    assert profile.xp == 250
    assert profile.high_conviction_wins == 1
    assert profile.high_conviction_total == 1


# ---------------------------------------------------------------------------
# 4. High conviction wrong penalty
# ---------------------------------------------------------------------------

def test_high_conviction_wrong_penalty():
    """Conviction > 0.7 AND wrong costs extra -100 XP on top of base -50."""
    engine = GamificationEngine()
    engine.get_profile("agent_1").xp = 500
    outcome = _make_outcome(correct=False, conviction=0.9)
    xp_delta = engine.record_outcome(outcome)
    # Base -50 + high-conviction penalty -100 = -150.
    assert xp_delta == -150
    assert engine.get_profile("agent_1").xp == 350


# ---------------------------------------------------------------------------
# 5. Win streak bonus XP
# ---------------------------------------------------------------------------

def test_win_streak_bonus_xp():
    """3+ win streak gives streak bonus (+25 x streak length)."""
    engine = GamificationEngine()
    # Record 3 consecutive wins.
    for _ in range(2):
        engine.record_outcome(_make_outcome(correct=True, conviction=0.5))
    # Third win triggers streak bonus: +100 base + 25*3 = +175.
    xp_delta = engine.record_outcome(_make_outcome(correct=True, conviction=0.5))
    assert xp_delta == 100 + 25 * 3  # 175
    profile = engine.get_profile("agent_1")
    assert profile.current_win_streak == 3
    assert profile.best_win_streak == 3


# ---------------------------------------------------------------------------
# 6. Loss streak penalty
# ---------------------------------------------------------------------------

def test_loss_streak_penalty():
    """3+ loss streak gives penalty (-25 x streak length)."""
    engine = GamificationEngine()
    engine.get_profile("agent_1").xp = 1000
    # Record 2 losses first.
    for _ in range(2):
        engine.record_outcome(_make_outcome(correct=False, conviction=0.5))
    # Third loss triggers streak penalty: -50 base + -25*3 = -125.
    xp_delta = engine.record_outcome(_make_outcome(correct=False, conviction=0.5))
    assert xp_delta == -50 + (-25 * 3)  # -125
    profile = engine.get_profile("agent_1")
    assert profile.current_loss_streak == 3


# ---------------------------------------------------------------------------
# 7. Promotion INTERN -> JUNIOR
# ---------------------------------------------------------------------------

def test_promotion_intern_to_junior():
    """Level 3+ (500 XP) + win rate > 45% promotes INTERN to JUNIOR."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    # Directly set XP to near the threshold so we can trigger promotion with
    # a correct signal. We need >= 500 XP and level >= 3 and win_rate > 0.45.
    # _level_for_xp(500) = floor(sqrt(500/25))+1 = floor(4.47)+1 = 5. OK.
    profile.xp = 499
    profile.level = _level_for_xp(499)
    # Set up win rate: need wins_30d / signals_30d > 0.45 *after* recording.
    profile.signals_30d = 9
    profile.wins_30d = 5  # After +1: 6/10 = 0.60 > 0.45.

    outcome = _make_outcome(correct=True, conviction=0.5)
    engine.record_outcome(outcome)

    profile = engine.get_profile("agent_1")
    assert profile.rank == AgentRank.JUNIOR
    assert profile.xp >= 500
    assert profile.signal_weight_multiplier == 0.75


# ---------------------------------------------------------------------------
# 8. Promotion requires win rate
# ---------------------------------------------------------------------------

def test_promotion_requires_win_rate():
    """High XP but low win rate does NOT promote."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    # Set XP and level high enough, but terrible win rate.
    profile.xp = 600
    profile.level = _level_for_xp(600)
    profile.signals_30d = 20
    profile.wins_30d = 5  # 25% win rate — below 45% threshold.

    # Record another correct signal; even after: 6/21 = 0.286 < 0.45.
    outcome = _make_outcome(correct=True, conviction=0.5)
    engine.record_outcome(outcome)

    profile = engine.get_profile("agent_1")
    assert profile.rank == AgentRank.INTERN  # No promotion.


# ---------------------------------------------------------------------------
# 9. Demotion on 5 losses
# ---------------------------------------------------------------------------

def test_demotion_on_5_losses():
    """5 consecutive losses demotes one rank."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.rank = AgentRank.JUNIOR
    profile.xp = 1000
    profile.level = _level_for_xp(1000)
    engine._apply_rank_abilities(profile)

    # Record 5 consecutive losses.
    for _ in range(5):
        engine.record_outcome(_make_outcome(correct=False, conviction=0.5))

    profile = engine.get_profile("agent_1")
    assert profile.rank == AgentRank.INTERN
    assert profile.current_loss_streak == 5


# ---------------------------------------------------------------------------
# 10. Probation on 3 losses
# ---------------------------------------------------------------------------

def test_probation_on_3_losses():
    """3 consecutive losses enters probation."""
    engine = GamificationEngine()
    engine.get_profile("agent_1").xp = 500

    for _ in range(3):
        engine.record_outcome(_make_outcome(correct=False, conviction=0.5))

    profile = engine.get_profile("agent_1")
    assert profile.on_probation is True
    assert profile.probation_until is not None
    assert profile.current_loss_streak == 3


# ---------------------------------------------------------------------------
# 11. Probation exit requires 2 wins
# ---------------------------------------------------------------------------

def test_probation_exit_requires_2_wins():
    """Need 2 consecutive wins to exit probation."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.xp = 500
    profile.on_probation = True

    # One win is not enough.
    engine.record_outcome(_make_outcome(correct=True, conviction=0.5))
    assert engine.get_profile("agent_1").on_probation is True

    # Second consecutive win exits probation.
    engine.record_outcome(_make_outcome(correct=True, conviction=0.5))
    assert engine.get_profile("agent_1").on_probation is False
    assert engine.get_profile("agent_1").probation_until is None


# ---------------------------------------------------------------------------
# 12. Benched agent signals skipped
# ---------------------------------------------------------------------------

def test_benched_agent_signals_skipped():
    """Benched agent returns 0.0 weight multiplier."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.benched = True

    mult = engine.get_weight_multiplier("agent_1")
    assert mult == 0.0
    assert engine.is_benched("agent_1") is True


# ---------------------------------------------------------------------------
# 13. Weight multiplier by rank
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "rank, expected_multiplier",
    [
        (AgentRank.INTERN, 0.5),
        (AgentRank.JUNIOR, 0.75),
        (AgentRank.ANALYST, 1.0),
        (AgentRank.SENIOR, 1.3),
        (AgentRank.PRINCIPAL, 1.5),
        (AgentRank.PARTNER, 2.0),
    ],
)
def test_weight_multiplier_by_rank(rank: AgentRank, expected_multiplier: float):
    """Each rank has expected signal weight multiplier."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.rank = rank
    engine._apply_rank_abilities(profile)

    mult = engine.get_weight_multiplier("agent_1")
    assert mult == pytest.approx(expected_multiplier)


# ---------------------------------------------------------------------------
# 14. Conviction cap by rank
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "rank, expected_cap",
    [
        (AgentRank.INTERN, 0.5),
        (AgentRank.JUNIOR, 0.7),
        (AgentRank.ANALYST, 1.0),
        (AgentRank.SENIOR, 1.0),
        (AgentRank.PRINCIPAL, 1.0),
        (AgentRank.PARTNER, 1.0),
    ],
)
def test_conviction_cap_by_rank(rank: AgentRank, expected_cap: float):
    """INTERN capped at 0.5, JUNIOR at 0.7, ANALYST+ at 1.0."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.rank = rank
    engine._apply_rank_abilities(profile)

    cap = engine.get_conviction_cap("agent_1")
    assert cap == pytest.approx(expected_cap)


# ---------------------------------------------------------------------------
# 15. XP cannot go below zero
# ---------------------------------------------------------------------------

def test_xp_cannot_go_below_zero():
    """XP floor is 0 even after large penalties."""
    engine = GamificationEngine()
    profile = engine.get_profile("agent_1")
    profile.xp = 10  # Start with small XP.

    # High-conviction wrong: -50 + -100 = -150 penalty, but XP floors at 0.
    outcome = _make_outcome(correct=False, conviction=0.9)
    engine.record_outcome(outcome)

    assert engine.get_profile("agent_1").xp == 0


# ---------------------------------------------------------------------------
# 16. Regime specialist tracking
# ---------------------------------------------------------------------------

def test_regime_specialist_tracking():
    """Regime wins/totals tracked correctly."""
    engine = GamificationEngine()

    # Record several outcomes in "risk_off" regime.
    for _ in range(4):
        engine.record_outcome(
            _make_outcome(correct=True, conviction=0.5, regime="risk_off")
        )
    for _ in range(2):
        engine.record_outcome(
            _make_outcome(correct=False, conviction=0.5, regime="risk_off")
        )

    profile = engine.get_profile("agent_1")
    assert profile.regime_total["risk_off"] == 6
    assert profile.regime_wins["risk_off"] == 4


# ---------------------------------------------------------------------------
# 17. Leaderboard ordering
# ---------------------------------------------------------------------------

def test_leaderboard_ordering():
    """Leaderboard sorted by XP descending."""
    engine = GamificationEngine()
    # Create three agents with different XP.
    engine.get_profile("low").xp = 100
    engine.get_profile("mid").xp = 500
    engine.get_profile("high").xp = 1000

    board = engine.get_leaderboard()
    xps = [p.xp for p in board]
    assert xps == [1000, 500, 100]
    assert board[0].agent_id == "high"
    assert board[1].agent_id == "mid"
    assert board[2].agent_id == "low"


# ---------------------------------------------------------------------------
# 18. "Called it" bonus
# ---------------------------------------------------------------------------

def test_called_it_bonus():
    """Large move (>5% crypto) AND correct gives +300 XP bonus."""
    engine = GamificationEngine()
    # actual_move_pct = 0.06 > crypto threshold 0.05.
    outcome = _make_outcome(
        correct=True, conviction=0.5, asset_class="crypto", actual_move_pct=0.06,
    )
    xp_delta = engine.record_outcome(outcome)
    # Base +100 + called-it +300 = 400.
    assert xp_delta == 400
    assert engine.get_profile("agent_1").xp == 400


# ---------------------------------------------------------------------------
# 19. Cross-asset bonus
# ---------------------------------------------------------------------------

def test_cross_asset_bonus():
    """Non-crypto correct signal gives extra +50 XP."""
    engine = GamificationEngine()
    outcome = _make_outcome(
        correct=True, conviction=0.5, asset_class="equity", actual_move_pct=0.005,
    )
    xp_delta = engine.record_outcome(outcome)
    # Base +100 + cross-asset +50 = 150. (0.005 < 0.015 equity threshold, no called-it.)
    assert xp_delta == 150
    assert engine.get_profile("agent_1").xp == 150


# ---------------------------------------------------------------------------
# 20. Primary driver bonus
# ---------------------------------------------------------------------------

def test_primary_driver_bonus():
    """was_primary_driver=True AND correct gives +200 extra XP."""
    engine = GamificationEngine()
    outcome = _make_outcome(
        correct=True, conviction=0.5, was_primary_driver=True,
    )
    xp_delta = engine.record_outcome(outcome)
    # Base +100 + primary driver +200 = 300.
    assert xp_delta == 300
    assert engine.get_profile("agent_1").xp == 300
