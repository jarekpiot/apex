"""
Gamification Engine — competitive ranking system for APEX agents.

Every agent has an AgentProfile tracking XP, rank, streaks, and abilities.
XP is earned by correct signals and lost by incorrect ones. Rank determines
signal weight multiplier, conviction caps, and special abilities (urgent IC,
veto, research suggestions).

Rank progression:
  INTERN -> JUNIOR -> ANALYST -> SENIOR -> PRINCIPAL -> PARTNER

XP cannot go below 0. Agents get benched at 0 XP after demotion.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class AgentRank(StrEnum):
    """Agent ranks — earned through performance, not assigned."""
    INTERN = "intern"
    JUNIOR = "junior"
    ANALYST = "analyst"
    SENIOR = "senior"
    PRINCIPAL = "principal"
    PARTNER = "partner"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SignalOutcome(BaseModel):
    """Result of evaluating a signal against actual market movement."""

    signal_id: str
    agent_id: str
    asset: str
    asset_class: str = "crypto"
    predicted_direction: float
    actual_move_pct: float
    correct: bool
    timeframe_hours: float = 24.0
    conviction: float = 0.5
    was_primary_driver: bool = False
    regime_at_signal: str = ""
    pnl_contribution: float = 0.0


class AgentProfile(BaseModel):
    """Competitive profile for every agent. Persisted to DB + Redis."""

    agent_id: str
    rank: AgentRank = AgentRank.INTERN
    xp: int = 0
    level: int = 1

    # Streak tracking.
    current_win_streak: int = 0
    current_loss_streak: int = 0
    best_win_streak: int = 0
    worst_loss_streak: int = 0

    # Performance windows.
    signals_24h: int = 0
    wins_24h: int = 0
    signals_7d: int = 0
    wins_7d: int = 0
    signals_30d: int = 0
    wins_30d: int = 0

    # Conviction calibration.
    high_conviction_wins: int = 0
    high_conviction_total: int = 0
    low_conviction_wins: int = 0
    low_conviction_total: int = 0

    # Specialisation tracking.
    regime_wins: dict[str, int] = Field(default_factory=dict)
    regime_total: dict[str, int] = Field(default_factory=dict)
    asset_class_wins: dict[str, int] = Field(default_factory=dict)
    asset_class_total: dict[str, int] = Field(default_factory=dict)
    best_regime: str = ""
    worst_regime: str = ""
    best_asset_class: str = ""
    worst_asset_class: str = ""

    # Abilities (unlocked at higher ranks).
    can_trigger_urgent_ic: bool = False
    can_veto_weak_trades: bool = False
    can_suggest_research: bool = False
    signal_weight_multiplier: float = 0.5
    max_conviction_override: float = 0.5

    # Penalties.
    on_probation: bool = False
    benched: bool = False
    probation_until: Optional[datetime] = None

    # Timestamps.
    last_signal: Optional[datetime] = None
    last_win: Optional[datetime] = None
    promoted_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# XP constants
# ---------------------------------------------------------------------------

_XP_CORRECT = 100
_XP_TRADED = 50
_XP_PRIMARY_DRIVER = 200
_XP_HIGH_CONV_CORRECT = 150
_XP_STREAK_BONUS = 25
_XP_CALLED_IT = 300
_XP_REGIME_SPECIALIST = 100
_XP_CROSS_ASSET = 50

_XP_INCORRECT = -50
_XP_HIGH_CONV_WRONG = -100
_XP_PRIMARY_LOSS = -150
_XP_LOSS_STREAK_PENALTY = -25
_XP_BLOWN_CALL = -200

# Rank thresholds: (min_level, min_xp, min_win_rate, min_sharpe, min_streak).
_RANK_THRESHOLDS: dict[AgentRank, dict[str, float]] = {
    AgentRank.JUNIOR: {"level": 3, "xp": 500, "win_rate": 0.45},
    AgentRank.ANALYST: {"level": 7, "xp": 2000, "win_rate": 0.50},
    AgentRank.SENIOR: {"level": 12, "xp": 5000, "win_rate": 0.55, "sharpe": 0.5},
    AgentRank.PRINCIPAL: {"level": 18, "xp": 10000, "win_rate": 0.58, "sharpe": 1.0, "streak": 5},
    AgentRank.PARTNER: {"level": 25, "xp": 20000, "win_rate": 0.60, "sharpe": 1.5},
}

# Rank abilities.
_RANK_ABILITIES: dict[AgentRank, dict[str, Any]] = {
    AgentRank.INTERN: {
        "signal_weight_multiplier": 0.5,
        "max_conviction_override": 0.5,
        "can_trigger_urgent_ic": False,
        "can_veto_weak_trades": False,
        "can_suggest_research": False,
    },
    AgentRank.JUNIOR: {
        "signal_weight_multiplier": 0.75,
        "max_conviction_override": 0.7,
        "can_trigger_urgent_ic": False,
        "can_veto_weak_trades": False,
        "can_suggest_research": False,
    },
    AgentRank.ANALYST: {
        "signal_weight_multiplier": 1.0,
        "max_conviction_override": 1.0,
        "can_trigger_urgent_ic": False,
        "can_veto_weak_trades": False,
        "can_suggest_research": True,
    },
    AgentRank.SENIOR: {
        "signal_weight_multiplier": 1.3,
        "max_conviction_override": 1.0,
        "can_trigger_urgent_ic": False,
        "can_veto_weak_trades": True,
        "can_suggest_research": True,
    },
    AgentRank.PRINCIPAL: {
        "signal_weight_multiplier": 1.5,
        "max_conviction_override": 1.0,
        "can_trigger_urgent_ic": True,
        "can_veto_weak_trades": True,
        "can_suggest_research": True,
    },
    AgentRank.PARTNER: {
        "signal_weight_multiplier": 2.0,
        "max_conviction_override": 1.0,
        "can_trigger_urgent_ic": True,
        "can_veto_weak_trades": True,
        "can_suggest_research": True,
    },
}

# Large-move thresholds by asset class.
LARGE_MOVE_THRESHOLDS: dict[str, float] = {
    "crypto": 0.05,
    "crypto_major": 0.05,
    "crypto_alt": 0.05,
    "equity": 0.015,
    "commodity": 0.02,
    "fx": 0.008,
}

# Rank ordering for promotion / demotion.
_RANK_ORDER = list(AgentRank)


def _level_for_xp(xp: int) -> int:
    """Compute level from total XP. Each level needs progressively more XP."""
    if xp <= 0:
        return 1
    # Roughly: level = floor(sqrt(xp / 25)) + 1, capped at 50.
    return min(int(math.sqrt(xp / 25)) + 1, 50)


# ---------------------------------------------------------------------------
# GamificationEngine
# ---------------------------------------------------------------------------

class GamificationEngine:
    """Manages all agent profiles, XP, ranks, and abilities."""

    def __init__(self) -> None:
        self.profiles: dict[str, AgentProfile] = {}
        # Sharpe ratios fed externally (from PerformanceAuditor).
        self._agent_sharpes: dict[str, float] = {}

    # -- profile management --------------------------------------------------

    def get_profile(self, agent_id: str) -> AgentProfile:
        """Get or create a profile for an agent."""
        if agent_id not in self.profiles:
            self.profiles[agent_id] = AgentProfile(agent_id=agent_id)
        return self.profiles[agent_id]

    def set_sharpe(self, agent_id: str, sharpe: float) -> None:
        """Update externally-computed Sharpe ratio for rank checks."""
        self._agent_sharpes[agent_id] = sharpe

    # -- XP and outcome recording --------------------------------------------

    def record_outcome(self, outcome: SignalOutcome) -> int:
        """Record a signal outcome. Returns net XP change."""
        profile = self.get_profile(outcome.agent_id)
        xp_delta = 0

        # Update window counters.
        profile.signals_30d += 1
        profile.signals_7d += 1
        profile.signals_24h += 1
        profile.last_signal = _utcnow()

        # Regime tracking.
        regime = outcome.regime_at_signal
        if regime:
            profile.regime_total[regime] = profile.regime_total.get(regime, 0) + 1

        # Asset class tracking.
        ac = outcome.asset_class
        if ac:
            profile.asset_class_total[ac] = profile.asset_class_total.get(ac, 0) + 1

        # Conviction calibration tracking.
        if outcome.conviction > 0.7:
            profile.high_conviction_total += 1
        elif outcome.conviction < 0.4:
            profile.low_conviction_total += 1

        large_move_threshold = LARGE_MOVE_THRESHOLDS.get(ac, 0.05)
        is_large_move = abs(outcome.actual_move_pct) >= large_move_threshold

        if outcome.correct:
            # --- Correct signal ---
            profile.wins_30d += 1
            profile.wins_7d += 1
            profile.wins_24h += 1
            profile.last_win = _utcnow()

            xp_delta += _XP_CORRECT

            # Primary driver bonus.
            if outcome.was_primary_driver:
                xp_delta += _XP_PRIMARY_DRIVER

            # Traded bonus.
            if outcome.pnl_contribution != 0:
                xp_delta += _XP_TRADED

            # High conviction correct — calibration bonus.
            if outcome.conviction > 0.7:
                xp_delta += _XP_HIGH_CONV_CORRECT
                profile.high_conviction_wins += 1

            # Low conviction but correct.
            if outcome.conviction < 0.4:
                profile.low_conviction_wins += 1

            # "Called it" bonus for large moves.
            if is_large_move:
                xp_delta += _XP_CALLED_IT

            # Cross-asset bonus (non-crypto).
            if ac not in ("crypto", "crypto_major", "crypto_alt"):
                xp_delta += _XP_CROSS_ASSET

            # Regime specialist bonus: correct in a historically weak regime.
            if regime and profile.regime_total.get(regime, 0) > 5:
                regime_wr = profile.regime_wins.get(regime, 0) / max(profile.regime_total.get(regime, 1), 1)
                if regime_wr < 0.45:
                    xp_delta += _XP_REGIME_SPECIALIST

            # Update regime wins.
            if regime:
                profile.regime_wins[regime] = profile.regime_wins.get(regime, 0) + 1
            if ac:
                profile.asset_class_wins[ac] = profile.asset_class_wins.get(ac, 0) + 1

            # Streaks.
            profile.current_win_streak += 1
            profile.current_loss_streak = 0
            if profile.current_win_streak > profile.best_win_streak:
                profile.best_win_streak = profile.current_win_streak

            # Win streak bonus (+25 per streak length, starting at 3).
            if profile.current_win_streak >= 3:
                xp_delta += _XP_STREAK_BONUS * profile.current_win_streak

            # Probation exit: 2 consecutive wins.
            if profile.on_probation and profile.current_win_streak >= 2:
                profile.on_probation = False
                profile.probation_until = None
                logger.info("Agent %s exits probation (2 consecutive wins).", outcome.agent_id)

        else:
            # --- Incorrect signal ---
            xp_delta += _XP_INCORRECT

            # High conviction wrong — overconfidence penalty.
            if outcome.conviction > 0.7:
                xp_delta += _XP_HIGH_CONV_WRONG

            # Primary driver of losing trade.
            if outcome.was_primary_driver:
                xp_delta += _XP_PRIMARY_LOSS

            # "Blown call" — predicted opposite of large move.
            if is_large_move:
                xp_delta += _XP_BLOWN_CALL

            # Streaks.
            profile.current_loss_streak += 1
            profile.current_win_streak = 0
            if profile.current_loss_streak > profile.worst_loss_streak:
                profile.worst_loss_streak = profile.current_loss_streak

            # Loss streak penalty.
            if profile.current_loss_streak >= 3:
                xp_delta += _XP_LOSS_STREAK_PENALTY * profile.current_loss_streak

            # 3 consecutive losses -> probation.
            if profile.current_loss_streak >= 3 and not profile.on_probation:
                profile.on_probation = True
                profile.probation_until = _utcnow() + timedelta(hours=24)
                logger.info("Agent %s enters probation (3 loss streak).", outcome.agent_id)

            # 5 consecutive losses -> demotion.
            if profile.current_loss_streak >= 5:
                self._demote(profile)

        # Apply XP (cannot go below 0).
        profile.xp = max(0, profile.xp + xp_delta)
        profile.level = _level_for_xp(profile.xp)

        # Check promotion.
        self._check_promotion(profile)

        # Check if benched (0 XP after demotion).
        if profile.xp == 0 and profile.rank == AgentRank.INTERN:
            profile.benched = True
            logger.warning("Agent %s benched (0 XP at INTERN).", outcome.agent_id)

        # Update specialisation fields.
        self._update_specialisations(profile)
        # Update abilities.
        self._apply_rank_abilities(profile)

        return xp_delta

    # -- promotion / demotion ------------------------------------------------

    def _check_promotion(self, profile: AgentProfile) -> bool:
        """Check if agent qualifies for next rank. Promote if yes."""
        idx = _RANK_ORDER.index(profile.rank)
        if idx >= len(_RANK_ORDER) - 1:
            return False  # Already PARTNER.

        next_rank = _RANK_ORDER[idx + 1]
        thresholds = _RANK_THRESHOLDS.get(next_rank)
        if thresholds is None:
            return False

        # Check level / XP.
        if profile.level < thresholds.get("level", 0):
            return False
        if profile.xp < thresholds.get("xp", 0):
            return False

        # Check win rate.
        total = profile.signals_30d
        win_rate = profile.wins_30d / total if total > 0 else 0.0
        if win_rate < thresholds.get("win_rate", 0):
            return False

        # Check Sharpe (if required).
        min_sharpe = thresholds.get("sharpe", 0)
        if min_sharpe > 0:
            sharpe = self._agent_sharpes.get(profile.agent_id, 0.0)
            if sharpe < min_sharpe:
                return False

        # Check best streak (if required).
        min_streak = thresholds.get("streak", 0)
        if min_streak > 0 and profile.best_win_streak < min_streak:
            return False

        # Cannot promote while on probation.
        if profile.on_probation:
            return False

        # Promote!
        profile.rank = next_rank
        profile.promoted_at = _utcnow()
        self._apply_rank_abilities(profile)
        logger.info(
            "Agent %s PROMOTED to %s (XP=%d, Level=%d).",
            profile.agent_id, next_rank, profile.xp, profile.level,
        )
        return True

    def _demote(self, profile: AgentProfile) -> bool:
        """Demote agent one rank level."""
        idx = _RANK_ORDER.index(profile.rank)
        if idx <= 0:
            return False  # Already INTERN.

        prev_rank = _RANK_ORDER[idx - 1]
        profile.rank = prev_rank
        self._apply_rank_abilities(profile)
        logger.warning(
            "Agent %s DEMOTED to %s (5 loss streak).",
            profile.agent_id, prev_rank,
        )
        return True

    def _apply_rank_abilities(self, profile: AgentProfile) -> None:
        """Set abilities based on current rank."""
        abilities = _RANK_ABILITIES.get(profile.rank, _RANK_ABILITIES[AgentRank.INTERN])
        profile.signal_weight_multiplier = abilities["signal_weight_multiplier"]
        profile.max_conviction_override = abilities["max_conviction_override"]
        profile.can_trigger_urgent_ic = abilities["can_trigger_urgent_ic"]
        profile.can_veto_weak_trades = abilities["can_veto_weak_trades"]
        profile.can_suggest_research = abilities["can_suggest_research"]

    def _update_specialisations(self, profile: AgentProfile) -> None:
        """Recompute best/worst regime and asset class."""
        # Best/worst regime.
        best_wr, worst_wr = 1.0, 0.0
        best_r, worst_r = "", ""
        for regime, total in profile.regime_total.items():
            if total < 3:
                continue
            wr = profile.regime_wins.get(regime, 0) / total
            if wr >= best_wr or not best_r:
                best_wr = wr
                best_r = regime
            if wr <= worst_wr or not worst_r:
                worst_wr = wr
                worst_r = regime
        profile.best_regime = best_r
        profile.worst_regime = worst_r

        # Best/worst asset class.
        best_wr, worst_wr = 1.0, 0.0
        best_ac, worst_ac = "", ""
        for ac, total in profile.asset_class_total.items():
            if total < 3:
                continue
            wr = profile.asset_class_wins.get(ac, 0) / total
            if wr >= best_wr or not best_ac:
                best_wr = wr
                best_ac = ac
            if wr <= worst_wr or not worst_ac:
                worst_wr = wr
                worst_ac = ac
        profile.best_asset_class = best_ac
        profile.worst_asset_class = worst_ac

    # -- queries -------------------------------------------------------------

    def get_weight_multiplier(self, agent_id: str) -> float:
        """Get the effective signal weight multiplier for an agent."""
        profile = self.get_profile(agent_id)
        if profile.benched:
            return 0.0
        mult = profile.signal_weight_multiplier
        if profile.on_probation:
            mult *= 0.5
        return mult

    def get_conviction_cap(self, agent_id: str) -> float:
        """Get the max conviction this agent's signals can carry."""
        profile = self.get_profile(agent_id)
        return profile.max_conviction_override

    def is_benched(self, agent_id: str) -> bool:
        """Check if agent is benched (signals should be skipped)."""
        profile = self.get_profile(agent_id)
        return profile.benched

    def get_leaderboard(self) -> list[AgentProfile]:
        """Return agents sorted by XP descending."""
        return sorted(self.profiles.values(), key=lambda p: p.xp, reverse=True)

    def get_rank_display(self, agent_id: str) -> str:
        """Build a compact display string for CIO prompts."""
        p = self.get_profile(agent_id)
        parts = [f"[{p.rank.upper()}, L{p.level}"]
        if p.current_win_streak >= 3:
            parts.append(f"{p.current_win_streak}-streak")
        if p.on_probation:
            parts.append("PROBATION")
        if p.benched:
            parts.append("BENCHED")
        parts_str = ", ".join(parts) + f", {p.signal_weight_multiplier:.1f}x]"
        return parts_str
