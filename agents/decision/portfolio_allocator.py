"""
PortfolioAllocator — Kelly criterion position sizing.

Computes optimal position sizes using the Kelly criterion (Half-Kelly default)
with adjustments for:
  - CIO conviction level
  - Debate quality (IC record)
  - Portfolio correlation
  - Regime risk multiplier
  - Drawdown limits

Used by the CIO during the Investment Committee's Phase 5 (position sizing).
This agent does NOT run its own loop — it exposes ``compute_size()`` which
the CIO calls synchronously during debate.
"""

from __future__ import annotations

import math
from typing import Any

from agents.base_agent import BaseAgent
from config.settings import settings
from core.message_bus import MessageBus, STREAM_PORTFOLIO_STATE
from core.models import (
    Direction,
    InvestmentThesis,
    MarketRegime,
    PortfolioState,
    TradeAction,
    TradeProposal,
)


# Regime → risk multiplier (scales Kelly fraction).
_REGIME_MULTIPLIER: dict[MarketRegime, float] = {
    MarketRegime.TRENDING_UP: 1.0,
    MarketRegime.TRENDING_DOWN: 0.6,
    MarketRegime.MEAN_REVERTING: 0.7,
    MarketRegime.HIGH_VOLATILITY: 0.4,
    MarketRegime.LOW_VOLATILITY: 0.9,
}


class PortfolioAllocator(BaseAgent):
    """Kelly criterion position sizer for the CIO."""

    def __init__(self, bus: MessageBus, **kw: Any) -> None:
        super().__init__(
            agent_id="portfolio_allocator",
            agent_type="decision",
            bus=bus,
            **kw,
        )
        self._portfolio = PortfolioState()

    async def process(self) -> None:
        """Listen for portfolio state updates."""
        async for _mid, payload in self.bus.subscribe_to(
            STREAM_PORTFOLIO_STATE,
            group="portfolio_allocator",
            consumer=self.agent_id,
        ):
            try:
                self._portfolio = PortfolioState.model_validate(payload)
            except Exception:
                pass

    def compute_size(
        self,
        thesis: InvestmentThesis,
        regime: MarketRegime = MarketRegime.LOW_VOLATILITY,
        debate_quality: float = 0.5,
        win_rate: float = 0.55,
        avg_win_loss_ratio: float = 1.5,
    ) -> TradeProposal:
        """Compute a sized trade proposal from an investment thesis.

        Parameters
        ----------
        thesis : InvestmentThesis
            The trade thesis from the CIO.
        regime : MarketRegime
            Current market regime (adjusts risk budget).
        debate_quality : float
            Quality score from the IC debate (0..1).
        win_rate : float
            Estimated probability of a winning trade.
        avg_win_loss_ratio : float
            Expected reward-to-risk ratio.
        """
        # --- Kelly fraction ---
        # f* = (p * b - q) / b  where p=win_rate, q=1-p, b=win/loss ratio.
        p = max(0.01, min(0.99, win_rate))
        q = 1.0 - p
        b = max(0.01, avg_win_loss_ratio)
        kelly_full = (p * b - q) / b
        kelly_full = max(0.0, kelly_full)  # No negative sizing.

        # Half-Kelly (or configured fraction).
        kelly_frac = kelly_full * settings.kelly_fraction

        # --- Adjustments ---
        # 1. Conviction from thesis.
        conviction_mult = thesis.conviction

        # 2. Debate quality: good debate → more confidence.
        debate_mult = 0.5 + 0.5 * debate_quality

        # 3. Regime risk.
        regime_mult = _REGIME_MULTIPLIER.get(regime, 0.5)

        # 4. Drawdown dampening: reduce size as drawdown approaches limits.
        dd = self._portfolio.drawdown
        dd_limit = settings.max_daily_drawdown
        if dd > dd_limit * 0.5:
            dd_mult = max(0.1, 1.0 - (dd - dd_limit * 0.5) / (dd_limit * 0.5))
        else:
            dd_mult = 1.0

        # 5. Portfolio correlation: penalise if many same-direction positions.
        same_dir_count = self._count_same_direction(thesis.direction)
        corr_mult = max(0.3, 1.0 - same_dir_count * 0.1)

        # --- Final size ---
        raw_size = kelly_frac * conviction_mult * debate_mult * regime_mult * dd_mult * corr_mult
        final_size = min(raw_size, settings.max_position_pct)
        final_size = max(0.001, final_size)  # Minimum floor.

        action = TradeAction.OPEN_LONG if thesis.direction == Direction.LONG else TradeAction.OPEN_SHORT

        return TradeProposal(
            thesis_id=thesis.thesis_id,
            asset=thesis.asset,
            action=action,
            size_pct=round(final_size, 6),
            entry_price=thesis.entry_price,
            stop_loss=thesis.stop_loss,
            take_profit=thesis.take_profit,
            kelly_fraction=round(kelly_frac, 6),
            conviction_adjusted_size=round(raw_size, 6),
            reasoning=(
                f"Kelly={kelly_full:.4f}, half={kelly_frac:.4f}, "
                f"conv={conviction_mult:.2f}, debate={debate_mult:.2f}, "
                f"regime={regime_mult:.2f}, dd={dd_mult:.2f}, corr={corr_mult:.2f}"
            ),
        )

    def _count_same_direction(self, direction: Direction) -> int:
        """Count portfolio positions in the same direction."""
        return sum(
            1 for pos in self._portfolio.positions
            if pos.direction == direction
        )

    def health(self) -> dict[str, Any]:
        base = super().health()
        base.update({
            "portfolio_positions": len(self._portfolio.positions),
            "portfolio_nav": self._portfolio.total_nav,
            "portfolio_drawdown": self._portfolio.drawdown,
        })
        return base
