"""Tests for Pydantic model validation, defaults, and serialization."""

import pytest
from pydantic import ValidationError

from core.models import (
    AgentSignal,
    AnomalyAlert,
    AnomalyType,
    AlertSeverity,
    AssetClass,
    ChallengeType,
    Direction,
    ExecutedTrade,
    ExecutionMode,
    HedgeSuggestion,
    PortfolioState,
    Position,
    PriceUpdate,
    RedTeamChallenge,
    RedTeamRecommendation,
    RiskCheck,
    Timeframe,
    TradeAction,
    TradeDecision,
)


class TestAgentSignal:
    def test_direction_too_high(self):
        with pytest.raises(ValidationError):
            AgentSignal(
                agent_id="t", asset="BTC", direction=1.5, conviction=0.5,
                timeframe=Timeframe.INTRADAY,
            )

    def test_direction_too_low(self):
        with pytest.raises(ValidationError):
            AgentSignal(
                agent_id="t", asset="BTC", direction=-1.5, conviction=0.5,
                timeframe=Timeframe.INTRADAY,
            )

    def test_conviction_too_high(self):
        with pytest.raises(ValidationError):
            AgentSignal(
                agent_id="t", asset="BTC", direction=0.0, conviction=1.5,
                timeframe=Timeframe.INTRADAY,
            )

    def test_conviction_too_low(self):
        with pytest.raises(ValidationError):
            AgentSignal(
                agent_id="t", asset="BTC", direction=0.0, conviction=-0.1,
                timeframe=Timeframe.INTRADAY,
            )

    def test_defaults(self):
        sig = AgentSignal(
            agent_id="t", asset="BTC", direction=0.5, conviction=0.8,
            timeframe=Timeframe.INTRADAY,
        )
        assert sig.signal_id  # auto-generated
        assert sig.timestamp is not None
        assert sig.reasoning == ""
        assert sig.data_sources == []
        assert sig.metadata == {}
        assert sig.expires_at is None


class TestTradeDecision:
    def test_size_pct_bounds(self):
        with pytest.raises(ValidationError):
            TradeDecision(
                asset="BTC", action=TradeAction.OPEN_LONG,
                size_pct=1.5, consensus_score=0.5,
            )
        with pytest.raises(ValidationError):
            TradeDecision(
                asset="BTC", action=TradeAction.OPEN_LONG,
                size_pct=-0.1, consensus_score=0.5,
            )

    def test_consensus_bounds(self):
        with pytest.raises(ValidationError):
            TradeDecision(
                asset="BTC", action=TradeAction.OPEN_LONG,
                size_pct=0.01, consensus_score=1.5,
            )
        with pytest.raises(ValidationError):
            TradeDecision(
                asset="BTC", action=TradeAction.OPEN_LONG,
                size_pct=0.01, consensus_score=-1.5,
            )


class TestRedTeamChallenge:
    def test_severity_bounds(self):
        with pytest.raises(ValidationError):
            RedTeamChallenge(
                decision_id="x", challenge_type=ChallengeType.CONTRARIAN_DATA,
                severity=1.5, counter_thesis="test",
                recommendation=RedTeamRecommendation.PROCEED,
            )

    def test_conviction_adj_bounds(self):
        with pytest.raises(ValidationError):
            RedTeamChallenge(
                decision_id="x", challenge_type=ChallengeType.CONTRARIAN_DATA,
                severity=0.5, counter_thesis="test",
                recommendation=RedTeamRecommendation.PROCEED,
                conviction_adjustment=0.5,  # Must be <= 0
            )


class TestPortfolioState:
    def test_drawdown_non_negative(self):
        with pytest.raises(ValidationError):
            PortfolioState(drawdown=-0.1)


class TestPositionRoundtrip:
    def test_json_roundtrip(self):
        from datetime import datetime, timezone
        pos = Position(
            asset="ETH", direction=Direction.LONG, size_pct=0.05,
            entry_price=3000.0, current_price=3100.0, unrealised_pnl=500.0,
            opened_at=datetime.now(timezone.utc),
        )
        json_str = pos.model_dump_json()
        rebuilt = Position.model_validate_json(json_str)
        assert rebuilt.asset == "ETH"
        assert rebuilt.entry_price == 3000.0
        assert rebuilt.unrealised_pnl == 500.0


class TestEnumsAreStr:
    def test_all_enums_have_string_values(self):
        assert Direction.LONG == "long"
        assert TradeAction.OPEN_LONG == "open_long"
        assert Timeframe.SCALP == "scalp"
        assert ExecutionMode.MARKET == "market"
        assert AlertSeverity.CRITICAL == "critical"
        assert AssetClass.CRYPTO_MAJOR == "crypto_major"
        assert AnomalyType.VOLUME_SPIKE == "volume_spike"
        assert ChallengeType.CONTRARIAN_DATA == "contrarian_data"
        assert RedTeamRecommendation.VETO == "veto"


class TestValidModelsConstruct:
    def test_every_model_builds(self):
        """Sanity check that all models can be constructed with valid data."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

        AgentSignal(agent_id="t", asset="BTC", direction=0.5, conviction=0.5, timeframe=Timeframe.INTRADAY)
        TradeDecision(asset="BTC", action=TradeAction.OPEN_LONG, size_pct=0.01, consensus_score=0.5)
        RedTeamChallenge(decision_id="x", challenge_type=ChallengeType.CONTRARIAN_DATA,
                         severity=0.5, counter_thesis="t", recommendation=RedTeamRecommendation.PROCEED)
        RiskCheck(decision_id="x", approved=True)
        Position(asset="BTC", direction=Direction.LONG, size_pct=0.02,
                 entry_price=50000, current_price=51000, unrealised_pnl=100, opened_at=now)
        PortfolioState()
        PriceUpdate(prices={"BTC": 50000.0})
        ExecutedTrade(decision_id="x", asset="BTC", side="buy", size=0.1, price=50000)
        AnomalyAlert(anomaly_type=AnomalyType.PRICE_DEVIATION, severity=AlertSeverity.WARNING)
        HedgeSuggestion(reason="test", hedge_asset="GOLD",
                        hedge_action=TradeAction.OPEN_LONG, hedge_size_pct=0.01)
