"""
APEX — main entry-point.

Boots infrastructure connections, registers agents, and runs the async
event loop until interrupted.
"""

from __future__ import annotations

import asyncio
import signal
import sys

from config.agent_registry import AGENT_REGISTRY, AgentStatus
from config.settings import settings
from core.database import close_db, init_db
from core.logger import get_logger
from core.message_bus import MessageBus

log = get_logger(name="apex.main")

# Registered agent instances — populated by ``_create_agents``.
_agents: list = []


async def _connect_infra(bus: MessageBus) -> None:
    """Establish connections to Redis and Postgres."""
    log.info("Connecting to Redis …")
    await bus.connect()

    log.info("Initialising database …")
    await init_db()

    log.info("Infrastructure ready.")


async def _disconnect_infra(bus: MessageBus) -> None:
    """Tear down infrastructure connections (tolerates partial startup)."""
    try:
        await bus.close()
    except Exception:
        pass
    try:
        await close_db()
    except Exception:
        pass
    log.info("Infrastructure connections closed.")


def _create_agents(bus: MessageBus) -> list:
    """Instantiate every ACTIVE agent from the registry.

    Agents are imported and wired here so that adding a new agent only
    requires a new module + a registry entry.
    """
    from agents.ingestion.market_data import MarketDataCollector
    from agents.ingestion.platform_specialist import PlatformSpecialist
    from agents.execution.engine import ExecutionEngine
    from agents.execution.position_manager import PositionManager
    from agents.risk.guardian import RiskGuardian
    from agents.risk.anomaly import AnomalyDetector
    from agents.risk.hedging import HedgingEngine
    from agents.analysis.technical import TechnicalAnalyst
    from agents.analysis.funding_arb import FundingRateArb
    from agents.fundamental.valuation import FundamentalValuation
    from agents.on_chain.flow import OnChainFlow
    from agents.sentiment.social import SentimentSocial
    from agents.sentiment.funding import SentimentFunding
    from agents.macro.regime import MacroRegime
    from agents.red_team.challenger import RedTeamChallenger
    from agents.meta.orchestrator import MetaOrchestrator

    agents: list = []

    # --- Ingestion / platform layer (no dependencies) ---
    market_data = MarketDataCollector(bus=bus)
    platform = PlatformSpecialist(bus=bus)

    # --- Analysis layer (reads market data streams + DB) ---
    technical_analyst = TechnicalAnalyst(bus=bus)
    funding_arb = FundingRateArb(bus=bus)

    # --- Fundamental / on-chain / sentiment / macro ---
    fundamental = FundamentalValuation(bus=bus)
    on_chain = OnChainFlow(bus=bus)
    sentiment_social = SentimentSocial(bus=bus)
    sentiment_funding = SentimentFunding(bus=bus)
    macro_regime = MacroRegime(bus=bus)

    # --- Meta / ensemble layer (aggregates signals → decisions) ---
    orchestrator = MetaOrchestrator(bus=bus)

    # --- Risk layer (reads streams, no direct agent deps) ---
    risk_guardian = RiskGuardian(bus=bus)
    anomaly_detector = AnomalyDetector(bus=bus)
    hedging_engine = HedgingEngine(bus=bus)

    # --- Red team (advisory, reads decisions:pending) ---
    red_team = RedTeamChallenger(bus=bus)

    # --- Execution layer (depends on platform specialist) ---
    execution = ExecutionEngine(bus=bus, platform_specialist=platform)
    position_mgr = PositionManager(bus=bus, platform_specialist=platform)

    # Map agent_id -> instance for implemented agents.
    implemented = {
        "market_data": market_data,
        "platform_specialist": platform,
        "technical_analyst": technical_analyst,
        "funding_arb": funding_arb,
        "fundamental_valuation": fundamental,
        "on_chain_flow": on_chain,
        "sentiment_social": sentiment_social,
        "sentiment_funding": sentiment_funding,
        "macro_regime": macro_regime,
        "meta_orchestrator": orchestrator,
        "risk_guardian": risk_guardian,
        "anomaly_detector": anomaly_detector,
        "hedging_engine": hedging_engine,
        "red_team": red_team,
        "execution_engine": execution,
        "position_manager": position_mgr,
    }

    for entry in AGENT_REGISTRY.values():
        if entry.status == AgentStatus.DISABLED:
            continue
        instance = implemented.get(entry.agent_id)
        if instance:
            agents.append(instance)
            log.info("Agent READY: %s [%s]", entry.agent_id, entry.status)
        else:
            log.info(
                "Agent registered (pending implementation): %s [%s]",
                entry.agent_id,
                entry.status,
            )
    return agents


async def _start_agents(agents: list) -> None:
    for agent in agents:
        await agent.start()


async def _stop_agents(agents: list) -> None:
    for agent in agents:
        await agent.stop()


async def run() -> None:
    """Main async entry-point."""
    bus = MessageBus()

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        log.info("Shutdown signal received.")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals.
            pass

    agents: list = []
    try:
        await _connect_infra(bus)
        agents = _create_agents(bus)
        await _start_agents(agents)

        log.info(
            "APEX is live  |  testnet=%s  |  agents=%d  |  max_pos=%.1f%%  |  circuit_breaker=%.1f%%",
            settings.hyperliquid_testnet,
            len(agents),
            settings.max_position_pct * 100,
            settings.circuit_breaker_drawdown * 100,
        )

        # Block until a termination signal arrives.
        await shutdown_event.wait()

    except Exception:
        log.exception("Fatal error during startup.")

    finally:
        log.info("Shutting down …")
        await _stop_agents(agents)
        await _disconnect_infra(bus)
        log.info("APEX stopped.")


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
