# APEX — AI-Powered Crypto Hedge Fund

> **This file is read by Claude at the start of every session.**
> **Update it after each sprint to maintain continuity.**

---

## Project Overview

APEX is an autonomous multi-agent trading system on Hyperliquid perpetual futures
(crypto, equities, commodities, FX). 24 registered agents collaborate via Redis
Streams, persist to TimescaleDB, and execute through the Hyperliquid SDK.

**Repository:** `C:\apex\`
**Language:** Python 3.13
**Mode:** Paper trading (default). Live requires `.env` config + `paper_trade_mode=False`.

---

## Sprint Status

| Sprint | Status | What Was Built |
|--------|--------|----------------|
| 1 — Foundation | ✅ DONE | Pydantic v2 models, Redis Streams bus, TimescaleDB schema, BaseAgent, settings, agent registry |
| 2 — Hyperliquid | ✅ DONE | MarketDataCollector, PlatformSpecialist, ExecutionEngine, PositionManager |
| 3 — Risk Gate | ✅ DONE | RiskGuardian, AnomalyDetector, HedgingEngine |
| 4 — Orchestration | ✅ DONE | MetaOrchestrator (consensus, signal aggregation, decision dispatch) |
| 5 — TA Agents | ✅ DONE | TechnicalAnalyst, FundingRateArb |
| 6 — External Intel | ✅ DONE | SentimentSocial, SentimentFunding, MacroRegime |
| 7 — On-Chain | ✅ DONE | FundamentalValuation, OnChainFlow |
| 8 — Red Team | ✅ DONE | RedTeamChallenger (heuristic adversarial challenges) |
| 9 — Data Ingestion | ✅ DONE | OnChainIntelligence, MacroFeed, SentimentScraper |
| 10 — CIO System | ✅ DONE | ChiefInvestmentOfficer, SignalAggregator, RedTeamStrategist, PortfolioAllocator, RegimeClassifier |

**24 of 24 agents implemented.** All agents wired and ready.

---

## Decision Flow (end-to-end)

```
Data ingestion → data:onchain / data:macro / data:sentiment / data:regime
  → SignalAggregator compiles real-time SignalMatrix → cio:signal_matrix
    → CIO (Claude reasoning) generates InvestmentThesis
      → IC 5-phase debate (experts + RedTeamStrategist + PortfolioAllocator)
        → CIO decision → decisions:pending
          → RiskGuardian (7-check pipeline) → decisions:approved → ExecutionEngine → trades:executed
                                              → decisions:rejected (with veto_reason)

Legacy path (parallel):
  Analysis agents → apex:signals → MetaOrchestrator (weighted consensus) → decisions:pending

AnomalyDetector → risk:anomaly → RiskGuardian enters defensive mode
HedgingEngine → decisions:pending (goes through full risk pipeline)
PositionManager → portfolio:state (consumed by risk + hedging + CIO)
RegimeClassifier → data:regime (HMM-based 5-state classification)
```

---

## Redis Streams

| Stream | Publisher | Subscribers |
|--------|-----------|-------------|
| `market:prices` | MarketDataCollector | AnomalyDetector, RiskGuardian, HedgingEngine |
| `platform:state` | PlatformSpecialist | — |
| `platform:account_health` | PlatformSpecialist | ExecutionEngine, PositionManager |
| `platform:execution_advisory` | PlatformSpecialist | ExecutionEngine, AnomalyDetector |
| `platform:new_listing` | PlatformSpecialist | — |
| `data:onchain` | OnChainIntelligence | (analysis agents) |
| `data:macro` | MacroFeed | (analysis agents) |
| `data:sentiment` | SentimentScraper | (analysis agents) |
| `data:regime` | RegimeClassifier | CIO, SignalAggregator |
| `cio:signal_matrix` | SignalAggregator | CIO |
| `cio:research_tasks` | CIO | (research agents) |
| `cio:research_results` | (research agents) | CIO |
| `cio:priorities` | CIO | (all agents) |
| `apex:signals` | Analysis agents | MetaOrchestrator, SignalAggregator |
| `decisions:pending` | MetaOrchestrator, CIO, HedgingEngine | RiskGuardian |
| `decisions:approved` | RiskGuardian | ExecutionEngine |
| `decisions:rejected` | RiskGuardian | (audit) |
| `trades:executed` | ExecutionEngine | PositionManager |
| `portfolio:state` | PositionManager | RiskGuardian, HedgingEngine |
| `risk:anomaly` | AnomalyDetector | RiskGuardian |
| `apex:decisions` | (legacy) | — |
| `apex:risk_checks` | RiskGuardian | (audit) |
| `apex:red_team` | RedTeam | — |

---

## Key Models (core/models.py)

| Model | Purpose |
|-------|---------|
| `AgentSignal` | Directional opinion from any agent (direction -1..+1, conviction 0..1) |
| `TradeDecision` | Aggregated decision routed through risk pipeline |
| `RiskCheck` | RiskGuardian's verdict (approved/rejected + adjustments) |
| `RedTeamChallenge` | Adversarial challenge to a trade thesis |
| `PortfolioState` | Full snapshot: positions, NAV, exposure, drawdown |
| `Position` | Single open position with entry/current price, unrealised PnL |
| `PriceUpdate` | Tick-level mid-prices from WebSocket |
| `AssetInfo` | Perp metadata: leverage, tick size, OI, volume, funding |
| `AccountHealth` | Margin utilisation, liquidation distance, severity |
| `ExecutionAdvisory` | Per-asset depth, spread, recommended order type |
| `ExecutedTrade` | Fill confirmation with slippage, fees, execution mode |
| `AnomalyAlert` | Market anomaly: type, severity, asset, value vs threshold |
| `HedgeSuggestion` | Hedge proposal: asset, action, size, correlation |
| `OnChainDataPoint` | Aggregated on-chain metrics: TVL, DEX vol, stablecoins, trending, unlocks |
| `MacroDataPoint` | Macro indicators: FRED, yfinance, fear/greed, calendar |
| `SentimentDataPoint` | Per-asset sentiment: Reddit, news, trends, LunarCrush |
| `RegimeState` | HMM regime classification (5 states) with confidence |
| `MarketBrief` | CIO's synthesised market overview (themes, opportunities, risks) |
| `InvestmentThesis` | Full trade thesis: edge, catalyst, risk factors, invalidation |
| `ExpertOpinion` | Expert review during IC debate |
| `RedTeamChallengeV2` | LLM-powered adversarial challenge (7 categories) |
| `TradeProposal` | Kelly-criterion sized trade proposal |
| `InvestmentCommitteeRecord` | Master IC debate record (thesis + opinions + challenges + decision) |
| `StrategyPriority` | CIO's broadcast strategic priorities |
| `DailySummary` | End-of-day performance summary with agent grades |
| `PositionReview` | CIO's review verdict for open positions |
| `ResearchTask` / `ResearchResult` | CIO-to-agent research assignment and response |
| `StopLevel` | Stop-loss config (fixed or trailing) |
| `TakeProfitLevel` | Scaled exit target (price + close fraction) |
| `NewListingAlert` | New perp detected on Hyperliquid |

**Enums:** Direction, TradeAction, Timeframe, ExecutionMode, AlertSeverity, AssetClass, AnomalyType, ChallengeType, RedTeamRecommendation, MarketRegime, ICDecision, ChallengeCategory, PositionAction

---

## Risk Parameters (config/settings.py)

```
max_position_pct = 0.02            # 2% NAV per position
max_gross_exposure = 3.0           # 300% leverage cap
max_net_exposure = 1.0             # 100% NAV net directional
max_single_asset_class_pct = 0.40  # 40% NAV per asset class
max_correlation_threshold = 0.80   # 14d Pearson — reject if exceeded
max_daily_drawdown = 0.10          # -10% daily → reduce new positions 50%
circuit_breaker_drawdown = 0.15    # -15% daily → reject all + flatten
weekly_drawdown_pause = 0.20       # -20% weekly → pause 24h
monthly_drawdown_review = 0.25     # -25% monthly → review mode

# Anomaly detection
volume_spike_multiplier = 5.0      # Trigger at 5× rolling average
price_zscore_threshold = 3.0       # 3σ move in 5 minutes
book_imbalance_threshold = 10.0    # 10:1 bid/ask ratio
funding_rate_extreme = 0.001       # ±0.1% per 8h

# Hedging
hedge_net_exposure_threshold = 0.50
hedge_tail_risk_pct = 0.20

# Execution
paper_trade_mode = True
slippage_threshold_bps = 50
defensive_position_reduction = 0.30
defensive_pause_minutes = 15
```

---

## File Structure

```
C:\apex\
├── .env / .env.example
├── CLAUDE.md                          ← THIS FILE
├── requirements.txt
├── main.py                            # Entry point — wires all agents
│
├── config/
│   ├── settings.py                    # Pydantic BaseSettings (all env + risk params)
│   └── agent_registry.py             # 24 agents: id, type, weight, status
│
├── core/
│   ├── models.py                      # All shared Pydantic v2 domain models
│   ├── message_bus.py                 # Redis Streams pub/sub (publish_to/subscribe_to)
│   ├── database.py                    # Async SQLAlchemy ORM for TimescaleDB
│   └── logger.py                      # Structured JSON logging with agent_id
│
├── agents/
│   ├── base_agent.py                  # Abstract base: start/stop/process lifecycle
│   ├── ingestion/
│   │   ├── market_data.py             # ✅ HL WebSocket → OHLCV (6 TFs) → Redis + DB
│   │   ├── platform_specialist.py     # ✅ 5 concurrent loops: assets/fees/margin/bridge/advisory
│   │   ├── onchain_intel.py          # ✅ 6 loops: TVL/DEX/stablecoins/trending/fear-greed/unlocks
│   │   ├── macro_feed.py             # ✅ 4 loops: FRED/yfinance/fear-greed/calendar
│   │   └── sentiment.py              # ✅ 4 loops: Reddit/CryptoPanic/Trends/LunarCrush
│   ├── risk/
│   │   ├── guardian.py                # ✅ 7-check gate: size/gross/net/corr/dd/weekly/asset-class
│   │   ├── anomaly.py                 # ✅ 6 detectors: vol/price/book/funding/corr/latency
│   │   └── hedging.py                 # ✅ Auto-hedge + tail-risk (VIX proxy)
│   ├── analysis/
│   │   ├── technical.py              # ✅ Multi-TF TA (RSI, MACD, BB, ADX, EMAs, S/R)
│   │   └── funding_arb.py           # ✅ Cross-exchange funding rate arb scanner
│   ├── fundamental/
│   │   └── valuation.py             # ✅ NVT, TVL ratio, CoinGecko + DefiLlama
│   ├── on_chain/
│   │   └── flow.py                  # ✅ TVL momentum, stablecoin capital flows
│   ├── sentiment/
│   │   ├── social.py                # ✅ CoinGecko votes + NewsAPI keyword scoring
│   │   └── funding.py               # ✅ Contrarian funding/OI sentiment
│   ├── macro/
│   │   └── regime.py                # ✅ FRED → risk_on/risk_off/neutral classification
│   ├── decision/
│   │   ├── cio.py                  # ✅ CIO: Claude reasoning, IC debates, portfolio review
│   │   ├── signal_aggregator.py    # ✅ Real-time signal matrix for CIO
│   │   ├── red_team.py             # ✅ LLM adversarial challenger (Claude Sonnet)
│   │   ├── portfolio_allocator.py  # ✅ Kelly criterion sizing (Half-Kelly)
│   │   └── regime_classifier.py    # ✅ HMM 5-state regime detection
│   ├── red_team/
│   │   └── challenger.py            # ✅ 6 heuristic challenges on decisions:pending
│   ├── meta/
│   │   └── orchestrator.py          # ✅ Signal aggregation, consensus voting, dispatch
│   └── execution/
│       ├── engine.py                  # ✅ MARKET/LIMIT/TWAP/ICEBERG, paper-trade default
│       └── position_manager.py        # ✅ Trailing stops, scaled TPs, circuit breaker
│
├── tests/                             # 411 tests (pytest + pytest-asyncio)
│   ├── conftest.py                   # MockMessageBus, FakeSession, model factories
│   └── test_*.py                     # Per-agent + lifecycle + model tests
│
├── data/
│   └── schema.sql                     # TimescaleDB DDL — 11 hypertables
│
└── docker/
    └── docker-compose.yml             # Redis 7 + TimescaleDB (pg16)
```

---

## Architecture Patterns

- **Agent lifecycle:** Inherit `BaseAgent`, implement `process()`. Override `start()` for concurrent sub-tasks via `asyncio.create_task()`.
- **HL SDK is sync:** Always wrap with `asyncio.get_running_loop().run_in_executor()`.
- **WebSocket:** Uses `websockets` library (not SDK built-in) for full async + reconnection.
- **HL REST:** POST to `/info` with `{"type": "..."}` payloads via `httpx`.
- **Direct wiring:** ExecutionEngine and PositionManager take `platform_specialist=` constructor arg.
- **Loose coupling:** Risk agents communicate via streams only — no direct references.
- **Correlation:** RiskGuardian uses `_PriceHistory` with hourly sampling for 14d Pearson.
- **Anomaly stats:** `_RollingStats` (Welford's online algorithm) for efficient mean/variance.
- **Data ingestion:** OnChainIntelligence, MacroFeed, SentimentScraper — centralised external data with per-source exponential backoff (1s base, 60s cap).
- **Sync libs in async:** `yfinance`, `pytrends`, and `hmmlearn` wrapped via `loop.run_in_executor(None, sync_fn)`.
- **NLP:** VADER `SentimentIntensityAnalyzer` for Reddit + news headline scoring.
- **CIO system:** Claude-powered reasoning (configurable model). 5-phase IC debates. RedTeamStrategist uses separate model context from CIO (adversarial). PortfolioAllocator uses Kelly criterion (Half-Kelly default). RegimeClassifier uses Gaussian HMM (hmmlearn).
- **CIO wiring:** CIO takes `red_team=` and `allocator=` constructor args for direct method calls during IC debates.
- **IC debate flow:** Thesis → Expert Review (signal matrix) → Red Team Challenge → CIO Deliberation → Position Sizing. 3 consecutive RT overrides → human review required.
- **CIO cannot override:** RiskGuardian or PlatformSpecialist. All CIO decisions go through the standard risk pipeline via `decisions:pending`.

---

## Gotchas

- Windows: `add_signal_handler` not supported for SIGTERM — wrapped in try/except
- `main.py`: `agents` var must be initialized before try block (UnboundLocalError in finally)
- `_disconnect_infra` must tolerate partial startup (try/except around each close)
- hyperliquid-python-sdk 0.22.0 — imports as `hyperliquid.*`
- TimescaleDB hypertables need `data/schema.sql` run separately — SQLAlchemy can't issue `create_hypertable()`
- numpy already installed via pandas dependency — used in risk agents

---

## Commands

```bash
docker compose -f docker/docker-compose.yml up -d   # Start Redis + TimescaleDB
pip install -r requirements.txt
python main.py                                        # 24 agents READY
python -m pytest tests/ -v                             # 411 tests
```
