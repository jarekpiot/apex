#!/usr/bin/env bash
##############################################################################
# APEX — Bootstrap script (Linux / macOS)
# Sets up the environment, starts infrastructure, and launches APEX.
##############################################################################
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[APEX]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --------------------------------------------------------------------------
# 1. Check Python
# --------------------------------------------------------------------------
info "Checking Python version..."
if ! command -v python3 &>/dev/null; then
    error "Python 3 not found. Install Python 3.13+."
fi

PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [[ "$PY_MAJOR" -lt 3 ]] || [[ "$PY_MAJOR" -eq 3 && "$PY_MINOR" -lt 13 ]]; then
    error "Python 3.13+ required (found $PY_VERSION)."
fi
info "Python $PY_VERSION OK."

# --------------------------------------------------------------------------
# 2. Virtual environment
# --------------------------------------------------------------------------
if [[ ! -d ".venv" ]]; then
    info "Creating virtual environment..."
    python3 -m venv .venv
fi

info "Activating virtual environment..."
source .venv/bin/activate

info "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# --------------------------------------------------------------------------
# 3. Check .env
# --------------------------------------------------------------------------
if [[ ! -f ".env" ]]; then
    if [[ -f ".env.example" ]]; then
        warn ".env not found — copying from .env.example"
        cp .env.example .env
        warn "Edit .env with your API keys before going live."
    else
        warn "No .env file found. APEX will use defaults (paper mode)."
    fi
fi

# --------------------------------------------------------------------------
# 4. Start Docker infrastructure
# --------------------------------------------------------------------------
info "Starting Docker containers (Redis + TimescaleDB + Prometheus + Grafana)..."
if ! command -v docker &>/dev/null; then
    error "Docker not found. Install Docker Desktop or Docker Engine."
fi

docker compose -f docker/docker-compose.yml up -d

# --------------------------------------------------------------------------
# 5. Wait for health checks
# --------------------------------------------------------------------------
info "Waiting for Redis..."
for i in $(seq 1 30); do
    if docker exec apex-redis redis-cli ping 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 1
done
docker exec apex-redis redis-cli ping | grep -q PONG || error "Redis not ready after 30s."
info "Redis OK."

info "Waiting for TimescaleDB..."
for i in $(seq 1 30); do
    if docker exec apex-timescaledb pg_isready -U apex 2>/dev/null | grep -q "accepting"; then
        break
    fi
    sleep 1
done
docker exec apex-timescaledb pg_isready -U apex | grep -q "accepting" || error "TimescaleDB not ready after 30s."
info "TimescaleDB OK."

# --------------------------------------------------------------------------
# 6. Launch APEX
# --------------------------------------------------------------------------
info ""
info "============================================"
info "  APEX is starting..."
info "  Dashboard:   http://localhost:8000"
info "  Grafana:     http://localhost:3000  (admin / apex)"
info "  Prometheus:  http://localhost:9090"
info "============================================"
info ""

python main.py
