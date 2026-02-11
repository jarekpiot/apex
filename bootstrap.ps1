##############################################################################
# APEX - Bootstrap script (Windows PowerShell)
# Sets up the environment, starts infrastructure, and launches APEX.
##############################################################################

$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "[APEX] $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Error($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red; exit 1 }

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# --------------------------------------------------------------------------
# 1. Check Python
# --------------------------------------------------------------------------
Info "Checking Python version..."
try {
    $pyVersion = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    $parts = $pyVersion.Split(".")
    $major = [int]$parts[0]
    $minor = [int]$parts[1]
    if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 13)) {
        Error "Python 3.13+ required (found $pyVersion)."
    }
    Info "Python $pyVersion OK."
} catch {
    Error "Python not found. Install Python 3.13+."
}

# --------------------------------------------------------------------------
# 2. Virtual environment
# --------------------------------------------------------------------------
if (-not (Test-Path ".venv")) {
    Info "Creating virtual environment..."
    python -m venv .venv
}

Info "Activating virtual environment..."
& .\.venv\Scripts\Activate.ps1

Info "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# --------------------------------------------------------------------------
# 3. Check .env
# --------------------------------------------------------------------------
if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Warn ".env not found - copying from .env.example"
        Copy-Item .env.example .env
        Warn "Edit .env with your API keys before going live."
    } else {
        Warn "No .env file found. APEX will use defaults (paper mode)."
    }
}

# --------------------------------------------------------------------------
# 4. Start Docker infrastructure
# --------------------------------------------------------------------------
Info "Starting Docker containers (Redis + TimescaleDB + Prometheus + Grafana)..."
try {
    docker compose -f docker/docker-compose.yml up -d
} catch {
    Error "Docker not found or failed. Install Docker Desktop."
}

# --------------------------------------------------------------------------
# 5. Wait for health checks
# --------------------------------------------------------------------------
Info "Waiting for Redis..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $result = docker exec apex-redis redis-cli ping 2>$null
        if ($result -match "PONG") { $ready = $true; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $ready) { Error "Redis not ready after 30s." }
Info "Redis OK."

Info "Waiting for TimescaleDB..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $result = docker exec apex-timescaledb pg_isready -U apex 2>$null
        if ($result -match "accepting") { $ready = $true; break }
    } catch {}
    Start-Sleep -Seconds 1
}
if (-not $ready) { Error "TimescaleDB not ready after 30s." }
Info "TimescaleDB OK."

# --------------------------------------------------------------------------
# 6. Launch APEX
# --------------------------------------------------------------------------
Info ""
Info "============================================"
Info "  APEX is starting..."
Info "  Dashboard:   http://localhost:8000"
Info "  Grafana:     http://localhost:3000  (admin / apex)"
Info "  Prometheus:  http://localhost:9090"
Info "============================================"
Info ""

python main.py
