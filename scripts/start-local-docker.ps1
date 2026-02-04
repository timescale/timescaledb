#requires -Version 5.1
$ErrorActionPreference = "Stop"

# --- Configuration ---
$IMAGE_NAME     = "timescale/timescaledb-ha:pg18"
$CONTAINER_NAME = "timescaledb-ha-pg18-quickstart"
$DB_PASSWORD    = "password"
$DB_PORT        = 6543
$VOLUME_NAME    = "timescaledb_data"

# Catch-all so users see the error and return to command prompt
trap {
  Write-Host "" 
  Write-Host ("[ERROR] " + $_.Exception.Message) -ForegroundColor Red
  return  # Return to command prompt instead of exiting
}

function Fail($msg) {
  throw $msg  # IMPORTANT: do NOT use exit here; it kills the host instantly when invoked via iwr|iex
}
function Info($msg)    { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Success($msg) { Write-Host "[SUCCESS] $msg" -ForegroundColor Green }

function Test-Command($name) {
  return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

Clear-Host
Write-Host ""
Write-Host "  _____ _                              __      ____  ____  " -ForegroundColor Yellow
Write-Host " |_   _(_)_ __ ___   ___  ___  ___ __ _| | ___|  _ \| __ ) " -ForegroundColor Yellow
Write-Host "   | | | | '_ \` _ \ / _ \/ __|/ __/ _\` | |/ _ \ | | |  _ \ " -ForegroundColor Yellow
Write-Host "   | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |_| | |_) |" -ForegroundColor Yellow
Write-Host "   |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|____/|____/ " -ForegroundColor Yellow
Write-Host ""

Write-Host "1. System Check" -ForegroundColor White

if (-not (Test-Command docker)) {
  Fail "Docker is not found. Install Docker Desktop first.`nDownload Docker: https://www.docker.com/get-started/"
}
Success "Docker found"

try {
  docker info *> $null
} catch {
  Fail "Docker is installed but not running. Start Docker Desktop and try again.`nDownload Docker: https://www.docker.com/get-started/"
}
Success "Docker is running"

# Cleanup old container
$existing = (docker ps -aq -f "name=^/$CONTAINER_NAME$").Trim()
if ($existing) {
  Info "Found existing container. Removing it .."
  docker rm -f $CONTAINER_NAME *> $null
  Success "Removed existing container"
}

Write-Host ""
Write-Host "2. Deployment" -ForegroundColor White

Info "Pulling image ($IMAGE_NAME) .."
docker pull $IMAGE_NAME | Out-Host
Success "Image pulled ($IMAGE_NAME)"

Info "Starting TimescaleDB on port $DB_PORT .."
docker run -d `
  --name $CONTAINER_NAME `
  -p "$DB_PORT`:5432" `
  -e "POSTGRES_PASSWORD=$DB_PASSWORD" `
  -v "$VOLUME_NAME`:/home/postgres/pgdata/data" `
  $IMAGE_NAME *> $null
Success "Container started ($CONTAINER_NAME)"

# Wait for readiness
Info "Waiting for database to accept connections .."
$retries = 30
$ready = $false
for ($i=0; $i -lt $retries; $i++) {
  try {
    docker exec $CONTAINER_NAME pg_isready -U postgres *> $null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
  } catch {}
  Start-Sleep -Seconds 1
}

if (-not $ready) {
  Fail "Database failed to start in time. Check logs with: docker logs $CONTAINER_NAME"
}
Success "Database is ready!"

# Get versions without host psql: use psql inside container
$tsdbVersion = (docker exec $CONTAINER_NAME psql -U postgres -t -A -c "select extversion from pg_extension where extname='timescaledb';" 2>$null).Trim()
$pgVersion   = (docker exec $CONTAINER_NAME psql -U postgres -t -A -c "select split_part(version(),' ',2);" 2>$null).Trim()

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║             🚀 SETUP COMPLETED SUCCESSFULLY              ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

Write-Host ("   Postgres:    {0}" -f ($(if ($pgVersion) { $pgVersion } else { "(unknown)" })))
Write-Host ("   TimescaleDB: {0}" -f ($(if ($tsdbVersion) { $tsdbVersion } else { "(unknown)" })))
Write-Host ""
Write-Host "   Container:   $CONTAINER_NAME"
Write-Host "   Port:        $DB_PORT"
Write-Host "   User:        postgres"
Write-Host "   Password:    $DB_PASSWORD"
Write-Host ""
Write-Host "   Connect:"
Write-Host "     psql \"postgres://postgres:$DB_PASSWORD@localhost:$DB_PORT/postgres\""
Write-Host ""
Write-Host "   Getting Started:"
Write-Host "    * Quick start:    https://tsdb.co/quick-start"
Write-Host "    * NYC taxis:      https://tsdb.co/quick-start-nyc-taxis"
Write-Host "    * S&P 500 stocks: https://tsdb.co/quick-start-sp500-stocks"
Write-Host "    * Sensor devices: https://tsdb.co/quick-start-sensors"
Write-Host ""

# Success: optionally pause if launched in a transient window
# Pause-OnExit 0
