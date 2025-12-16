#!/bin/bash
set -e

#
# Getting Started script with Docker
# 
# URL: https://tsdb.co/start-local 
#

# --- Configuration ---
IMAGE_NAME="timescale/timescaledb-ha:pg18" 
CONTAINER_NAME="timescaledb-ha-pg18-quickstart"
DB_PASSWORD="password"
DB_PORT="6543"

RESET="\033[0m"
BOLD="\033[1m"
COLOR_GREEN="\033[32m"
COLOR_BLUE="\033[34m"
COLOR_RED="\033[31m"
COLOR_CYAN="\033[36m"
COLOR_YELLOW="\033[33m"
COLOR_MAGENTA="\033[35m"

log() { echo -e "${COLOR_BLUE}[INFO]${RESET} $1"; }
success() { echo -e "${COLOR_GREEN}[SUCCESS]${RESET} $1"; }
error() { echo -e "${COLOR_RED}[ERROR]${RESET} $1"; exit 1; }

header() {
    clear
    echo ""
    echo -e "${COLOR_YELLOW}  _____ _                              __      ____  ____  ${RESET}"
    echo -e "${COLOR_YELLOW} |_   _(_)_ __ ___   ___  ___  ___ __ _| | ___|  _ \| __ ) ${RESET}"
    echo -e "${COLOR_YELLOW}   | | | | '_ \` _ \ / _ \/ __|/ __/ _\` | |/ _ \ | | |  _ \ ${RESET}"
    echo -e "${COLOR_YELLOW}   | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |_| | |_) |${RESET}"
    echo -e "${COLOR_YELLOW}   |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|____/|____/ ${RESET}"
    echo ""
}

get_timescaledb_version() {
    psql "postgres://postgres:$DB_PASSWORD@localhost:$DB_PORT/postgres" \
        -c "select extversion from pg_extension where extname = 'timescaledb';" \
        | awk 'NR==3 {print $1}'
}

get_postgres_version() {
    psql "postgres://postgres:$DB_PASSWORD@localhost:$DB_PORT/postgres" \
        -t \
        -c "select version();" \
        | awk '{print $2}'
}

# --------------------------------------------------------------- #

header

# --- 1. Check for Docker ---
echo -e "${BOLD}1. System Check${RESET}"
if ! command -v docker &> /dev/null; then
    error "${COLOR_RED}✖${RESET} Docker is not found. Please install Docker Desktop (Windows/Mac) or Docker Engine (Linux) first."
else
    echo -e "${COLOR_GREEN}✔${RESET} Docker found"
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    error "${COLOR_RED}✖${RESET} Docker is installed but not running. Please start Docker and try again."
else
    echo -e "${COLOR_GREEN}✔${RESET} Docker is running"
fi

# Cleanup Old Containers ---
if [ "$(docker ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
    echo -e "${COLOR_GREEN}✔${RESET} Found existing container named '${CONTAINER_NAME}'. Removing it .."
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# --- 2. Pull and Run ---
echo -e ""
echo -e "${BOLD}2. Deployment${RESET}"
echo -ne "${COLOR_CYAN}::${RESET} Pulling image ($IMAGE_NAME) .. ⏳"
if docker pull -q $IMAGE_NAME > /dev/null 2>&1; then
    echo -e "\r${COLOR_GREEN}✔${RESET} Image pulled ($IMAGE_NAME)        "
else
    echo -e "\r${COLOR_RED}✖${RESET} Failed to pull image. Check your internet or image name."
    exit 1
fi

echo -e "\r${COLOR_GREEN}✔${RESET} Starting TimescaleDB on port $DB_PORT"

# Run Command:
# -d: Detached mode
# --name: Name the container
# -p: Map port 5432
# -e: Set admin password
# -v: Create a persistent volume so data survives restart
docker run -d \
    --name "$CONTAINER_NAME" \
    -p ${DB_PORT}:5432 \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -v timescaledb_data:/home/postgres/pgdata/data \
    "$IMAGE_NAME" > /dev/null

# --- 4. Wait for Healthcheck ---
echo -ne "${COLOR_CYAN}::${RESET} Waiting for database to accept connections .. ${COLOR_YELLOW}⏳${RESET}"

# Loop until pg_isready returns 0 inside the container
RETRIES=30
SUCCESS=false
while [ $RETRIES -gt 0 ]; do
    if docker exec $CONTAINER_NAME pg_isready -U postgres &> /dev/null; then
        SUCCESS=true
        break
    fi
    sleep 1
    ((RETRIES--))
done
 
if [ $RETRIES -eq 0 ]; then
    error "{COLOR_RED}✖${RESET} Database failed to start in time. Check logs with: docker logs $CONTAINER_NAME"
fi

# --- 5. Success ---
if [ "$SUCCESS" = true ]; then
    echo -e "\r${COLOR_GREEN}✔${RESET} TimescaleDB is ready                                 "
    echo ""

    TSDB_VERSION=$(get_timescaledb_version)
    POSTGRES_VERSION=$(get_postgres_version)

    # --- Final Summary Box ---
    echo -e "${COLOR_GREEN}╔══════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${COLOR_GREEN}║             🚀 SETUP COMPLETED SUCCESSFULLY              ║${RESET}"
    echo -e "${COLOR_GREEN}╚══════════════════════════════════════════════════════════╝${RESET}"
    echo ""
    echo -e "   ${BOLD}Postgres:${RESET}    $POSTGRES_VERSION"
    echo -e "   ${BOLD}TimescaleDB:${RESET} $TSDB_VERSION"
    echo ""
    echo -e "   ${BOLD}Container:${RESET}   $CONTAINER_NAME"
    echo -e "   ${BOLD}Port:${RESET}        $DB_PORT"
    echo -e "   ${BOLD}Password:${RESET}    $DB_PASSWORD"
    echo ""
    echo -e "${BOLD}   Connect:${RESET}"
    echo -e "     psql \"postgres://postgres:$DB_PASSWORD@localhost:$DB_PORT/postgres\""
    echo ""
    echo -e "${BOLD}   Getting Started:${RESET}"
    echo -e "    * Quick start:    https://tsdb.co/quick-start"
    echo -e "    * NYC taxis:      https://tsdb.co/quick-start-nyc-taxis"
    echo -e "    * S&P 500 stocks: https://tsdb.co/quick-start-sp500-stocks"
    echo -e "    * Senor devices:  https://tsdb.co/quick-start-sensors"
    echo ""
else
    echo -e "\r${COLOR_RED}✖${RESET} Database timed out."
    echo -e "   Check logs with: docker logs $CONTAINER_NAME"
    exit 1
fi