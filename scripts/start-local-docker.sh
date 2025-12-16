#!/bin/sh
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

# Colors
RESET="\033[0m"
BOLD="\033[1m"
COLOR_GREEN="\033[32m"
COLOR_BLUE="\033[34m"
COLOR_RED="\033[31m"
COLOR_CYAN="\033[36m"
COLOR_YELLOW="\033[33m"

# --- Formatting Helpers ---
# Hide cursor for cleaner UI, restore on exit
cleanup() {
    tput cnorm # restore cursor
}
trap cleanup EXIT INT TERM

log() { printf "%b[INFO]%b %s\n" "${COLOR_BLUE}" "${RESET}" "$1"; }
success() { printf "%b[SUCCESS]%b %s\n" "${COLOR_GREEN}" "${RESET}" "$1"; }
error() { printf "%b[ERROR]%b %s\n" "${COLOR_RED}" "${RESET}" "$1"; exit 1; }

# --- Spinner Function ---
# Usage: Run command in background, then call: spinner $! "Loading text..."
spinner() {
    local pid=$1
    local text=$2
    local delay=0.1
    local spinstr='|/-\'
    
    # Hide cursor
    tput civis

    printf "%s  " "$text"

    while kill -0 "$pid" 2>/dev/null; do
        # Loop through spin string characters
        # Note: POSIX sh handles string slicing differently, so we use a simpler approach
        printf "\b%s" "|"
        sleep $delay
        printf "\b%s" "/"
        sleep $delay
        printf "\b%s" "-"
        sleep $delay
        printf "\b%s" "\\"
        sleep $delay
    done
    
    # Clear spinner character
    printf "\b " 
    # Restore cursor
    tput cnorm
}

header() {
    clear
    echo ""
    printf "%b  _____ _                              __      ____  ____  %b\n" "${COLOR_YELLOW}" "${RESET}"
    printf "%b |_   _(_)_ __ ___   ___  ___  ___ __ _| | ___|  _ \| __ ) %b\n" "${COLOR_YELLOW}" "${RESET}"
    printf "%b   | | | | '_ \` _ \ / _ \/ __|/ __/ _\` | |/ _ \ | | |  _ \ %b\n" "${COLOR_YELLOW}" "${RESET}"
    printf "%b   | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |_| | |_) |%b\n" "${COLOR_YELLOW}" "${RESET}"
    printf "%b   |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|____/|____/ %b\n" "${COLOR_YELLOW}" "${RESET}"
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
printf "%b1. System Check%b\n" "${BOLD}" "${RESET}"

if ! command -v docker > /dev/null 2>&1; then
    error "${COLOR_RED}✖${RESET} Docker is not found. Please install Docker Desktop (Windows/Mac) or Docker Engine (Linux) first."
else
    printf "%b✔%b Docker found\n" "${COLOR_GREEN}" "${RESET}"
fi

if ! docker info > /dev/null 2>&1; then
    error "${COLOR_RED}✖${RESET} Docker is installed but not running. Please start Docker and try again."
else
    printf "%b✔%b Docker is running\n" "${COLOR_GREEN}" "${RESET}"
fi

# Cleanup Old Containers
if [ "$(docker ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
    printf "%b✔%b Found existing container. Removing it ..\n" "${COLOR_GREEN}" "${RESET}"
    docker rm -f $CONTAINER_NAME > /dev/null
fi

# --- 2. Pull and Run ---
echo ""
printf "%b2. Deployment%b\n" "${BOLD}" "${RESET}"

# Start pull in background
printf "%b::%b " "${COLOR_CYAN}" "${RESET}"
docker pull -q $IMAGE_NAME > /dev/null 2>&1 &
PID=$!

# Run spinner attached to that PID
spinner $PID "Pulling image ($IMAGE_NAME) .."

# Wait for PID to capture exit code
wait $PID
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    printf "\r%b::%b Image pulled ($IMAGE_NAME)                 \n" "${COLOR_GREEN}" "${RESET}"
    printf "%b✔%b Success\n" "${COLOR_GREEN}" "${RESET}"
else
    printf "\r%b✖%b Failed to pull image.\n" "${COLOR_RED}" "${RESET}"
    exit 1
fi

printf "%b✔%b Starting TimescaleDB on port $DB_PORT\n" "${COLOR_GREEN}" "${RESET}"

docker run -d \
    --name "$CONTAINER_NAME" \
    -p ${DB_PORT}:5432 \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -v timescaledb_data:/home/postgres/pgdata/data \
    "$IMAGE_NAME" > /dev/null

# --- 4. Wait for Healthcheck ---
printf "%b::%b Waiting for database to accept connections ..  " "${COLOR_CYAN}" "${RESET}"

RETRIES=30
SUCCESS=false

# We use a custom loop here to animate the spinner while sleeping
tput civis # Hide cursor
while [ $RETRIES -gt 0 ]; do
    # Check DB
    if docker exec $CONTAINER_NAME pg_isready -U postgres > /dev/null 2>&1; then
        SUCCESS=true
        break
    fi
    
    # Animate spinner for 1 second (4 * 0.25s)
    for i in 1 2 3 4; do
        printf "\b|" ; sleep 0.1
        printf "\b/" ; sleep 0.1
        printf "\b-" ; sleep 0.1
        printf "\b\\" ; sleep 0.1
    done
    
    RETRIES=$((RETRIES-1))
done
tput cnorm # Restore cursor

if [ $RETRIES -eq 0 ]; then
    printf "\n"
    error "{COLOR_RED}✖${RESET} Database failed to start in time. Check logs with: docker logs $CONTAINER_NAME"
fi

# --- 5. Success ---
if [ "$SUCCESS" = true ]; then
    printf "\r%b::%b Database is ready!                              \n" "${COLOR_GREEN}" "${RESET}"
    echo ""

    TSDB_VERSION=$(get_timescaledb_version)
    POSTGRES_VERSION=$(get_postgres_version)

    printf "${COLOR_GREEN}╔══════════════════════════════════════════════════════════╗${RESET}"
    echo ""
    printf "${COLOR_GREEN}║             🚀 SETUP COMPLETED SUCCESSFULLY              ║${RESET}"
    echo ""
    printf "${COLOR_GREEN}╚══════════════════════════════════════════════════════════╝${RESET}"
    echo ""
    printf "   %bPostgres:%b    $POSTGRES_VERSION\n" "${BOLD}" "${RESET}"
    printf "   %bTimescaleDB:%b $TSDB_VERSION\n" "${BOLD}" "${RESET}"
    echo ""
    printf "   %bContainer:%b   $CONTAINER_NAME\n" "${BOLD}" "${RESET}"
    printf "   %bPort:%b        $DB_PORT\n" "${BOLD}" "${RESET}"
    printf "   %bPassword:%b    $DB_PASSWORD\n" "${BOLD}" "${RESET}"
    echo ""
    printf "%b   Connect:%b\n" "${BOLD}" "${RESET}"
    echo "     psql \"postgres://postgres:$DB_PASSWORD@localhost:$DB_PORT/postgres\""
    echo ""
    printf "%b   Getting Started:%b\n" "${BOLD}" "${RESET}"
    echo "    * Quick start:    https://tsdb.co/quick-start"
    echo "    * NYC taxis:      https://tsdb.co/quick-start-nyc-taxis"
    echo "    * S&P 500 stocks: https://tsdb.co/quick-start-sp500-stocks"
    echo "    * Senor devices:  https://tsdb.co/quick-start-sensors"
    echo ""
else
    printf "\r%b✖%b Database timed out.\n" "${COLOR_RED}" "${RESET}"
    echo "   Check logs with: docker logs $CONTAINER_NAME"
    exit 1
fi
