#!/usr/bin/env bash
# fetch_iers_eop.sh — Download IERS Bulletin A (finals2000A.all) for EOP refresh
#
# Usage:
#   ./scripts/fetch_iers_eop.sh [output_dir]
#
# Default output: /var/lib/postgresql/eop/finals2000A.all
#
# Exit codes:
#   0 — success
#   1 — download failed (both primary and mirror)
#   2 — downloaded file too small (likely corrupt or empty)
#   3 — output directory not writable
#
# Designed to be called by cron or systemd timer:
#   0 3 * * * /path/to/fetch_iers_eop.sh /var/lib/postgresql/eop

set -euo pipefail

# --- Configuration ---
PRIMARY_URL="https://maia.usno.navy.mil/ser7/finals2000A.all"
MIRROR_URL="https://datacenter.iers.org/data/latestVersion/finals2000A.all"
MIN_FILE_SIZE=500000  # finals2000A is typically ~600KB+
FILENAME="finals2000A.all"
DEFAULT_OUTPUT_DIR="/var/lib/postgresql/eop"

# --- Parse arguments ---
OUTPUT_DIR="${1:-$DEFAULT_OUTPUT_DIR}"
OUTPUT_FILE="${OUTPUT_DIR}/${FILENAME}"
TMP_FILE="${OUTPUT_FILE}.tmp.$$"

# --- Validate output directory ---
if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir -p "$OUTPUT_DIR" 2>/dev/null || {
        echo "ERROR: Cannot create output directory: $OUTPUT_DIR" >&2
        exit 3
    }
fi

if [ ! -w "$OUTPUT_DIR" ]; then
    echo "ERROR: Output directory not writable: $OUTPUT_DIR" >&2
    exit 3
fi

# --- Cleanup on exit ---
cleanup() {
    rm -f "$TMP_FILE"
}
trap cleanup EXIT

# --- Download with fallback ---
download_ok=false

for url in "$PRIMARY_URL" "$MIRROR_URL"; do
    echo "Downloading from: $url"
    if curl -sS --fail --max-time 120 -o "$TMP_FILE" "$url" 2>/dev/null; then
        download_ok=true
        echo "Download succeeded from: $url"
        break
    else
        echo "WARN: Download failed from: $url" >&2
        rm -f "$TMP_FILE"
    fi
done

if [ "$download_ok" = false ]; then
    echo "ERROR: Download failed from all sources" >&2
    exit 1
fi

# --- Validate file size ---
file_size=$(stat -c%s "$TMP_FILE" 2>/dev/null || stat -f%z "$TMP_FILE" 2>/dev/null || echo 0)

if [ "$file_size" -lt "$MIN_FILE_SIZE" ]; then
    echo "ERROR: Downloaded file too small: ${file_size} bytes (minimum: ${MIN_FILE_SIZE})" >&2
    exit 2
fi

# --- Atomic move ---
mv -f "$TMP_FILE" "$OUTPUT_FILE"

echo "OK: ${OUTPUT_FILE} (${file_size} bytes)"
exit 0
