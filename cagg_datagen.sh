#!/usr/bin/env bash
#
# CAgg Data Generation Script
#
# Creates a TimescaleDB hypertable with a configurable sensor pool and
# generates data using a two-pass approach (seed + fill) to guarantee
# a minimum number of distinct sensors per chunk.
#
# Usage:
#   ./cagg_datagen.sh --all          # Run setup + generate
#   ./cagg_datagen.sh --setup        # Phase 1: Create schema
#   ./cagg_datagen.sh --generate     # Phase 2: Parallel data generation
#
set -euo pipefail

###############################################################################
# Configurable Parameters
###############################################################################
CONNSTR="${CONNSTR:-postgres://tsdbadmin:tx6c64410vcro0m1@wqk1vtuoav.p9tp0soz0w.tsdb.cloud.timescale.com:35968/tsdb?sslmode=require}"
NUM_SENSORS="${NUM_SENSORS:-22000000}"        # Real: ~22.5M
NUM_DEVICES="${NUM_DEVICES:-7000000}"         # Real: ~7.3M (ratio ~3 sensors/device)
NUM_CHANNELS="${NUM_CHANNELS:-4}"
TARGET_SIZE_GB="${TARGET_SIZE_GB:-1000}"       # Real: ~6400 GB uncompressed
DATA_SPAN_DAYS="${DATA_SPAN_DAYS:-365}"       # Real: ~385 days
CHUNK_INTERVAL="${CHUNK_INTERVAL:-5 hours}"   # Matches customer
SENSORS_PER_CHUNK="${SENSORS_PER_CHUNK:-1000000}" # Distinct sensors seeded per chunk (max: NUM_SENSORS)
SKIP_SEED="${SKIP_SEED:-false}"               # Set to "true" to skip seed pass (fill only)
BATCH_SIZE="${BATCH_SIZE:-1000000}"
PARALLEL_JOBS="${PARALLEL_JOBS:-4}"

###############################################################################
# Helpers
###############################################################################
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }
stat()  { echo -e "${CYAN}[STAT]${NC}  $*"; }

run_sql() {
	psql "$CONNSTR" -X -q --set ON_ERROR_STOP=on "$@"
}

run_sql_val() {
	psql "$CONNSTR" -X -tAq --set ON_ERROR_STOP=on "$@"
}

elapsed_since() {
	local start_ts=$1
	local now_ts
	now_ts=$(date +%s)
	echo $((now_ts - start_ts))
}

###############################################################################
# Phase 1: Setup — table, hypertable, sensor pool
###############################################################################
phase_setup() {
	info "=== Phase 1: Setup ==="

	info "Creating readings table..."
	run_sql <<-'SQL'
		DROP TABLE IF EXISTS readings CASCADE;
		CREATE TABLE readings (
			timestamp    TIMESTAMPTZ    NOT NULL,
			sensor_uuid  UUID           NOT NULL,
			device_uuid  UUID           NOT NULL,
			channel      SMALLINT       NOT NULL,
			value        DOUBLE PRECISION,
			min_value    DOUBLE PRECISION,
			max_value    DOUBLE PRECISION,
			avg_value    DOUBLE PRECISION,
			sample_count INTEGER
		);
	SQL

	info "Converting to hypertable (chunk_interval = '${CHUNK_INTERVAL}')..."
	run_sql -c "SELECT create_hypertable('readings', 'timestamp',
		chunk_time_interval => INTERVAL '${CHUNK_INTERVAL}');"

	info "Creating sensor pool table (_sim_sensor_pool)..."
	info "  Generating ${NUM_DEVICES} devices and ${NUM_SENSORS} sensors (~3 sensors/device)..."
	run_sql <<-SQL
		DROP TABLE IF EXISTS _sim_sensor_pool;
		CREATE TABLE _sim_sensor_pool (
			id           SERIAL PRIMARY KEY,
			sensor_uuid  UUID NOT NULL,
			device_uuid  UUID NOT NULL
		);

		CREATE TEMP TABLE _tmp_devices (id INT, device_uuid UUID);
		INSERT INTO _tmp_devices (id, device_uuid)
		SELECT g, gen_random_uuid()
		FROM generate_series(1, ${NUM_DEVICES}) g;

		INSERT INTO _sim_sensor_pool (sensor_uuid, device_uuid)
		SELECT gen_random_uuid(), d.device_uuid
		FROM generate_series(1, ${NUM_SENSORS}) AS s(i)
		JOIN _tmp_devices d ON d.id = ((s.i - 1) % ${NUM_DEVICES}) + 1;

		DROP TABLE _tmp_devices;
	SQL

	local pool_count
	pool_count=$(run_sql_val -c "SELECT count(*) FROM _sim_sensor_pool;")
	info "Sensor pool created with ${pool_count} entries."

	local avg_per_device
	avg_per_device=$(run_sql_val -c "SELECT round(avg(cnt)::numeric, 2) FROM (SELECT count(*) cnt FROM _sim_sensor_pool GROUP BY device_uuid) t;")
	info "Avg sensors/device: ${avg_per_device}"

	info "Phase 1 complete."
	echo ""
}

###############################################################################
# Phase 2: Parallel data generation
#
# Two-pass approach:
#   Pass 1 (Seed):  For EVERY chunk, insert 1 row per sensor so each chunk
#                   has exactly SENSORS_PER_CHUNK distinct sensor IDs.
#   Pass 2 (Fill):  Random rows to reach TARGET_SIZE_GB (if needed).
###############################################################################

_seed_worker() {
	local worker_id=$1
	local chunk_start_idx=$2
	local chunk_end_idx=$3
	local chunk_interval_secs=$4
	local sensor_count=$5
	local progress_file=$6

	local rows_done=0

	for ((c=chunk_start_idx; c<chunk_end_idx; c++)); do
		local offset_start=$((c * chunk_interval_secs))
		local chunk_span=$chunk_interval_secs

		local current_id=1
		while [ "$current_id" -le "$sensor_count" ]; do
			local batch_end=$((current_id + BATCH_SIZE - 1))
			if [ "$batch_end" -gt "$sensor_count" ]; then
				batch_end=$sensor_count
			fi

			psql "$CONNSTR" -X -q --set ON_ERROR_STOP=on <<-SQL
				SET synchronous_commit = off;
				INSERT INTO readings (timestamp, sensor_uuid, device_uuid, channel,
				                      value, min_value, max_value, avg_value, sample_count)
				SELECT
					now() - INTERVAL '${offset_start} seconds'
					      - (random() * ${chunk_span}) * INTERVAL '1 second',
					sp.sensor_uuid,
					sp.device_uuid,
					floor(random() * ${NUM_CHANNELS})::int + 1,
					random() * 1000,
					random() * 500,
					500 + random() * 500,
					random() * 1000,
					(random() * 100)::int + 1
				FROM _sim_sensor_pool sp
				WHERE sp.id BETWEEN ${current_id} AND ${batch_end};
			SQL

			local batch_count=$((batch_end - current_id + 1))
			rows_done=$((rows_done + batch_count))
			echo "$rows_done" > "$progress_file"
			current_id=$((batch_end + 1))
		done
	done
}

_fill_worker() {
	local worker_id=$1
	local day_start=$2
	local day_end=$3
	local worker_rows=$4
	local pool_count=$5
	local progress_file=$6

	local rows_done=0
	local day_span=$((day_end - day_start))
	local batch_count=0
	local checkpoint_every=10  # request checkpoint every N batches

	local max_offset=$((pool_count - BATCH_SIZE))
	if [ "$max_offset" -lt 1 ]; then
		max_offset=1
	fi

	while [ "$rows_done" -lt "$worker_rows" ]; do
		local remaining=$((worker_rows - rows_done))
		local this_batch=$BATCH_SIZE
		if [ "$remaining" -lt "$this_batch" ]; then
			this_batch=$remaining
		fi

		# Random offset into the sensor pool for diversity across batches
		local offset=$((RANDOM % max_offset + 1))
		local batch_end=$((offset + this_batch - 1))
		if [ "$batch_end" -gt "$pool_count" ]; then
			batch_end=$pool_count
		fi

		psql "$CONNSTR" -X -q --set ON_ERROR_STOP=on <<-SQL
			SET synchronous_commit = off;
			INSERT INTO readings (timestamp, sensor_uuid, device_uuid, channel,
			                      value, min_value, max_value, avg_value, sample_count)
			SELECT
				now() - INTERVAL '${day_start} days'
				      - (random() * ${day_span}) * INTERVAL '1 day'
				      - (random() * 86400) * INTERVAL '1 second',
				sp.sensor_uuid,
				sp.device_uuid,
				floor(random() * ${NUM_CHANNELS})::int + 1,
				random() * 1000,
				random() * 500,
				500 + random() * 500,
				random() * 1000,
				(random() * 100)::int + 1
			FROM _sim_sensor_pool sp
			WHERE sp.id BETWEEN ${offset} AND ${batch_end};
		SQL

		rows_done=$((rows_done + this_batch))
		batch_count=$((batch_count + 1))
		echo "$rows_done" > "$progress_file"

		# Worker 0 periodically requests a checkpoint to flush dirty pages
		if [ "$worker_id" -eq 0 ] && [ $((batch_count % checkpoint_every)) -eq 0 ]; then
			psql "$CONNSTR" -X -q --set ON_ERROR_STOP=off -c "CHECKPOINT;" 2>/dev/null || true
		fi
	done
}

_monitor_workers() {
	local label=$1
	local total_rows=$2
	local start_ts=$3
	local progress_dir=$4
	shift 4
	local worker_pids=("$@")
	local num_workers=${#worker_pids[@]}

	local all_done=false
	local iter=0
	local actual_size="?"
	while ! $all_done; do
		sleep 10
		iter=$((iter + 1))

		local total_done=0
		local alive=0
		for ((w=0; w<num_workers; w++)); do
			local wdone
			wdone=$(cat "${progress_dir}/worker_${w}" 2>/dev/null || echo "0")
			total_done=$((total_done + wdone))
			if kill -0 "${worker_pids[$w]}" 2>/dev/null; then
				alive=$((alive + 1))
			fi
		done

		local elapsed
		elapsed=$(elapsed_since "$start_ts")
		local rate=0
		if [ "$elapsed" -gt 0 ]; then
			rate=$((total_done / elapsed))
		fi
		local pct=0
		if [ "$total_rows" -gt 0 ]; then
			pct=$((total_done * 100 / total_rows))
		fi

		if [ $((iter % 3)) -eq 0 ]; then
			actual_size=$(run_sql_val -c "SELECT pg_size_pretty(sum(total_bytes)) from chunks_detailed_size('readings');" 2>/dev/null || echo "?")
		fi

		stat "[${label}] ${total_done}/${total_rows} rows (${pct}%) | Size: ${actual_size} | Rate: ${rate} rows/sec | Workers: ${alive} | Elapsed: ${elapsed}s"

		if [ "$alive" -eq 0 ]; then
			all_done=true
		fi
	done

	local failed=0
	for ((w=0; w<num_workers; w++)); do
		if ! wait "${worker_pids[$w]}"; then
			err "Worker ${w} failed!"
			failed=$((failed + 1))
		fi
	done
	if [ "$failed" -gt 0 ]; then
		err "${failed} worker(s) failed during ${label}. Aborting."
		exit 1
	fi
}

phase_generate() {
	info "=== Phase 2: Data Generation (${PARALLEL_JOBS} parallel workers) ==="

	local bytes_per_row=120
	local total_rows=$(( TARGET_SIZE_GB * 1000000000 / bytes_per_row ))

	local pool_count
	pool_count=$(run_sql_val -c "SELECT count(*) FROM _sim_sensor_pool;")
	if [ "$pool_count" -eq 0 ]; then
		err "Sensor pool is empty. Run --setup first."
		exit 1
	fi

	local chunk_interval_secs
	chunk_interval_secs=$(run_sql_val -c "SELECT EXTRACT(EPOCH FROM INTERVAL '${CHUNK_INTERVAL}')::int;")
	local total_seconds=$((DATA_SPAN_DAYS * 86400))
	local num_chunks=$((total_seconds / chunk_interval_secs))

	local sensors_per_chunk=$SENSORS_PER_CHUNK
	if [ "$sensors_per_chunk" -gt "$pool_count" ]; then
		sensors_per_chunk=$pool_count
	fi

	local seed_rows=0
	if [ "$SKIP_SEED" != "true" ]; then
		seed_rows=$(( sensors_per_chunk * num_chunks ))
	fi
	local fill_rows=$((total_rows - seed_rows))
	if [ "$fill_rows" -lt 0 ]; then
		fill_rows=0
	fi

	info "Target: ${TARGET_SIZE_GB} GB => ~${total_rows} rows (est. ${bytes_per_row} bytes/row)"
	info "Data span: ${DATA_SPAN_DAYS} days, chunk interval: ${CHUNK_INTERVAL} (${chunk_interval_secs}s) => ${num_chunks} chunks"
	info "Sensors: ${pool_count} total, ${sensors_per_chunk}/chunk, batch: ${BATCH_SIZE}, workers: ${PARALLEL_JOBS}"
	if [ "$SKIP_SEED" = "true" ]; then
		info "Pass 1 (Seed): SKIPPED (SKIP_SEED=true)"
	else
		info "Pass 1 (Seed): ${seed_rows} rows — ${sensors_per_chunk} sensors x ${num_chunks} chunks (1 row/sensor/chunk)"
	fi
	info "Pass 2 (Fill): ${fill_rows} rows — random to reach target size"
	echo ""

	local progress_dir
	progress_dir=$(mktemp -d)
	trap "rm -rf '$progress_dir'" EXIT

	# ---- Pass 1: Seed ----
	if [ "$SKIP_SEED" != "true" ]; then
		info "--- Pass 1: Seeding (${sensors_per_chunk} sensors x ${num_chunks} chunks) ---"
		local seed_start
		seed_start=$(date +%s)

		local chunks_per_worker=$((num_chunks / PARALLEL_JOBS))
		local worker_pids=()

		for ((w=0; w<PARALLEL_JOBS; w++)); do
			local c_start=$((w * chunks_per_worker))
			local c_end=$(( (w + 1) * chunks_per_worker ))
			if [ $((w + 1)) -eq "$PARALLEL_JOBS" ]; then
				c_end=$num_chunks
			fi
			local w_chunks=$((c_end - c_start))
			local w_seed_rows=$((sensors_per_chunk * w_chunks))

			local pfile="${progress_dir}/worker_${w}"
			echo "0" > "$pfile"

			info "  Worker ${w}: chunks [${c_start}, ${c_end}) => ${w_chunks} chunks x ${sensors_per_chunk} sensors = ${w_seed_rows} rows"
			_seed_worker "$w" "$c_start" "$c_end" "$chunk_interval_secs" "$sensors_per_chunk" "$pfile" &
			worker_pids+=($!)
		done

		_monitor_workers "Seed" "$seed_rows" "$seed_start" "$progress_dir" "${worker_pids[@]}"

		local seed_elapsed
		seed_elapsed=$(elapsed_since "$seed_start")
		info "Pass 1 (Seed) complete in ${seed_elapsed}s."
		echo ""
	fi

	# ---- Pass 2: Fill ----
	if [ "$fill_rows" -gt 0 ]; then
		info "--- Pass 2: Filling to target size ---"
		local fill_start
		fill_start=$(date +%s)

		local days_per_worker=$(( DATA_SPAN_DAYS / PARALLEL_JOBS ))
		local rows_per_worker=$(( fill_rows / PARALLEL_JOBS ))
		worker_pids=()

		for ((w=0; w<PARALLEL_JOBS; w++)); do
			local day_start=$((w * days_per_worker))
			local day_end=$(( (w + 1) * days_per_worker ))
			local w_rows=$rows_per_worker
			if [ $((w + 1)) -eq "$PARALLEL_JOBS" ]; then
				day_end=$DATA_SPAN_DAYS
				w_rows=$(( fill_rows - rows_per_worker * w ))
			fi

			local pfile="${progress_dir}/worker_${w}"
			echo "0" > "$pfile"

			info "  Worker ${w}: days [${day_start}, ${day_end}) => ${w_rows} rows"
			_fill_worker "$w" "$day_start" "$day_end" "$w_rows" "$pool_count" "$pfile" &
			worker_pids+=($!)
		done

		_monitor_workers "Fill" "$fill_rows" "$fill_start" "$progress_dir" "${worker_pids[@]}"

		local fill_elapsed
		fill_elapsed=$(elapsed_since "$fill_start")
		info "Pass 2 (Fill) complete in ${fill_elapsed}s."
		echo ""
	else
		info "Seed pass already exceeds target size, skipping fill pass."
	fi

	# Update planner statistics after bulk load
	info "Running ANALYZE on readings..."
	run_sql -c "ANALYZE readings;"

	# Final stats
	local final_size
	final_size=$(run_sql_val -c "SELECT pg_size_pretty(pg_total_relation_size('readings'));")
	local chunk_count
	chunk_count=$(run_sql_val -c "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'readings';")
	local actual_rows
	actual_rows=$(run_sql_val -c "SELECT reltuples::bigint FROM pg_class WHERE relname = 'readings';")
	stat "Final: ${final_size} | Chunks: ${chunk_count} | Rows (est): ${actual_rows}"
	echo ""
}

###############################################################################
# CLI Dispatch
###############################################################################
usage() {
	cat <<-EOF
		Usage: $0 [--setup|--generate|--all]

		  --setup      Phase 1: Create table, hypertable, sensor pool
		  --generate   Phase 2: Parallel data generation (seed + fill)
		  --all        Run setup + generate

		Configuration via environment variables:
		  CONNSTR            Connection string
		  NUM_SENSORS        Sensor count (default: 22000000)
		  NUM_DEVICES        Device count (default: 7000000)
		  NUM_CHANNELS       Channels per sensor (default: 4)
		  SENSORS_PER_CHUNK  Distinct sensors seeded per chunk (default: 1000000, max: NUM_SENSORS)
		  SKIP_SEED          Skip seed pass, fill only (default: false)
		  TARGET_SIZE_GB     Target data size in GB (default: 1000)
		  DATA_SPAN_DAYS     Days of historical data (default: 365)
		  CHUNK_INTERVAL     Chunk interval (default: '5 hours')
		  BATCH_SIZE         Rows per batch (default: 1000000)
		  PARALLEL_JOBS      Number of parallel workers (default: 4)

		Quick test: TARGET_SIZE_GB=1 PARALLEL_JOBS=2 ./cagg_datagen.sh --all
	EOF
}

case "${1:-}" in
	--setup|--generate|--all) ;;
	*)
		usage
		exit 1
		;;
esac

info "Config: CONNSTR=${CONNSTR}"
info "Config: NUM_SENSORS=${NUM_SENSORS}, NUM_DEVICES=${NUM_DEVICES}, NUM_CHANNELS=${NUM_CHANNELS}"
info "Config: SENSORS_PER_CHUNK=${SENSORS_PER_CHUNK}, SKIP_SEED=${SKIP_SEED}, TARGET_SIZE_GB=${TARGET_SIZE_GB}, DATA_SPAN_DAYS=${DATA_SPAN_DAYS}, CHUNK_INTERVAL='${CHUNK_INTERVAL}'"
info "Config: BATCH_SIZE=${BATCH_SIZE}, PARALLEL_JOBS=${PARALLEL_JOBS}"
echo ""

case "${1}" in
	--setup)    phase_setup ;;
	--generate) phase_generate ;;
	--all)
		phase_setup
		phase_generate
		;;
esac
