DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION IF EXISTS _timescaledb_internal.subtract_integer_from_now;

--changes for compression policy ---
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression;
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_interval;
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_integer;

--changes for continuous Aggregates over distributed hypertables ---
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_dist_ht_invalidation_trigger;
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_invalidation_log_delete;
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_cagg_log_add_entry;
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_hyper_log_add_entry;
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log;
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log;
DROP FUNCTION IF EXISTS _timescaledb_internal.materialization_invalidation_log_delete;
