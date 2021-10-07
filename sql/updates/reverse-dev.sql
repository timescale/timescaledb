DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION IF EXISTS _timescaledb_internal.subtract_integer_from_now; 

--changes for compression policy ---
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression;
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_interval;
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_integer;
