-- Remove time_bucket_ng functions
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ);
DROP FUNCTION IF EXISTS timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ);
DROP PROCEDURE IF EXISTS _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS);
