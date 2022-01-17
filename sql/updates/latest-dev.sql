-- Get rid of chunk_id from materialization hypertables
DROP FUNCTION IF EXISTS timescaledb_experimental.refresh_continuous_aggregate(REGCLASS, REGCLASS);
