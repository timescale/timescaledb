CREATE OR REPLACE FUNCTION timescaledb_experimental.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    hypertable_chunk         REGCLASS
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh_chunk' LANGUAGE C VOLATILE;
