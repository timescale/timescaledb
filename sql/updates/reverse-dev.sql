DROP FUNCTION IF EXISTS _timescaledb_functions.move_to_columnstore(REGCLASS, BOOLEAN);

DROP VIEW IF EXISTS timescaledb_information.hypertable_granular_refresh_settings;

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_get_tenant_tracking_info(REGCLASS);
CREATE FUNCTION _timescaledb_functions.hypertable_get_tenant_tracking_info(
    hypertable REGCLASS,
    OUT seq_num int4,
    OUT active_generation int4,
    OUT nentries int4,
    OUT status int4,
    OUT late_threshold_start int8,
    OUT late_threshold_end int8)
AS '@MODULE_PATHNAME@', 'ts_hypertable_get_tenant_tracking_info' LANGUAGE C VOLATILE;
