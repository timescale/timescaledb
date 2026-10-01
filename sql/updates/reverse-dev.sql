DROP FUNCTION IF EXISTS _timescaledb_functions.move_to_columnstore(REGCLASS, BOOLEAN);

DROP VIEW IF EXISTS timescaledb_information.hypertable_granular_refresh_settings;

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
