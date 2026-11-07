DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement);

DROP FUNCTION IF EXISTS _timescaledb_functions.get_create_command(table_name NAME);

-- switch the telemetry job to the database owner
UPDATE _timescaledb_catalog.bgw_job SET owner = d.datdba
FROM pg_catalog.pg_database d
WHERE id IN (1,2,3) AND d.datname = pg_catalog.current_database() AND owner <> d.datdba;
