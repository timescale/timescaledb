DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement);

DROP FUNCTION IF EXISTS _timescaledb_functions.get_create_command(table_name NAME);

-- switch the telemetry job to the database owner
UPDATE _timescaledb_catalog.bgw_job SET owner = d.datdba
FROM pg_catalog.pg_database d
WHERE id = 1 AND d.datname = pg_catalog.current_database() AND owner <> d.datdba AND owner = 'superuser'::pg_catalog.regrole;
