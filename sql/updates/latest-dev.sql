ALTER TABLE _timescaledb_catalog.telemetry_metadata ADD COLUMN include_in_telemetry BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE _timescaledb_catalog.telemetry_metadata ALTER COLUMN include_in_telemetry DROP DEFAULT;
ALTER TABLE _timescaledb_catalog.telemetry_metadata RENAME TO metadata;
ALTER INDEX _timescaledb_catalog.telemetry_metadata_pkey RENAME TO metadata_pkey;

DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range_open(bigint, bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range_closed(bigint, smallint);
