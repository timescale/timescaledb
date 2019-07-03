ALTER TABLE _timescaledb_catalog.telemetry_metadata ADD COLUMN include_in_telemetry BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE _timescaledb_catalog.telemetry_metadata ALTER COLUMN include_in_telemetry DROP DEFAULT;
ALTER TABLE _timescaledb_catalog.telemetry_metadata RENAME TO metadata;
ALTER INDEX _timescaledb_catalog.telemetry_metadata_pkey RENAME TO metadata_pkey;
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_materialization_invalidation_log(
    materialization_id INTEGER
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id)
        ON DELETE CASCADE,
    lowest_modified_value BIGINT NOT NULL,
    greatest_modified_value BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO PUBLIC;
DROP FUNCTION IF EXISTS get_telemetry_report();

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregate_stats;
