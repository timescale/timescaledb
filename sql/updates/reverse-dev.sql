DROP VIEW IF EXISTS timescaledb_information.dimensions;
--
-- Rebuild the catalog table `_timescaledb_catalog.continuous_agg` to add `finalized` column
--

DROP VIEW IF EXISTS timescaledb_experimental.policies;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.jobs;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    DROP CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog._tmp_continuous_agg AS
    SELECT
        mat_hypertable_id,
        raw_hypertable_id,
        parent_mat_hypertable_id,
        user_view_schema,
        user_view_name,
        partial_view_schema,
        partial_view_name,
        direct_view_schema,
        direct_view_name,
        materialized_only,
        true AS finalized
    FROM
        _timescaledb_catalog.continuous_agg
    ORDER BY
        mat_hypertable_id;

DROP TABLE _timescaledb_catalog.continuous_agg;

CREATE TABLE _timescaledb_catalog.continuous_agg (
    mat_hypertable_id integer NOT NULL,
    raw_hypertable_id integer NOT NULL,
    parent_mat_hypertable_id integer,
    user_view_schema name NOT NULL,
    user_view_name name NOT NULL,
    partial_view_schema name NOT NULL,
    partial_view_name name NOT NULL,
    direct_view_schema name NOT NULL,
    direct_view_name name NOT NULL,
    materialized_only bool NOT NULL DEFAULT FALSE,
    finalized bool NOT NULL DEFAULT TRUE,
    -- table constraints
    CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
    CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
    CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
    CONSTRAINT continuous_agg_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_raw_hypertable_id_fkey
        FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
    CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey
        FOREIGN KEY (parent_mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_agg
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    ADD CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id)
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id)
        REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE;

ANALYZE _timescaledb_catalog.continuous_agg;

--
-- END Rebuild the catalog table `_timescaledb_catalog.continuous_agg`
--

DROP FUNCTION IF EXISTS _timescaledb_functions.extension_state();
CREATE SCHEMA _timescaledb_debug;
GRANT USAGE ON SCHEMA _timescaledb_debug TO PUBLIC;

CREATE SCHEMA _timescaledb_config;
GRANT USAGE ON SCHEMA _timescaledb_config TO PUBLIC;

ALTER TABLE _timescaledb_catalog.bgw_job SET SCHEMA _timescaledb_config;

DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_compressed_batch_size(REGCLASS);
DROP PROCEDURE IF EXISTS _timescaledb_functions.rebuild_columnstore(REGCLASS);
DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_grouping_columns;

DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_to_array(_timescaledb_internal.compressed_data, ANYELEMENT);
DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_column_size(_timescaledb_internal.compressed_data, ANYELEMENT);

DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size;


-- Revert time_bucket_gapfill functions to old signatures
-- Integer variants stay at 4 args (unchanged between versions)
-- Only timestamp and timezone variants need to be reverted

-- Drop the new timestamp functions with origin/offset parameters (6 params)
DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(INTERVAL, DATE, DATE, DATE, DATE, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(INTERVAL, TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, INTERVAL);

-- Drop the new timezone function with origin/offset parameters (7 params)
DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(INTERVAL, TIMESTAMPTZ, TEXT, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ, INTERVAL);

-- Recreate old timestamp variants (4 params)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE=NULL, finish DATE=NULL) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP=NULL, finish TIMESTAMP=NULL) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;

-- Recreate old timezone variant (5 params)
CREATE OR REPLACE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE PARALLEL SAFE;
