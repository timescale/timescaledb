DROP VIEW IF EXISTS timescaledb_information.dimensions;

-- Drop new function signatures that include origin parameter
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(
    regclass, name, name, integer, name, name, anyelement,
    boolean, boolean, regproc, boolean, text, regproc, regproc, "any"
);
DROP FUNCTION IF EXISTS @extschema@.set_chunk_time_interval(regclass, anyelement, name, "any");
DROP FUNCTION IF EXISTS @extschema@.set_partitioning_interval(regclass, anyelement, name, "any");
DROP FUNCTION IF EXISTS @extschema@.add_dimension(regclass, name, integer, anyelement, regproc, boolean, "any");
DROP FUNCTION IF EXISTS @extschema@.by_range(name, anyelement, regproc, "any");

--
-- Rebuild the catalog table `_timescaledb_catalog.dimension` to remove interval_origin column
--

-- Drop views that depend on the dimension table
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.hypertable_columnstore_settings;
DROP VIEW IF EXISTS timescaledb_information.hypertable_compression_settings;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Drop foreign key constraints referencing dimension table
ALTER TABLE _timescaledb_catalog.dimension_slice
    DROP CONSTRAINT dimension_slice_dimension_id_fkey;

-- Drop the dimension table and its sequence from the extension so we can rebuild it
ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.dimension;
ALTER EXTENSION timescaledb
    DROP SEQUENCE _timescaledb_catalog.dimension_id_seq;

-- Save existing data without interval_origin column
CREATE TABLE _timescaledb_catalog._tmp_dimension AS
    SELECT
        id,
        hypertable_id,
        column_name,
        column_type,
        aligned,
        num_slices,
        partitioning_func_schema,
        partitioning_func,
        interval_length,
        compress_interval_length,
        integer_now_func_schema,
        integer_now_func
    FROM
        _timescaledb_catalog.dimension
    ORDER BY
        id;

-- Drop old table
DROP TABLE _timescaledb_catalog.dimension;

-- Create table without interval_origin column
CREATE TABLE _timescaledb_catalog.dimension (
    id serial NOT NULL,
    hypertable_id integer NOT NULL,
    column_name name NOT NULL,
    column_type REGTYPE NOT NULL,
    aligned boolean NOT NULL,
    -- closed dimensions
    num_slices smallint NULL,
    partitioning_func_schema name NULL,
    partitioning_func name NULL,
    -- open dimensions (e.g., time)
    interval_length bigint NULL,
    -- compress interval for rollup during compression
    compress_interval_length bigint NULL,
    integer_now_func_schema name NULL,
    integer_now_func name NULL,
    -- table constraints
    CONSTRAINT dimension_pkey PRIMARY KEY (id),
    CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
    CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
    CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
    CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
    CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0),
    CONSTRAINT dimension_compress_interval_length_check CHECK (compress_interval_length IS NULL OR compress_interval_length > 0),
    CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

-- Copy data from temp table
INSERT INTO _timescaledb_catalog.dimension
SELECT * FROM _timescaledb_catalog._tmp_dimension;

-- Drop temp table
DROP TABLE _timescaledb_catalog._tmp_dimension;

-- Restore sequence value
SELECT setval(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'),
              max(id), true)
FROM _timescaledb_catalog.dimension;

-- Re-add foreign key constraint
ALTER TABLE _timescaledb_catalog.dimension_slice
    ADD CONSTRAINT dimension_slice_dimension_id_fkey
        FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE;

-- Register for pg_dump
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

GRANT SELECT ON TABLE _timescaledb_catalog.dimension TO PUBLIC;
GRANT SELECT ON SEQUENCE _timescaledb_catalog.dimension_id_seq TO PUBLIC;

ANALYZE _timescaledb_catalog.dimension;

--
-- END Rebuild the catalog table `_timescaledb_catalog.dimension`
--

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

