-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log`
-- to drop the `seqnum` column.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log AS
    SELECT hypertable_id, lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (
  hypertable_id integer NOT NULL,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL
);

INSERT INTO _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_hypertable_invalidation_log;

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');
-- end rebuild _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log --

-- Rebuild the catalog table `_timescaledb_catalog.continuous_aggs_materialization_invalidation_log`
-- to drop the `seqnum` column.
CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log AS
    SELECT materialization_id, lowest_modified_value, greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (
  materialization_id integer,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL,
  CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_materialization_invalidation_log;

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');
-- end rebuild _timescaledb_catalog.continuous_aggs_materialization_invalidation_log --

-- Rebuild _timescaledb_catalog.continuous_agg to drop granular_refresh_enabled.
DROP VIEW IF EXISTS timescaledb_experimental.policies;

ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    DROP CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    DROP CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    DROP CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg;

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
        schema_change_timestamp
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
    schema_change_timestamp bigint,
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

INSERT INTO _timescaledb_catalog.continuous_agg (
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
    schema_change_timestamp
)
SELECT * FROM _timescaledb_catalog._tmp_continuous_agg;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg;

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg TO PUBLIC;

ALTER TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
    ADD CONSTRAINT continuous_aggs_jobs_refresh_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    ADD CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey
        FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges
    ADD CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey
        FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_watermark
    ADD CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey
        FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id) ON DELETE CASCADE;
-- end rebuild _timescaledb_catalog.continuous_agg --

DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_get_tenant_tracking_info(REGCLASS);
DROP FUNCTION IF EXISTS _timescaledb_functions.tenant_tracking_map();
