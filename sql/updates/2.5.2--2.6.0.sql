DROP FUNCTION IF EXISTS @extschema@.recompress_chunk;
DROP FUNCTION IF EXISTS @extschema@.delete_data_node;
DROP FUNCTION IF EXISTS @extschema@.get_telemetry_report;

-- Also see the comments for ContinuousAggsBucketFunction structure.
CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function(
  mat_hypertable_id integer PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  -- The schema of the function. Equals TRUE for "timescaledb_experimental", FALSE otherwise.
  experimental bool NOT NULL,
  -- Name of the bucketing function, e.g. "time_bucket" or "time_bucket_ng"
  name text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- `origin` argument of the function provided by the user
  origin text NOT NULL,
  -- `timezone` argument of the function provided by the user
  timezone text NOT NULL
);

-- in tables.sql the same is done with GRANT SELECT ON ALL TABLES IN SCHEMA
GRANT SELECT ON _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

-- Adding overloaded versions of invalidation_process_hypertable_log() and invalidation_process_cagg_log()
-- with bucket_functions argument is done in cagg_utils.sql. Note that this file is included when building
-- the update scripts, so we don't have to do it here.

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

CREATE FUNCTION @extschema@.delete_data_node(
    node_name              NAME,
    if_exists              BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE,
	drop_database          BOOLEAN = FALSE
) RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_data_node_delete' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.get_telemetry_report() RETURNS jsonb
    AS '@MODULE_PATHNAME@', 'ts_telemetry_get_report_jsonb'
LANGUAGE C STABLE PARALLEL SAFE;

