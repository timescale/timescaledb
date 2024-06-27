-- Hypercore AM
DROP ACCESS METHOD IF EXISTS hypercore_proxy;
DROP FUNCTION IF EXISTS ts_hypercore_proxy_handler;
DROP ACCESS METHOD IF EXISTS hypercore;
DROP FUNCTION IF EXISTS ts_hypercore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN, hypercore_use_access_method BOOL);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL, initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL, hypercore_use_access_method BOOL);

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_policy_compression_add'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies(relation REGCLASS, if_not_exists BOOL, refresh_start_offset "any", refresh_end_offset "any", compress_after "any", drop_after "any", hypercore_use_access_method BOOL);

CREATE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL)
RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_policies_add'
LANGUAGE C VOLATILE;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(job_id INTEGER, htid INTEGER, lag ANYELEMENT, maxchunks INTEGER, verbose_log BOOLEAN, recompress_enabled  BOOLEAN, use_creation_time BOOLEAN, useam BOOLEAN);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);
DROP PROCEDURE IF EXISTS @extschema@.convert_to_columnstore(REGCLASS, BOOLEAN, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS @extschema@.convert_to_rowstore(REGCLASS, BOOLEAN);
DROP PROCEDURE IF EXISTS @extschema@.add_columnstore_policy(REGCLASS, "any", BOOL, INTERVAL, TIMESTAMPTZ, TEXT, INTERVAL, BOOL);
DROP PROCEDURE IF EXISTS @extschema@.remove_columnstore_policy(REGCLASS, BOOL);
DROP FUNCTION IF EXISTS @extschema@.hypertable_columnstore_stats(REGCLASS);
DROP FUNCTION IF EXISTS @extschema@.chunk_columnstore_stats(REGCLASS);

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.hypertable_columnstore_settings;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.chunk_columnstore_settings;

DROP VIEW timescaledb_information.hypertable_columnstore_settings;
DROP VIEW timescaledb_information.chunk_columnstore_settings;

-- Restore the removed metadata table
CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_aggs_bucket_function_func_check CHECK (pg_catalog.to_regprocedure(bucket_func) IS DISTINCT FROM 0)
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  (mat_hypertable_id, bucket_func, bucket_width, bucket_origin, bucket_offset, bucket_timezone, bucket_fixed_width)
SELECT mat_hypertable_id, bf.bucket_func::text, bf.bucket_width, bf.bucket_origin, bf.bucket_offset, bf.bucket_timezone, bf.bucket_fixed_width
FROM _timescaledb_catalog.continuous_agg, LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id) AS bf;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function_info(INTEGER);

