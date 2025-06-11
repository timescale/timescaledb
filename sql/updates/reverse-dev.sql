DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS);
DROP PROCEDURE @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP PROCEDURE @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);
DROP PROCEDURE _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
DROP FUNCTION _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(
  INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN
);

CREATE PROCEDURE _timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$
BEGIN
  -- empty body
END;
$$ LANGUAGE PLPGSQL;

-- Add back the chunk_column_stats NOT NULL constraint. But first
-- delete all entries with with NULL since they will no longer be
-- allowed. Note that reverting chunk_id back to 0 because it will
-- violate the FK constraint. Even if we would revert, the downgrade
-- tests for "restore" would fail due to violating entries. Removing
-- the entries effectively means that collecting column stats for
-- those columns will be disabled. It can be enabled again after
-- downgrade. We emit a warning if anything was disabled.
DO $$
DECLARE
    num_null_chunk_ids int;
BEGIN

    SELECT count(*) INTO num_null_chunk_ids
    FROM _timescaledb_catalog.chunk_column_stats WHERE chunk_id IS NULL;

    IF num_null_chunk_ids > 0 THEN
       RAISE WARNING 'chunk skipping has been disabled for all hypertables'
              USING HINT = 'Use enable_chunk_skipping() to re-enable chunk skipping';
    END IF;
END
$$;
DELETE FROM _timescaledb_catalog.chunk_column_stats WHERE chunk_id IS NULL;
ALTER TABLE _timescaledb_catalog.chunk_column_stats ALTER COLUMN chunk_id SET NOT NULL;

DROP PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate REGCLASS,
    window_start "any",
    window_end "any",
    force BOOLEAN,
    options JSONB
);

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE) RETURNS DATE
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT) RETURNS TIMESTAMPTZ
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STABLE PARALLEL SAFE STRICT;

 CREATE OR REPLACE FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
     AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STABLE PARALLEL SAFE STRICT;

-- Migrate a CAgg which is using the experimental time_bucket_ng function
 -- into a CAgg using the regular time_bucket function
 CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS)
 AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

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

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function_info(
    mat_hypertable_id INTEGER,
    -- The bucket function
    OUT bucket_func REGPROCEDURE,
    -- `bucket_width` argument of the function, e.g. "1 month"
    OUT bucket_width TEXT,
    -- optional `origin` argument of the function provided by the user
    OUT bucket_origin TEXT,
    -- optional `offset` argument of the function provided by the user
    OUT bucket_offset TEXT,
    -- optional `timezone` argument of the function provided by the user
    OUT bucket_timezone TEXT,
    -- fixed or variable sized bucket
    OUT bucket_fixed_width BOOLEAN
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_bucket_function_info' LANGUAGE C STRICT VOLATILE;

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  (mat_hypertable_id, bucket_func, bucket_width, bucket_origin, bucket_offset, bucket_timezone, bucket_fixed_width)
SELECT mat_hypertable_id, bf.bucket_func::text, bf.bucket_width, bf.bucket_origin, bf.bucket_offset, bf.bucket_timezone, bf.bucket_fixed_width
FROM _timescaledb_catalog.continuous_agg, LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id) AS bf;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;
