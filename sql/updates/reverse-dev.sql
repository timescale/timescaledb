DROP FUNCTION _timescaledb_functions.bloom1_contains_any(_timescaledb_internal.bloom1, anyarray);

DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Revert support for concurrent merge chunks()
DROP PROCEDURE IF EXISTS _timescaledb_functions.chunk_rewrite_cleanup();
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks_concurrently(REGCLASS[]);
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS, BOOLEAN);
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_rewrite;

-- Remove UUID time_bucket functions
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, TEXT, TIMESTAMPTZ, INTERVAL);

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
