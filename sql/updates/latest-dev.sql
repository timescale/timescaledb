DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Add support for concurrent merge_chunks()
CREATE TABLE _timescaledb_catalog.chunk_rewrite (
  chunk_relid REGCLASS NOT NULL,
  new_relid REGCLASS NOT NULL,
  CONSTRAINT chunk_rewrite_key UNIQUE (chunk_relid)
);

GRANT SELECT ON _timescaledb_catalog.chunk_rewrite TO PUBLIC;
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS);


-- Check whether the database has the sparse bloom filter indexes on compressed
-- chunks, which will require manual action to re-enable.
DO $$
DECLARE
    num_chunks_with_bloom int;
BEGIN
    SELECT count(*) INTO num_chunks_with_bloom
    FROM pg_attribute WHERE attname LIKE '_ts_meta_v2_bloom1_%';

    IF num_chunks_with_bloom > 0 THEN
       RAISE WARNING 'bloom filter sparse indexes require action to re-enable'
              USING HINT = 'See the changelog for details.';
    END IF;
END
$$;
-- Make chunk_id use NULL to mark special entries instead of 0
-- (Invalid chunk) since that doesn't work with the FK constraint on
-- chunk_id.
ALTER TABLE _timescaledb_catalog.chunk_column_stats ALTER COLUMN chunk_id DROP NOT NULL;
UPDATE _timescaledb_catalog.chunk_column_stats SET chunk_id = NULL WHERE chunk_id = 0;

DROP PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate REGCLASS,
    window_start "any",
    window_end "any",
    force BOOLEAN
);

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE,
    options                  JSONB = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts DATE, origin DATE);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ, timezone TEXT);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ);

DROP FUNCTION timescaledb_experimental.time_bucket_ng(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ);

DROP PROCEDURE _timescaledb_functions.cagg_migrate_to_time_bucket(cagg REGCLASS);
