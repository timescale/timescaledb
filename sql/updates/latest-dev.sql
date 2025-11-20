CREATE FUNCTION _timescaledb_functions.bloom1_contains_any(_timescaledb_internal.bloom1, anyarray)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

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

    IF num_null_chunk_ids > 0 THEN
       RAISE WARNING 'bloom filter sparse indexes require action to re-enable'
              USING HINT = 'See the changelog for details.';
    ELSE
        RAISE WARNING 'this is just for testing!';
    END IF;
END
$$;
