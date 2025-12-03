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
