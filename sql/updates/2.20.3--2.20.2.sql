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
