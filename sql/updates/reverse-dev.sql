<<<<<<< HEAD
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
=======
>>>>>>> 7b30533d4 (Release 2.20.3 (#8248))
