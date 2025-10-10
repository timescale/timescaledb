DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config - 'max_successes_per_job' - 'max_failures_per_job',
          schedule_interval = '1 month'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will keep full history for each job and run once each month.';
END
$$;

DROP VIEW IF EXISTS timescaledb_information.job_stats;

-- Revert support for concurrent merge chunks()
DROP PROCEDURE IF EXISTS _timescaledb_functions.chunk_rewrite_cleanup();
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks_concurrently(REGCLASS[]);
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS, BOOLEAN);
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_rewrite;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_rewrite;

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
  chunk1 REGCLASS, chunk2 REGCLASS
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

