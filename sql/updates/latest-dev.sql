-- Add support for concurrent merge_chunks()
CREATE TABLE _timescaledb_catalog.chunk_rewrite (
  chunk_relid REGCLASS NOT NULL,
  new_relid REGCLASS NOT NULL,
  CONSTRAINT chunk_rewrite_key UNIQUE (chunk_relid)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_rewrite', '');
GRANT SELECT ON _timescaledb_catalog.chunk_rewrite TO PUBLIC;
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS);

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
   chunk1 REGCLASS, chunk2 REGCLASS, concurrently BOOLEAN = false
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';


DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config || '{"max_successes_per_job": 1000, "max_failures_per_job": 1000}',
          schedule_interval = '6 hours'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will only keep the last 1000 successes and failures and run once each day.';
END
$$;

DROP VIEW IF EXISTS timescaledb_information.job_stats;


-- Fix wrong migration by removing all sparse index configurations
-- which only contain auto sparse indexing definitions on hypertable
DO $$
DECLARE
    rec RECORD;
    num_config INT;
BEGIN
	IF NOT EXISTS (
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = '_timescaledb_catalog'
		AND table_name = 'compression_settings'
		AND column_name = 'index') THEN
		RETURN;
	END IF;

    FOR rec IN
        SELECT relid, compress_relid, index
        FROM _timescaledb_catalog.compression_settings
		WHERE compress_relid IS NULL
    LOOP
		num_config:=0;

		SELECT count(*)
		INTO num_config
		FROM jsonb_array_elements(rec.index) AS idx
		WHERE idx::jsonb @> '{"storage":"config"}'::jsonb;

		IF num_config = 0 THEN
            UPDATE _timescaledb_catalog.compression_settings
            SET index = NULL
            WHERE relid = rec.relid;
		END IF;
    END LOOP;
END $$;

