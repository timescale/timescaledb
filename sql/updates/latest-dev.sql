
-- the update script from 2.14.2 to 2.15.0 migrated _timescaledb_internal.bgw_job_stat_history with incorrect definition
-- fix _timescaledb_internal.bgw_job_stat_history if definition is incorrect
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_attribute WHERE attrelid='_timescaledb_internal.bgw_job_stat_history'::regclass AND attname = 'id' AND atttypid <> 'bigint'::regtype) THEN

    --
    -- START bgw_job_stat_history
    --
    DROP VIEW IF EXISTS timescaledb_information.job_history;

    CREATE TABLE _timescaledb_internal._tmp_bgw_job_stat_history AS SELECT * FROM _timescaledb_internal.bgw_job_stat_history;
    CREATE TABLE _timescaledb_internal._tmp_job_stat_history_id_seq AS SELECT last_value, is_called FROM _timescaledb_internal.bgw_job_stat_history_id_seq;

    DROP TABLE _timescaledb_internal.bgw_job_stat_history;

    CREATE SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq MINVALUE 1;

    CREATE TABLE _timescaledb_internal.bgw_job_stat_history (
      id BIGINT NOT NULL DEFAULT nextval('_timescaledb_internal.bgw_job_stat_history_id_seq'),
      job_id INTEGER NOT NULL,
      pid INTEGER,
      execution_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      execution_finish TIMESTAMPTZ,
      succeeded boolean,
      data jsonb,
      -- table constraints
      CONSTRAINT bgw_job_stat_history_pkey PRIMARY KEY (id)
    );

    ALTER SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq OWNED BY _timescaledb_internal.bgw_job_stat_history.id;

    INSERT INTO _timescaledb_internal.bgw_job_stat_history (id, job_id, pid, execution_start, execution_finish, succeeded, data) SELECT id, job_id, pid, execution_start, execution_finish, succeeded, data FROM _timescaledb_internal._tmp_bgw_job_stat_history;
    SELECT setval('_timescaledb_internal.bgw_job_stat_history_id_seq', last_value, is_called) FROM _timescaledb_internal._tmp_job_stat_history_id_seq;

    CREATE INDEX bgw_job_stat_history_job_id_idx ON _timescaledb_internal.bgw_job_stat_history (job_id);
    REVOKE ALL ON _timescaledb_internal.bgw_job_stat_history FROM PUBLIC;
    --
    -- END bgw_job_stat_history
    --

  END IF;
END
$$;

DROP FUNCTION IF EXISTS _timescaledb_functions.estimate_uncompressed_size(regclass);
