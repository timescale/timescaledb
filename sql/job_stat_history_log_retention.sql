-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- A retention policy is set up for the table _timescaledb_internal.job_errors (Error Log Retention Policy [2])
-- By default, it will run once a month and and drop rows older than a month.

CREATE OR REPLACE FUNCTION _timescaledb_functions.job_history_bsearch(search_point TIMESTAMPTZ) RETURNS BIGINT
AS
$$
DECLARE
  id_lower BIGINT;
  id_upper BIGINT;
  id_middle BIGINT DEFAULT 0;
  id_middle_next BIGINT;
  range_start TIMESTAMPTZ;
  range_end TIMESTAMPTZ;
  drop_after INTERVAL;
  target_tz TIMESTAMPTZ;
BEGIN
  SELECT (config->>'drop_after')::interval
  INTO drop_after
  FROM _timescaledb_config.bgw_job
  WHERE id = 3;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'system job 3 (job execution history retention) not found';
  END IF;

  RAISE DEBUG 'search_point %, drop_after %',
    search_point, drop_after;

  SELECT COALESCE(min(id), 0), COALESCE(max(id), 0)
  INTO id_lower, id_upper
  FROM _timescaledb_internal.bgw_job_stat_history;

  IF id_lower = 0 AND id_upper = 0 THEN
    RETURN NULL;
  END IF;

  -- We want the first entry in the table where execution_finish is >= search_point
  WHILE id_lower < id_upper LOOP
    id_middle := id_lower + (id_upper - id_lower) / 2;

    SELECT execution_finish
    INTO target_tz
    FROM _timescaledb_internal.bgw_job_stat_history
    WHERE id = id_middle;

    -- If the id_middle is not found, shift to the next id
    IF NOT FOUND THEN
      SELECT execution_finish, id
      INTO target_tz, id_middle_next
      FROM _timescaledb_internal.bgw_job_stat_history
      WHERE id >= id_middle AND id <= id_upper
      ORDER BY id LIMIT 1;

      IF NOT FOUND THEN
        RETURN NULL;
      END IF;

      RAISE DEBUG 'id_middle % not found, shift to the next id %', id_middle, id_middle_next;
      id_middle := id_middle_next;
    END IF;

    RAISE DEBUG 'target_tz %, id_lower %, id_upper %, id_middle %, le %, gt %',
      target_tz, id_lower, id_upper, id_middle, (target_tz <= search_point),
      (target_tz > search_point);

    IF search_point >= target_tz THEN
      id_upper := id_middle;
    ELSE
      id_lower := id_middle + 1;
    END IF;
  END LOOP;

  RETURN id_lower;
END;
$$
LANGUAGE plpgsql SET search_path TO pg_catalog, pg_temp, public;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
    search_point TIMESTAMPTZ;
    id_found BIGINT;
BEGIN
    drop_after := config->>'drop_after';
  SELECT now() - drop_after::interval
  INTO search_point
  FROM _timescaledb_config.bgw_job
  WHERE id = 3;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'system job 3 (job execution history retention) not found';
    RETURN 0;
  END IF;

  id_found := job_history_bsearch(search_point);

  IF id_found IS NULL THEN
    RAISE WARNING 'no job history for cleaning up';
    RETURN 0;
  END IF;

  CREATE TEMP TABLE __tmp_bgw_job_stat_history ON COMMIT DROP AS
  SELECT * FROM _timescaledb_internal.bgw_job_stat_history
  WHERE id >= id_found AND execution_finish >= search_point
  ORDER BY id;

  ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    DROP CONSTRAINT IF EXISTS bgw_job_stat_history_pkey;

  DROP INDEX IF EXISTS _timescaledb_internal.bgw_job_stat_history_job_id_idx;

  TRUNCATE _timescaledb_internal.bgw_job_stat_history;

   INSERT INTO _timescaledb_internal.bgw_job_stat_history
  SELECT * FROM __tmp_bgw_job_stat_history;

ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ADD CONSTRAINT bgw_job_stat_history_pkey PRIMARY KEY (id);

  CREATE INDEX bgw_job_stat_history_job_id_idx
    ON _timescaledb_internal.bgw_job_stat_history (job_id);

    GET DIAGNOSTICS numrows = ROW_COUNT;

    RETURN numrows;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention_check(config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;

    IF config->>'drop_after' IS NULL THEN
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF ;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

INSERT INTO _timescaledb_config.bgw_job (
    id,
    application_name,
    schedule_interval,
    max_runtime,
    max_retries,
    retry_period,
    proc_schema,
    proc_name,
    owner,
    scheduled,
    config,
    check_schema,
    check_name,
    fixed_schedule,
    initial_start
)
VALUES
(
    3,
    'Job History Log Retention Policy [3]',
    INTERVAL '1 month',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    '_timescaledb_functions',
    'policy_job_stat_history_retention',
    pg_catalog.quote_ident(current_role)::regrole,
    true,
    '{"drop_after":"1 month"}',
    '_timescaledb_functions',
    'policy_job_stat_history_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;
