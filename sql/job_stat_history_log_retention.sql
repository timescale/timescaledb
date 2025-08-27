-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- A retention policy is set up for the table _timescaledb_internal.job_errors (Error Log Retention Policy [2])
-- By default, it will run once a month and and drop rows older than a month.

-- We use binary search on the id column to figure out which rows should be retained from the job history table
-- Doing it this way allows us to use the index on the `id` column, which (empirically) we found is faster than querying on the `execution_finish` column directly without an index
-- This works because `execution_finish` is always ordered.
-- We can consider alternative approaches to simplify this in the future.
CREATE OR REPLACE FUNCTION _timescaledb_functions.job_history_bsearch(search_point TIMESTAMPTZ) RETURNS BIGINT
AS
$$
DECLARE
  id_lower BIGINT;
  id_upper BIGINT;
  id_middle BIGINT DEFAULT 0;
  target_tz TIMESTAMPTZ;
BEGIN

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

    -- If the id_middle is not found, shift to a previous id that's still in the search space
    IF NOT FOUND THEN
      SELECT execution_finish, id
      INTO target_tz, id_middle
      FROM _timescaledb_internal.bgw_job_stat_history
      WHERE id <= id_middle AND id >= id_lower
      ORDER BY id LIMIT 1;

      IF NOT FOUND THEN
        id_middle := id_lower;
      END IF;

    END IF;

    IF target_tz >= search_point THEN
      id_upper := id_middle;
    ELSE
      id_lower := id_middle + 1;
    END IF;
  END LOOP;

  -- Handle the case where no ids need to be deleted and return NULL instead
  SELECT execution_finish
  INTO target_tz
  FROM _timescaledb_internal.bgw_job_stat_history
  WHERE id = id_lower;

  IF target_tz < search_point THEN
    RETURN NULL;
  END IF;

  RETURN id_lower;
END
$$
LANGUAGE plpgsql SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL SECURITY DEFINER AS
$BODY$
DECLARE
    numrows INTEGER;
    search_point TIMESTAMPTZ;
    id_found BIGINT;
BEGIN
  PERFORM set_config('lock_timeout', coalesce(config->>'lock_timeout', '5s'), true /* is local */);

  -- We need to prevent concurrent changes on this table when running this retention job
  -- We take an AccessExclusiveLock at the start since we TRUNCATE later
  LOCK TABLE _timescaledb_internal.bgw_job_stat_history IN ACCESS EXCLUSIVE MODE;

  search_point := now() - (config->>'drop_after')::interval;

  id_found := _timescaledb_functions.job_history_bsearch(search_point);

  IF id_found IS NULL THEN
    RETURN 0;
  END IF;

  CREATE TEMP TABLE __tmp_bgw_job_stat_history ON COMMIT DROP AS
  SELECT * FROM _timescaledb_internal.bgw_job_stat_history
  WHERE id >= id_found
  ORDER BY id;

  TRUNCATE _timescaledb_internal.bgw_job_stat_history;

  INSERT INTO _timescaledb_internal.bgw_job_stat_history
  SELECT * FROM __tmp_bgw_job_stat_history;

  GET DIAGNOSTICS numrows = ROW_COUNT;

  RETURN numrows;
END
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
END
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
