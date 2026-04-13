-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- A retention policy is set up for the table _timescaledb_internal.job_errors (Error Log Retention Policy [2])
-- By default, it will run once a month and and drop rows older than a month.

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention(job_id integer, config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
BEGIN
  -- Delete rows older than the cutoff
  DELETE FROM _timescaledb_internal.bgw_job_stat_history
  WHERE execution_start < now() - (config->>'drop_after')::interval;

  -- Delete excess per-job entries beyond the configured limits
  WITH enumerated AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY j.job_id, j.succeeded
               ORDER BY j.execution_start DESC
           ) AS rn,
           j.succeeded
      FROM _timescaledb_internal.bgw_job_stat_history j
  )
  DELETE FROM _timescaledb_internal.bgw_job_stat_history
  WHERE id IN (
    SELECT e.id FROM enumerated e
    WHERE (e.succeeded AND e.rn > (config->>'max_successes_per_job')::int)
       OR (NOT e.succeeded AND e.rn > (config->>'max_failures_per_job')::int)
  );
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.policy_job_stat_history_retention_check(config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;

    IF NOT (config ? 'drop_after') THEN
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF;

    IF NOT (config ? 'max_successes_per_job') THEN
        RAISE EXCEPTION 'max_successes_per_job not provided';
    END IF;

    IF NOT (config ? 'max_failures_per_job') THEN
        RAISE EXCEPTION 'max_failures_per_job not provided';
    END IF;

    IF (config->>'max_successes_per_job')::integer < 10 THEN
        RAISE EXCEPTION 'max_successes_per_job has to be at least 10';
    END IF;

    IF (config->>'max_failures_per_job')::integer < 10 THEN
        RAISE EXCEPTION 'max_failures_per_job has to be at least 10';
    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

INSERT INTO _timescaledb_catalog.bgw_job (
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
    INTERVAL '6 hours',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    '_timescaledb_functions',
    'policy_job_stat_history_retention',
    pg_catalog.quote_ident(current_role)::regrole,
    true,
    '{"drop_after":"1 month","max_successes_per_job":1000,"max_failures_per_job":1000}',
    '_timescaledb_functions',
    'policy_job_stat_history_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;
