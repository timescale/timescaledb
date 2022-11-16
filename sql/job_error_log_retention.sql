-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- A retention policy is set up for the table _timescaledb_internal.job_errors (Error Log Retention Policy [2])
-- By default, it will run once a month and and drop rows older than a month.
CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_error_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
BEGIN
    SELECT  config->>'drop_after' INTO STRICT drop_after;
    WITH deleted AS
        (DELETE
        FROM _timescaledb_internal.job_errors
        WHERE finish_time < (now() - drop_after) RETURNING *)
        SELECT count(*)
        FROM deleted INTO numrows;
    RETURN numrows;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_error_retention_check(config JSONB) RETURNS VOID
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  drop_after interval;
BEGIN
    IF config IS NULL THEN
        RAISE EXCEPTION 'config cannot be NULL, and must contain drop_after';
    END IF;
    SELECT config->>'drop_after' INTO STRICT drop_after;
    IF drop_after IS NULL THEN
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
    2,
    'Error Log Retention Policy [2]',
    INTERVAL '1 month',
    INTERVAL '1 hour',
    -1,
    INTERVAL '1h',
    '_timescaledb_internal',
    'policy_job_error_retention',
    CURRENT_ROLE,
    true,
    '{"drop_after":"1 month"}',
    '_timescaledb_internal',
    'policy_job_error_retention_check',
    true,
    '2000-01-01 00:00:00+00'::timestamptz
) ON CONFLICT (id) DO NOTHING;
