-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.policy_job_error_retention(job_id integer, config JSONB) RETURNS integer
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    drop_after INTERVAL;
    numrows INTEGER;
BEGIN
    SELECT jsonb_object_field_text (config, 'drop_after')::interval INTO STRICT drop_after;
    WITH deleted as (DELETE FROM _timescaledb_internal.job_errors WHERE finish_time < (now() - drop_after) RETURNING *) select count(*) FROM deleted INTO numrows;
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
        RAISE EXCEPTION 'drop_after interval not provided';
    END IF;
    SELECT jsonb_object_field_text (config, 'drop_after')::interval INTO STRICT drop_after;
    IF drop_after IS NULL THEN 
        RAISE EXCEPTION 'Config can be NULL but must have drop_after if not';
    END IF ;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

INSERT INTO _timescaledb_config.bgw_job (id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, config, check_schema, check_name) VALUES
(2, 'Error Log Retention Policy [2]', INTERVAL '1 month', INTERVAL '10 min', -1, INTERVAL '1h', '_timescaledb_internal', 'policy_job_error_retention', CURRENT_ROLE, true, '{"drop_after":"1 month"}', '_timescaledb_internal', 'policy_job_error_retention_check')
ON CONFLICT (id) DO NOTHING;
