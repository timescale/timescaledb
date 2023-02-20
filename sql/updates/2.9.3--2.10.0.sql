CREATE OR REPLACE VIEW timescaledb_information.job_errors
WITH (security_barrier = true) AS
SELECT
    job_id,
    error_data ->> 'proc_schema' as proc_schema,
    error_data ->> 'proc_name' as proc_name,
    pid,
    start_time,
    finish_time,
    error_data ->> 'sqlerrcode' AS sqlerrcode,
    CASE WHEN error_data ->>'message' IS NOT NULL THEN
      CASE WHEN error_data ->>'detail' IS NOT NULL THEN
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data ->>'detail', '. ', error_data->>'hint')
        ELSE concat(error_data ->>'message', ' ', error_data ->>'detail')
        END
      ELSE
        CASE WHEN error_data ->>'hint' IS NOT NULL THEN concat(error_data ->>'message', '. ', error_data->>'hint')
        ELSE error_data ->>'message'
        END
      END
    ELSE
      'job crash detected, see server logs'
    END
    AS err_message
FROM
    _timescaledb_internal.job_errors
LEFT JOIN
    _timescaledb_config.bgw_job ON (bgw_job.id = job_errors.job_id)
WHERE
    pg_catalog.pg_has_role(current_user,
			   (SELECT pg_catalog.pg_get_userbyid(datdba)
			      FROM pg_catalog.pg_database
			     WHERE datname = current_database()),
			   'MEMBER') IS TRUE
    OR pg_catalog.pg_has_role(current_user, owner, 'MEMBER') IS TRUE;

REVOKE ALL ON _timescaledb_internal.job_errors FROM PUBLIC;
