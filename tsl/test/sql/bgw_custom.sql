-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE custom_log(job_id int, args jsonb, extra text, runner NAME DEFAULT CURRENT_ROLE);

CREATE OR REPLACE FUNCTION custom_func(jobid int, args jsonb) RETURNS VOID LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'custom_func');
$$;

CREATE OR REPLACE FUNCTION custom_func_definer(jobid int, args jsonb) RETURNS VOID LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'security definer');
$$ SECURITY DEFINER;

CREATE OR REPLACE PROCEDURE custom_proc(job_id int, args jsonb) LANGUAGE SQL AS
$$
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc');
$$;

-- procedure with transaction handling
CREATE OR REPLACE PROCEDURE custom_proc2(job_id int, args jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 1 COMMIT');
  COMMIT;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 2 ROLLBACK');
  ROLLBACK;
  INSERT INTO custom_log VALUES($1, $2, 'custom_proc 3 COMMIT');
  COMMIT;
END
$$;

SELECT add_job('custom_func','1h', config:='{"type":"function"}'::jsonb);
SELECT add_job('custom_proc','1h', config:='{"type":"procedure"}'::jsonb);
SELECT add_job('custom_proc2','1h', config:= '{"type":"procedure"}'::jsonb);

SELECT add_job('custom_func', '1h', config:='{"type":"function"}'::jsonb);
SELECT add_job('custom_func_definer', '1h', config:='{"type":"function"}'::jsonb);

CALL run_job(1000);
CALL run_job(1001);
CALL run_job(1002);
CALL run_job(1003);
CALL run_job(1004);

SELECT * FROM custom_log ORDER BY job_id, extra;

SELECT delete_job(1000);
SELECT delete_job(1001);
SELECT delete_job(1002);
SELECT delete_job(1003);
SELECT delete_job(1004);

-- check jobs got removed
SELECT * FROM _timescaledb_config.bgw_job WHERE id >= 1000;

