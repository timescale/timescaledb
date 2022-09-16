\c :TEST_DBNAME :ROLE_SUPERUSER

\set client_min_messages TO NOTICE;
create or replace procedure job_fail(jobid int, config jsonb) language plpgsql as $$
begin
perform pg_sleep(2);
raise exception 'raising an exception';
end
$$;

-- very simple case: job that raises an exception
select add_job('job_fail', '4 minutes') as jobf_id \gset

-- test jobs that try to update concurrently
CREATE TABLE custom_log (
    a int, 
    b int, 
    msg text
);

insert into custom_log values (0, 0, 'msg0');

ALTER SYSTEM SET DEFAULT_TRANSACTION_ISOLATION TO 'serializable';
SELECT pg_reload_conf();

-- test a concurrent update
CREATE OR REPLACE PROCEDURE custom_proc1(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE custom_log set msg = 'msg1' where msg = 'msg0';
  perform pg_sleep(10);
  COMMIT;
END
$$;

CREATE OR REPLACE PROCEDURE custom_proc2(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE custom_log set msg = 'msg2' where msg = 'msg0';
  perform pg_sleep(10);
  COMMIT;
END
$$;

select add_job('custom_proc1', '2 min', initial_start => now());
-- to make sure custom_log is first updated by custom_proc_1
select add_job('custom_proc2', '2 min', initial_start => now() + interval '5 seconds');

SELECT _timescaledb_internal.start_background_workers();
-- enough time to for job_fail to fail
select pg_sleep(10);
select job_id, error_data->>'message', error_data->>'sqlerrcode' from _timescaledb_internal.job_errors where job_id = :jobf_id;

select delete_job(:jobf_id);

select pg_sleep(20);
-- exclude the retention policy
select job_id, error_data->>'message' as err_message, error_data->>'sqlerrcode' as sqlerrcode 
from _timescaledb_internal.job_errors WHERE job_id != 2;

ALTER SYSTEM RESET DEFAULT_TRANSACTION_ISOLATION;
