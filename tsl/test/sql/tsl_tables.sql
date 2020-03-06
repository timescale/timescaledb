-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timescaledb.license_key='CommunityLicense';

CREATE OR REPLACE FUNCTION ts_test_chunk_stats_insert(job_id INTEGER, chunk_id INTEGER, num_times_run INTEGER, last_time_run TIMESTAMPTZ = NULL) RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION delete_job(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_bgw_job_delete_by_id'
LANGUAGE C VOLATILE STRICT;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

select * from _timescaledb_config.bgw_policy_drop_chunks;
select * from _timescaledb_config.bgw_policy_reorder;

CREATE TABLE test_table(time timestamptz, junk int);
CREATE TABLE test_table_int(time bigint, junk int);

SELECT create_hypertable('test_table', 'time');
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);

CREATE INDEX second_index on test_table (time);
CREATE INDEX third_index on test_table (time);

select add_reorder_policy('test_table', 'test_table_time_idx') as job_id \gset
-- Noop for duplicate policy
select add_reorder_policy('test_table', 'test_table_time_idx', true);
select add_reorder_policy('test_table', 'second_index', true);
select add_reorder_policy('test_table', 'third_index', true);

\set ON_ERROR_STOP 0
-- Error whenever incorrect arguments are applied (must have table and index)
select add_reorder_policy('test_table', 'bad_index');
select add_reorder_policy('test_table', '');
select add_reorder_policy('test_table');

select add_reorder_policy('test_table', 'second_index');
select add_reorder_policy('test_table', 'third_index');
\set ON_ERROR_STOP 1

select * from _timescaledb_config.bgw_policy_reorder where job_id=:job_id;

-- Now check that default scheduling interval for reorder policy is calculated correctly
-- Should be 1/2 default chunk interval length
CREATE TABLE test_table2(time timestamptz, junk int);
SELECT create_hypertable('test_table2', 'time', chunk_time_interval=>INTERVAL '1 day');
select add_reorder_policy('test_table2', 'test_table2_time_idx');

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'reorder');

DROP TABLE test_table2;
-- Make sure that test_table2 reorder policy gets dropped
select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'reorder');

-- Error whenever incorrect arguments are applied (must have table and interval)
\set ON_ERROR_STOP 0
select add_drop_chunks_policy();
select add_drop_chunks_policy('test_table');
select add_drop_chunks_policy(INTERVAL '3 hours');
select add_drop_chunks_policy('test_table', INTERVAL 'haha');
select add_drop_chunks_policy('test_table', 'haha');
select add_drop_chunks_policy('test_table', 42);
select add_drop_chunks_policy('fake_table', INTERVAL '3 month', true);
select add_drop_chunks_policy('fake_table', INTERVAL '3 month');
select add_drop_chunks_policy('test_table', cascade=>true);
select add_drop_chunks_policy('test_table_int', INTERVAL '3 month', true);
select add_drop_chunks_policy('test_table_int', 42, true);
\set ON_ERROR_STOP 1

select add_drop_chunks_policy('test_table', INTERVAL '3 month', true);
-- add_*_policy should be noop only for policies with the exact same parameters
select add_drop_chunks_policy('test_table', INTERVAL '3 month', true, true);
-- Should not add new policy with different parameters
select add_drop_chunks_policy('test_table', INTERVAL '3 month', false, true);
select add_drop_chunks_policy('test_table', INTERVAL '1 year', if_not_exists => true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', if_not_exists => true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', true, if_not_exists => true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', if_not_exists => true, cascade_to_materializations => true);

select * from _timescaledb_config.bgw_policy_drop_chunks;

\set ON_ERROR_STOP 0
select add_drop_chunks_policy('test_table', INTERVAL '3 month', false);
select add_drop_chunks_policy('test_table', INTERVAL '1 year');
select add_drop_chunks_policy('test_table', INTERVAL '3 days');
select add_drop_chunks_policy('test_table', INTERVAL '3 days', true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', cascade_to_materializations => true);
\set ON_ERROR_STOP 1

select * from _timescaledb_config.bgw_policy_drop_chunks;
select r.job_id,r.hypertable_id,r.older_than,r.cascade from _timescaledb_config.bgw_policy_drop_chunks as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';

select remove_drop_chunks_policy('test_table');

-- Test set_integer_now_func and add_drop_chunks_policy with
-- hypertables that have integer time dimension

select * from _timescaledb_catalog.dimension;
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE SCHEMA IF NOT EXISTS my_new_schema;
create or replace function my_new_schema.dummy_now2() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
grant execute on ALL FUNCTIONS IN SCHEMA my_new_schema to public;
select set_integer_now_func('test_table_int', 'my_new_schema.dummy_now2');

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
select * from _timescaledb_catalog.dimension;


select * from _timescaledb_config.bgw_policy_drop_chunks;
select r.job_id,r.hypertable_id,r.older_than,r.cascade from _timescaledb_config.bgw_policy_drop_chunks as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';
select remove_reorder_policy('test_table');

select * from _timescaledb_config.bgw_policy_reorder;
select r.job_id,r.hypertable_id,r.hypertable_index_name from _timescaledb_config.bgw_policy_reorder as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';

select add_drop_chunks_policy('test_table', INTERVAL '3 month');
select * from _timescaledb_config.bgw_policy_drop_chunks;
select remove_drop_chunks_policy('test_table');
select * from _timescaledb_config.bgw_policy_drop_chunks;

select add_drop_chunks_policy('test_table_int', 1);
select * from _timescaledb_config.bgw_policy_drop_chunks;
-- Should not add new policy with different parameters
select add_drop_chunks_policy('test_table_int', 2, false, true);
select add_drop_chunks_policy('test_table_int', 1, false, true, true);
select add_drop_chunks_policy('test_table_int', 1, true, true, false);

select remove_drop_chunks_policy('test_table_int');
select * from _timescaledb_config.bgw_policy_drop_chunks;

-- Make sure remove works when there's nothing to remove
select remove_drop_chunks_policy('test_table', true);
select remove_reorder_policy('test_table', true);

\set ON_ERROR_STOP 0
select remove_drop_chunks_policy();
select remove_drop_chunks_policy('fake_table');
select remove_reorder_policy();
select remove_reorder_policy('fake_table');
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
-- This should be noop
select remove_reorder_policy(2, true);
-- Fail with error message
select remove_reorder_policy(2);
\set ON_ERROR_STOP 1

-- Now make sure policy args have correct job deletion dependency
select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'reorder');

select add_drop_chunks_policy('test_table', INTERVAL '2 month') as job_id \gset
select add_reorder_policy('test_table', 'third_index') as reorder_job_id \gset

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:reorder_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

select delete_job(:job_id);

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
-- Job args should be gone
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
-- Job args should still be there
select count(*) from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

select delete_job(:reorder_job_id);
select count(*) from _timescaledb_config.bgw_job where id=:reorder_job_id;
-- Job args should be gone
select count(*) from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

-- Now make sure policy args have correct job deletion dependency
select add_drop_chunks_policy('test_table', INTERVAL '2 month') as job_id \gset
select add_reorder_policy('test_table', 'third_index') as reorder_job_id \gset

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:reorder_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;
select * from _timescaledb_config.bgw_job;

DROP TABLE test_table;

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:reorder_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

-- Check that we can't add policies on non-hypertables
CREATE TABLE non_hypertable(junk int, more_junk int);
CREATE INDEX non_ht_index on non_hypertable(junk);

\set ON_ERROR_STOP 0
select add_drop_chunks_policy('non_hypertable', INTERVAL '2 month');
select add_reorder_policy('non_hypertable', 'non_ht_index');
\set ON_ERROR_STOP 1

-- Now make sure things work with multiple policies on multiple hypertables
CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');
CREATE INDEX second_index on test_table (time);

CREATE TABLE test_table2(time timestamptz, junk int);
SELECT create_hypertable('test_table2', 'time');
CREATE INDEX junk_index on test_table2 (junk);

select add_drop_chunks_policy('test_table', INTERVAL '2 days');
select add_drop_chunks_policy('test_table2', INTERVAL '1 days');

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks');
select * from _timescaledb_config.bgw_policy_drop_chunks;

DROP TABLE test_table;
DROP TABLE test_table_int;

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks');
select * from _timescaledb_config.bgw_policy_drop_chunks;

DROP TABLE test_table2;

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks');
select * from _timescaledb_config.bgw_policy_drop_chunks;

-- Now test chunk_stat insertion
select ts_test_chunk_stats_insert(123, 123, 45);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE _timescaledb_internal.bgw_policy_chunk_stats;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Now test chunk_stat cascade deletion is correct
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');
CREATE INDEX second_index on test_table (time);

insert into test_table values (now(), 1);
insert into test_table values (now() - INTERVAL '5 weeks', 123);

select c.id from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.hypertable_id=h.id and h.table_name='test_table' ORDER BY c.id;

select c.id as chunk_id from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.hypertable_id=h.id and h.table_name='test_table' ORDER BY c.id LIMIT 1 \gset

select add_reorder_policy('test_table', 'second_index') as job_id \gset
-- Simulate reorder job running and setting this stat row
select ts_test_chunk_stats_insert(:job_id, :chunk_id, 1);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='reorder';

-- Deleting a chunk that has nothing to do with the job should do nothing
select c.table_name as other_chunk_name,c.schema_name as other_chunk_schema from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.id != :chunk_id \gset
select concat(:'other_chunk_schema','.',:'other_chunk_name') as other_chunk \gset

DROP TABLE :other_chunk;

select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='reorder';

-- Dropping the hypertable should drop the chunk, which should drop the reorder policy
DROP TABLE test_table;
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='reorder';

-- Now check dropping a job will drop the chunk_stat row
CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');

select add_reorder_policy('test_table', 'test_table_time_idx') as job_id \gset
select add_drop_chunks_policy('test_table', INTERVAL '2 days', true);

select ts_test_chunk_stats_insert(:job_id, 123, 1);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'reorder');

-- Dropping the drop_chunks job should not affect the chunk_stats row
select remove_drop_chunks_policy('test_table');
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'reorder');

select remove_reorder_policy('test_table');
-- Row should be gone
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'reorder');

-- Now test if alter_job_schedule works
select add_reorder_policy('test_table', 'test_table_time_idx') as job_id \gset
 select * from _timescaledb_config.bgw_job where id=:job_id;
-- No change
select * from alter_job_schedule(:job_id);
-- Changes expected
select * from alter_job_schedule(:job_id, INTERVAL '3 years', INTERVAL '5 min', 5, INTERVAL '123 sec');
select * from alter_job_schedule(:job_id, INTERVAL '123 years');
select * from alter_job_schedule(:job_id, retry_period => INTERVAL '33 hours');
select * from alter_job_schedule(:job_id, max_runtime => INTERVAL '456 sec');
select * from alter_job_schedule(:job_id, max_retries => 0);
select * from alter_job_schedule(:job_id, max_retries => -1);
select * from alter_job_schedule(:job_id, max_retries => 20);

-- No change
select * from alter_job_schedule(:job_id, max_runtime => NULL);
select * from alter_job_schedule(:job_id, max_retries => NULL);

--change schedule_interval when bgw_job_stat does not exist
select * from alter_job_schedule(:job_id, schedule_interval=>'1 min');
select count(*) = 0 from _timescaledb_internal.bgw_job_stat where job_id = :job_id;
--set next_start when bgw_job_stat does not exist
select * from alter_job_schedule(:job_id, next_start=>'2001-01-01 01:01:01');
--change schedule_interval when no last_finish set
select * from alter_job_schedule(:job_id, schedule_interval=>'10 min');
--next_start overrides any schedule_interval changes
select * from alter_job_schedule(:job_id, schedule_interval=>'20 min', next_start=>'2002-01-01 01:01:01');

--set the last_finish manually
\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_internal.bgw_job_stat SET last_finish = '2003-01-01:01:01:01' WHERE job_id = :job_id;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--not changing the interval doesn't change the next_start
select * from alter_job_schedule(:job_id, schedule_interval=>'20 min');
--changing the interval changes next_start
select * from alter_job_schedule(:job_id, schedule_interval=>'30 min');
--explicit next start overrides.
select * from alter_job_schedule(:job_id, schedule_interval=>'40 min', next_start=>'2004-01-01 01:01:01');
--test pausing
select * from alter_job_schedule(:job_id, next_start=>'infinity');
--test that you can use now() to unpause
select next_start = now() from alter_job_schedule(:job_id, next_start=>now());

\set ON_ERROR_STOP 0
-- negative infinity disallowed (used as special value)
select * from alter_job_schedule(:job_id, next_start=>'-infinity');
\set ON_ERROR_STOP 1

-- Check if_exists boolean works correctly
select * from alter_job_schedule(1234, if_exists => TRUE);

\set ON_ERROR_STOP 0
select * from alter_job_schedule(1234);
\set ON_ERROR_STOP 1

select remove_reorder_policy('test_table');

\c :TEST_DBNAME :ROLE_SUPERUSER
set session timescaledb.license_key='Community';

-- Test for failure cases
\set ON_ERROR_STOP 0
select alter_job_schedule(12345);
\set ON_ERROR_STOP 1

select add_reorder_policy('test_table', 'test_table_time_idx') as reorder_job_id \gset
select add_drop_chunks_policy('test_table', INTERVAL '4 months', true) as drop_chunks_job_id \gset

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
\set ON_ERROR_STOP 0
select from alter_job_schedule(:reorder_job_id, max_runtime => NULL);
select from alter_job_schedule(:drop_chunks_job_id, max_runtime => NULL);
\set ON_ERROR_STOP 1
