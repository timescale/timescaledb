-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

\c single :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SET timescaledb.license_key='CommunityLicense';

CREATE OR REPLACE FUNCTION ts_test_chunk_stats_insert(job_id INTEGER, chunk_id INTEGER, num_times_run INTEGER, last_time_run TIMESTAMPTZ = NULL) RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION delete_job(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_bgw_job_delete_by_id'
LANGUAGE C VOLATILE STRICT;

\c single :ROLE_DEFAULT_PERM_USER

select * from _timescaledb_config.bgw_policy_drop_chunks;
select * from _timescaledb_config.bgw_policy_recluster;

CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');
CREATE INDEX second_index on test_table (time);
CREATE INDEX third_index on test_table (time);

select add_recluster_policy('test_table', 'test_table_time_idx') as job_id \gset
-- Noop for duplicate policy
select add_recluster_policy('test_table', 'test_table_time_idx', true);
select add_recluster_policy('test_table', 'second_index', true);
select add_recluster_policy('test_table', 'third_index', true);

\set ON_ERROR_STOP 0
-- Error whenever incorrect arguments are applied (must have table and index)
select add_recluster_policy('test_table', 'bad_index');
select add_recluster_policy('test_table', '');
select add_recluster_policy('test_table');

select add_recluster_policy('test_table', 'second_index');
select add_recluster_policy('test_table', 'third_index');
\set ON_ERROR_STOP 1

select * from _timescaledb_config.bgw_policy_recluster where job_id=:job_id;

-- Now check that default scheduling interval for recluster policy is calculated correctly
-- Should be 1/2 default chunk interval length
CREATE TABLE test_table2(time timestamptz, junk int);
SELECT create_hypertable('test_table2', 'time', chunk_time_interval=>INTERVAL '1 day');
select add_recluster_policy('test_table2', 'test_table2_time_idx');

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'recluster');

DROP TABLE test_table2;
-- Make sure that test_table2 recluster policy gets dropped
select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'recluster');

-- Error whenever incorrect arguments are applied (must have table and interval)
\set ON_ERROR_STOP 0
select add_drop_chunks_policy();
select add_drop_chunks_policy('test_table');
select add_drop_chunks_policy(INTERVAL '3 hours');
select add_drop_chunks_policy('fake_table', INTERVAL '3 month', true);
select add_drop_chunks_policy('fake_table', INTERVAL '3 month');
select add_drop_chunks_policy('test_table', cascade=>true);
\set ON_ERROR_STOP 1

select add_drop_chunks_policy('test_table', INTERVAL '3 month', true);
-- add_*_policy should be noop only for policies with the exact same parameters
select add_drop_chunks_policy('test_table', INTERVAL '3 month', true, true);
-- Should not add new policy with different parameters
select add_drop_chunks_policy('test_table', INTERVAL '3 month', false, true);
select add_drop_chunks_policy('test_table', INTERVAL '1 year', if_not_exists => true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', if_not_exists => true);
select add_drop_chunks_policy('test_table', INTERVAL '3 days', true, if_not_exists => true);

\set ON_ERROR_STOP 0
select add_drop_chunks_policy('test_table', INTERVAL '3 month', false);
select add_drop_chunks_policy('test_table', INTERVAL '1 year');
select add_drop_chunks_policy('test_table', INTERVAL '3 days');
select add_drop_chunks_policy('test_table', INTERVAL '3 days', true);
\set ON_ERROR_STOP 1

select * from _timescaledb_config.bgw_policy_drop_chunks;
select r.job_id,r.hypertable_id,r.older_than,r.cascade from _timescaledb_config.bgw_policy_drop_chunks as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';

select remove_drop_chunks_policy('test_table');

select * from _timescaledb_config.bgw_policy_drop_chunks;
select r.job_id,r.hypertable_id,r.older_than,r.cascade from _timescaledb_config.bgw_policy_drop_chunks as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';
select remove_recluster_policy('test_table');

select * from _timescaledb_config.bgw_policy_recluster;
select r.job_id,r.hypertable_id,r.hypertable_index_name from _timescaledb_config.bgw_policy_recluster as r, _timescaledb_catalog.hypertable as h where r.hypertable_id=h.id and h.table_name='test_table';

select add_drop_chunks_policy('test_table', INTERVAL '3 month');
select * from _timescaledb_config.bgw_policy_drop_chunks;
select remove_drop_chunks_policy('test_table');
select * from _timescaledb_config.bgw_policy_drop_chunks;

-- Make sure remove works when there's nothing to remove
select remove_drop_chunks_policy('test_table', true);
select remove_recluster_policy('test_table', true);

\set ON_ERROR_STOP 0
select remove_drop_chunks_policy();
select remove_drop_chunks_policy('fake_table');
select remove_recluster_policy();
select remove_recluster_policy('fake_table');
\set ON_ERROR_STOP 1

\set ON_ERROR_STOP 0
-- This should be noop
select remove_recluster_policy(2, true);
-- Fail with error message
select remove_recluster_policy(2);
\set ON_ERROR_STOP 1

-- Now make sure policy args have correct job deletion dependency
select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks', 'recluster');

select add_drop_chunks_policy('test_table', INTERVAL '2 month') as job_id \gset
select add_recluster_policy('test_table', 'third_index') as recluster_job_id \gset

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:recluster_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_recluster where job_id=:recluster_job_id;

select delete_job(:job_id);

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
-- Job args should be gone
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
-- Job args should still be there
select count(*) from _timescaledb_config.bgw_policy_recluster where job_id=:recluster_job_id;

select delete_job(:recluster_job_id);
select count(*) from _timescaledb_config.bgw_job where id=:recluster_job_id;
-- Job args should be gone
select count(*) from _timescaledb_config.bgw_policy_recluster where job_id=:recluster_job_id;

-- Now make sure policy args have correct job deletion dependency
select add_drop_chunks_policy('test_table', INTERVAL '2 month') as job_id \gset
select add_recluster_policy('test_table', 'third_index') as recluster_job_id \gset

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:recluster_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_recluster where job_id=:recluster_job_id;
select * from _timescaledb_config.bgw_job;

DROP TABLE test_table;

select count(*) from _timescaledb_config.bgw_job where id=:job_id;
select count(*) from _timescaledb_config.bgw_job where id=:recluster_job_id;
select count(*) from _timescaledb_config.bgw_policy_drop_chunks where job_id=:job_id;
select count(*) from _timescaledb_config.bgw_policy_recluster where job_id=:recluster_job_id;

-- Check that we can't add policies on non-hypertables
CREATE TABLE non_hypertable(junk int, more_junk int);
CREATE INDEX non_ht_index on non_hypertable(junk);

\set ON_ERROR_STOP 0
select add_drop_chunks_policy('non_hypertable', INTERVAL '2 month');
select add_recluster_policy('non_hypertable', 'non_ht_index');
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

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks');
select * from _timescaledb_config.bgw_policy_drop_chunks;

DROP TABLE test_table2;

select * from _timescaledb_config.bgw_job where job_type IN ('drop_chunks');
select * from _timescaledb_config.bgw_policy_drop_chunks;

-- Now test chunk_stat insertion
select ts_test_chunk_stats_insert(123, 123, 45);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

\c single :ROLE_SUPERUSER
TRUNCATE _timescaledb_internal.bgw_policy_chunk_stats;
\c single :ROLE_DEFAULT_PERM_USER

-- Now test chunk_stat cascade deletion is correct
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');
CREATE INDEX second_index on test_table (time);

insert into test_table values (now(), 1);
insert into test_table values (now() - INTERVAL '5 weeks', 123);

select c.id from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.hypertable_id=h.id and h.table_name='test_table';

select c.id as chunk_id from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.hypertable_id=h.id and h.table_name='test_table' LIMIT 1 \gset

select add_recluster_policy('test_table', 'second_index') as job_id \gset
-- Simulate recluster job running and setting this stat row
select ts_test_chunk_stats_insert(:job_id, :chunk_id, 1);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='recluster';

-- Deleting a chunk that has nothing to do with the job should do nothing
select c.table_name as other_chunk_name,c.schema_name as other_chunk_schema from _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as h where c.id != :chunk_id \gset
select concat(:'other_chunk_schema','.',:'other_chunk_name') as other_chunk \gset

DROP TABLE :other_chunk;

select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='recluster';

-- Dropping the hypertable should drop the chunk, which should drop the recluster policy
DROP TABLE test_table;
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type='recluster';

-- Now check dropping a job will drop the chunk_stat row
CREATE TABLE test_table(time timestamptz, junk int);
SELECT create_hypertable('test_table', 'time');

select add_recluster_policy('test_table', 'test_table_time_idx') as job_id \gset
select add_drop_chunks_policy('test_table', INTERVAL '2 days', true);

select ts_test_chunk_stats_insert(:job_id, 123, 1);
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'recluster');

-- Dropping the drop_chunks job should not affect the chunk_stats row
select remove_drop_chunks_policy('test_table');
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'recluster');

select remove_recluster_policy('test_table');
-- Row should be gone
select job_id,chunk_id,num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select * from _timescaledb_config.bgw_job where job_type in ('drop_chunks', 'recluster');
