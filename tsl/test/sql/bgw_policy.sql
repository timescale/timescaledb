-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SET timescaledb.license_key='CommunityLicense';

CREATE OR REPLACE FUNCTION test_reorder(job_id INTEGER)
RETURNS TABLE(
chunk_oid INTEGER,
index_oid INTEGER
)
AS :TSL_MODULE_PATHNAME, 'ts_test_auto_reorder'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION test_drop_chunks(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_auto_drop_chunks'
LANGUAGE C VOLATILE STRICT;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE FUNCTION check_chunk_oid(chunk_id REGCLASS, chunk_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	select count(*) from pg_class as pgc, _timescaledb_catalog.chunk as c where pgc.relname=c.table_name and c.id=chunk_id and pgc.oid=chunk_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE FUNCTION check_index_oid(index_oid REGCLASS, hypertable_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	select count(*) from pg_index where indexrelid=index_oid and indrelid=hypertable_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE TABLE test_table(time timestamptz, chunk_id int);
SELECT create_hypertable('test_table', 'time');

-- These inserts should create 5 different chunks
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);

SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table';

-- Make sure reorder correctly selects chunks to reorder
-- by starting with oldest chunks
select add_reorder_policy('test_table', 'test_table_time_idx') as reorder_job_id \gset
select * from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

select * from _timescaledb_config.bgw_job where job_type IN ('reorder');
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

-- Make a manual calls to reorder: make sure the correct chunk is called
-- Chunk 5 should be first
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(5, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 3 is next
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(3, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 4 is next
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(4, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- The following calls should not reorder any chunk, because they're all too new
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

INSERT INTO test_table VALUES (now() - INTERVAL '7 days', 6);

-- This call should reorder chunk 1
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;
select check_chunk_oid(1, :reorder_chunk_oid);
select check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Should not reorder anything, because all chunks are too new
select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats;

select remove_reorder_policy('test_table');

-- Now do drop_chunks test
select add_drop_chunks_policy('test_table', INTERVAL '4 months', true) as drop_chunks_job_id \gset

SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table';

-- Now simulate drop_chunks running automatically by calling it explicitly
select test_drop_chunks(:drop_chunks_job_id);
-- Should have 4 chunks left
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_
select :before_count=4;

-- Make sure this second call does nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
select :before_count=:after_count;

INSERT INTO test_table VALUES (now() - INTERVAL '2 weeks', 1);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_

-- This call should also do nothing
select test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
select :before_count=:after_count;

select remove_drop_chunks_policy('test_table');

-- Now test reorder chunk selection when there is space partitioning
TRUNCATE test_table;
SELECT add_dimension('public.test_table', 'chunk_id', 2);

INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', -4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', -5);

select add_reorder_policy('test_table', 'test_table_time_idx') as reorder_job_id \gset
-- Should be nothing in the chunk_stats table
select count(*) from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id;

-- Make a manual calls to reorder: make sure the correct (oldest) chunk is called
select chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Now run reorder again and pick the next oldest chunk
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Ran out of chunks, so should be a noop
select * from test_reorder(:reorder_job_id) \gset  reorder_

-- Corner case: when there are no recent-enough chunks to reorder,
-- DO NOT reorder any new chunks created by space partitioning.
-- We only want to reorder when new dimension_slices on time are created.
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', -5);
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', -5);
INSERT INTO test_table VALUES (now(), -25);

-- Should be noop
select * from test_reorder(:reorder_job_id) \gset  reorder_

-- But if we create a new time dimension, reorder it
INSERT INTO test_table VALUES (now() - INTERVAL '1 year', 1);
select cc.chunk_id from _timescaledb_catalog.dimension_slice as ds, _timescaledb_catalog.chunk_constraint as cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (select chunk_id from _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

select * from test_reorder(:reorder_job_id) \gset  reorder_
select job_id, chunk_id, num_times_job_run from _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

select check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Should be noop again
select * from test_reorder(:reorder_job_id) \gset  reorder_
