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

CREATE OR REPLACE FUNCTION test_scheduled_index(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_scheduled_index'
LANGUAGE C VOLATILE STRICT;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE FUNCTION check_chunk_oid(chunk_id REGCLASS, chunk_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	SELECT count(*) FROM pg_class AS pgc, _timescaledb_catalog.chunk AS c where pgc.relname=c.table_name and c.id=chunk_id and pgc.oid=chunk_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE FUNCTION check_index_oid(index_oid REGCLASS, hypertable_oid REGCLASS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	count INTEGER;
BEGIN
	SELECT count(*) FROM pg_index where indexrelid=index_oid and indrelid=hypertable_oid INTO count;
	return (count = 1);
END
$BODY$;

CREATE TABLE test_table(time timestamptz, chunk_id int);
SELECT create_hypertable('test_table', 'time', create_default_indexes => false);

-- These inserts should create 5 different chunks
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);

SELECT COUNT(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table';

CREATE INDEX ON test_table(time) WITH (timescaledb.scheduled);

SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

SELECT * FROM _timescaledb_config.bgw_policy_scheduled_index;
SELECT job_id AS scheduled_index_job_id FROM _timescaledb_config.bgw_policy_scheduled_index LIMIT 1 \gset

\set ON_ERROR_STOP 0
SELECT add_scheduled_index_policy('test_table', 'test_table_time_idx') AS scheduled_index_job_id \gset
\set ON_ERROR_STOP 1

-- Make sure scheduled_index correctly SELECTs chunks to scheduled_index
-- by starting with oldest chunks
SELECT * FROM _timescaledb_config.bgw_job WHERE job_type IN ('scheduled_index');
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Make a manual calls to scheduled_index: make sure the correct chunk is called
-- Chunk 5 should be first
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

-- Confirm that scheduled_index was called on the correct chunk Oid
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Chunk 3 is next
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Chunk 4 is next
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- The following calls should not scheduled_index any chunk, because they're all too new
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

INSERT INTO test_table VALUES (now() - INTERVAL '7 days', 6);

SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- This call should scheduled_index chunk 1
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- Should not scheduled_index anything, because all chunks are too new
SELECT * FROM test_scheduled_index(:scheduled_index_job_id) \gset  scheduled_index_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

SELECT * FROM timescaledb_information.scheduled_index_policies;

-- indexes should still exist after removing the policy
SELECT remove_scheduled_index_policy('test_table');
SELECT * FROM test.show_indexes('test_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- but optional info should not exist
SELECT * FROM _timescaledb_catalog.optional_index_info;

-- Make sure reorder correctly SELECTs chunks to reorder
-- by starting with oldest chunks
SELECT add_reorder_policy('test_table', 'test_table_time_idx') AS reorder_job_id \gset
SELECT * FROM _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

SELECT * FROM _timescaledb_config.bgw_job where job_type IN ('reorder');
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

-- Make a manual calls to reorder: make sure the correct chunk is called
-- Chunk 5 should be first
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

-- Confirm that reorder was called on the correct chunk Oid
SELECT check_chunk_oid(5, :reorder_chunk_oid);
SELECT check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 3 is next
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT check_chunk_oid(3, :reorder_chunk_oid);
SELECT check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Chunk 4 is next
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT check_chunk_oid(4, :reorder_chunk_oid);
SELECT check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- The following calls should not reorder any chunk, because they're all too new
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

INSERT INTO test_table VALUES (now() - INTERVAL '7 days', 6);

-- This call should reorder chunk 1
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;
SELECT check_chunk_oid(1, :reorder_chunk_oid);
SELECT check_index_oid(:reorder_index_oid, 'test_table'::REGCLASS);

-- Should not reorder anything, because all chunks are too new
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats;

SELECT remove_reorder_policy('test_table');

-- Now do drop_chunks test
SELECT add_drop_chunks_policy('test_table', INTERVAL '4 months', true) AS drop_chunks_job_id \gset

SELECT count(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table';

-- Now simulate drop_chunks running automatically by calling it explicitly
SELECT test_drop_chunks(:drop_chunks_job_id);
-- Should have 4 chunks left
SELECT count(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_
SELECT :before_count=4;

-- Make sure this second call does nothing
SELECT test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
SELECT :before_count=:after_count;

INSERT INTO test_table VALUES (now() - INTERVAL '2 weeks', 1);
SELECT count(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset before_

-- This call should also do nothing
SELECT test_drop_chunks(:drop_chunks_job_id);
SELECT count(*) FROM _timescaledb_catalog.chunk AS c, _timescaledb_catalog.hypertable AS ht where c.hypertable_id = ht.id and ht.table_name='test_table' \gset after_

-- Should be true
SELECT :before_count=:after_count;

SELECT remove_drop_chunks_policy('test_table');

-- Now test reorder chunk SELECTion when there is space partitioning
TRUNCATE test_table;
SELECT add_dimension('public.test_table', 'chunk_id', 2);

INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', 1);
INSERT INTO test_table VALUES (now(), 2);
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', 3);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', 4);
INSERT INTO test_table VALUES (now() - INTERVAL '3 months', -4);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', 5);
INSERT INTO test_table VALUES (now() - INTERVAL '8 months', -5);

SELECT add_reorder_policy('test_table', 'test_table_time_idx') AS reorder_job_id \gset
-- Should be nothing in the chunk_stats table
SELECT count(*) FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id;

-- Make a manual calls to reorder: make sure the correct (oldest) chunk is called
SELECT chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Now run reorder again and pick the next oldest chunk
SELECT cc.chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (SELECT chunk_id FROM _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

-- Confirm that reorder was called on the correct chunk Oid
SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
SELECT cc.chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (SELECT chunk_id FROM _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
SELECT cc.chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (SELECT chunk_id FROM _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Again
SELECT cc.chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (SELECT chunk_id FROM _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Ran out of chunks, so should be a noop
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_

-- Corner case: when there are no recent-enough chunks to reorder,
-- DO NOT reorder any new chunks created by space partitioning.
-- We only want to reorder when new dimension_slices on time are created.
INSERT INTO test_table VALUES (now() - INTERVAL '5 months', -5);
INSERT INTO test_table VALUES (now() - INTERVAL '3 weeks', -5);
INSERT INTO test_table VALUES (now(), -25);

-- Should be noop
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_

-- But if we create a new time dimension, reorder it
INSERT INTO test_table VALUES (now() - INTERVAL '1 year', 1);
SELECT cc.chunk_id FROM _timescaledb_catalog.dimension_slice AS ds, _timescaledb_catalog.chunk_constraint AS cc where ds.dimension_id=1 and ds.id=cc.dimension_slice_id and cc.chunk_id NOT IN (SELECT chunk_id FROM _timescaledb_internal.bgw_policy_chunk_stats) ORDER BY ds.range_start LIMIT 1 \gset oldest_

SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_
SELECT job_id, chunk_id, num_times_job_run FROM _timescaledb_internal.bgw_policy_chunk_stats where job_id=:reorder_job_id and chunk_id=:oldest_chunk_id;

SELECT check_chunk_oid(:oldest_chunk_id, :reorder_chunk_oid);

-- Should be noop again
SELECT * FROM test_reorder(:reorder_job_id) \gset  reorder_

CREATE TABLE test_table_int(time int);
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);

\set ON_ERROR_STOP 0
-- we cannot add a drop_chunks policy on a table whose open dimension is not time
SELECT add_drop_chunks_policy('test_table_int', INTERVAL '4 months', true);
\set ON_ERROR_STOP 1
