-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

select setseed(0.3);
create table hyperdump (time timestamptz not null, device int, tempc float, tempf float generated always as (tempc * 2 + 30) stored, status text default 'idle');
select create_hypertable('hyperdump', by_range('time'), create_default_indexes => false);

insert into hyperdump (time, device, tempc)
select t, ceil(random()*10), random()*60
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t;

create index time_device_idx on hyperdump (device, time desc);

alter table hyperdump set (
      timescaledb.compress_orderby='time',
      timescaledb.compress_segmentby='device');

\set TEST_BASE_NAME hypercore_pgdump

SELECT
    format('%s/results/%s_results_original.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_ORIGINAL",
    format('%s/results/%s_results_restored.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_RESTORED" \gset

-- Save uncompressed table query output as a reference to compare against
\o :TEST_RESULTS_ORIGINAL
select * from hyperdump order by time, device;
\o

explain (costs off)
select * from hyperdump where device = 2 and time < '2022-06-03';

-- Convert to hypercore
select compress_chunk(ch, hypercore_use_access_method=>true) from show_chunks('hyperdump') ch;

reindex table hyperdump;
explain (costs off)
select * from hyperdump where device = 2 and time < '2022-06-03';

\set ON_ERROR_STOP 0
-- Test unsupported GUC values
set timescaledb.hypercore_copy_to_behavior=0;
set timescaledb.hypercore_copy_to_behavior=null;
set timescaledb.hypercore_copy_to_behavior='dummy';
\set ON_ERROR_STOP 1

set timescaledb.hypercore_copy_to_behavior='all_data';

select chunk from show_chunks('hyperdump') chunk offset 2 limit 1 \gset
\d+ :chunk

SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id) offset 2 limit 1 \gset

-- This should not return any data when in this COPY mode.
copy :cchunk to stdout;

---
-- Create a "compressed" dump where only uncompressed data is
-- returned dumped via the TAM relation. The rest of the data is
-- dumped via the internal compressed relation. This is compatible
-- with compression without TAM.
--
-- When restoring from the compressed dump, it will create hypercore
-- relations that are also compressed.
--
\c postgres :ROLE_SUPERUSER
\! utils/pg_dump_aux_dump.sh dump/hypercore-dump-compress.sql -ctimescaledb.hypercore_copy_to_behavior='no_compressed_data'
\c :TEST_DBNAME
create extension timescaledb;
select timescaledb_pre_restore();
\! utils/pg_dump_aux_restore.sh dump/hypercore-dump-compress.sql

select timescaledb_post_restore();
-- Background workers restarted by post restore, so stop again
select _timescaledb_functions.stop_background_workers();

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

\o :TEST_RESULTS_RESTORED
select * from hyperdump order by time, device;
\o

select chunk from show_chunks('hyperdump') chunk offset 2 limit 1 \gset
\d+ hyperdump
\d+ :chunk
explain (costs off)
select * from hyperdump where time < '2022-06-03';
reindex table hyperdump;
explain (costs off)
select * from hyperdump where time < '2022-06-03';

select format('\! diff -u --label "hypercore original" --label "hypercore restored" %s %s', :'TEST_RESULTS_ORIGINAL', :'TEST_RESULTS_RESTORED') as "DIFF_CMD" \gset

-- Original output and restored output should be the same, i.e., no
-- diff
:DIFF_CMD

SELECT format('%s/results/%s_results_restored_2.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_RESTORED" \gset

reindex table hyperdump;
explain (costs off)
select * from hyperdump where device = 2 and time < '2022-06-03';


---
-- Create an "uncompressed" dump where _all_ data is dumped via the
-- TAM relation. No data is dumped via the internal compressed
-- relation. This dump is compatible with uncompressed hypertables.
--
-- When restoring from the uncompressed dump, it will create
-- hypercore relations that are also uncompressed.
--
\c postgres :ROLE_SUPERUSER
\! utils/pg_dump_aux_dump.sh dump/hypercore-dump-uncompress.sql -ctimescaledb.hypercore_copy_to_behavior='all_data'
\c :TEST_DBNAME
create extension timescaledb;
select timescaledb_pre_restore();
\! utils/pg_dump_aux_restore.sh dump/hypercore-dump-uncompress.sql

select timescaledb_post_restore();
-- Background workers restarted by post restore, so stop again
select _timescaledb_functions.stop_background_workers();

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

select chunk from show_chunks('hyperdump') chunk offset 2 limit 1 \gset

-- If restore is OK, and TAM is used, we should see a ColumnarScan
explain (costs off)
select * from hyperdump order by time, device limit 10;

--
-- After restore, the status of the compressed chunks should be
-- partial since, with an uncompressed dump, the restore inserts data
-- via the hypercore relation in uncompressed form.
select c1.table_name, c1.status from _timescaledb_catalog.chunk c1
join _timescaledb_catalog.chunk c2 on (c1.compressed_chunk_id = c2.id)
order by c2.table_name;

-- Check that a compressed chunk holds no data
SELECT format('%I.%I', c2.schema_name, c2.table_name)::regclass AS cchunk
FROM _timescaledb_catalog.chunk c1
INNER JOIN _timescaledb_catalog.chunk c2
ON (c1.compressed_chunk_id = c2.id) offset 2 limit 1 \gset

-- Compressed relation should hold no data
select count(*) from :cchunk;

-- Compress all chunks
select compress_chunk(ch) from show_chunks('hyperdump') ch;
-- Data should now be compressed
select count(*) from :cchunk;

-- Output data to a file for comparison with original contents
\o :TEST_RESULTS_RESTORED
select * from hyperdump order by time, device;
\o

select format('\! diff -u --label "hypercore original" --label "hypercore restored" %s %s', :'TEST_RESULTS_ORIGINAL', :'TEST_RESULTS_RESTORED') as "DIFF_CMD" \gset

-- Outputs should be the same, i.e., no diff
:DIFF_CMD
