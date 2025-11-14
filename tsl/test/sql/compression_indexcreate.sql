-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- force index scan to be used when possible
set enable_seqscan to false;
\set PREFIX 'EXPLAIN (analyze, buffers off, costs off, summary off, timing off) '
create table segind(time timestamptz, a int, b int);
select create_hypertable('segind', by_range('time'));

-- enable compression on hypertable with no segment by column
alter table segind set (timescaledb.compress, timescaledb.compress_segmentby='', timescaledb.compress_orderby='time, b');
insert into segind values('2024-11-08 10:31:28.436014-07', 1, 1), ('2024-11-08 10:32:28.436014-07', 2, 1), ('2024-11-08 10:33:28.436014-07', 3, 1), ('2024-11-08 10:34:28.436014-07', 2, 1), ('2024-11-08 10:35:28.436014-07', 1, 2), ('2024-11-08 10:36:28.436014-07', 4, 1);

-- compress chunk
-- this should create an index using orderby columns
select compress_chunk(show_chunks('segind'));
vacuum analyze segind;

-- query using orderby columns should use the index
:PREFIX select * from segind where b = 1;
:PREFIX select * from segind where time = '2024-11-08 10:32:28.436014-07';
:PREFIX select * from segind where b = 1 and time = '2024-11-08 10:32:28.436014-07';

-- a query on another column should perform a seq scan since there is no index on it
:PREFIX select * from segind where a = 1;

-- decompress the chunk to drop the index
select decompress_chunk(show_chunks('segind'));

-- change compression settings to use segmentby column
alter table segind set (timescaledb.compress, timescaledb.compress_segmentby='a', timescaledb.compress_orderby='time, b');

-- compress chunk
-- this should create an index using segmentby and orderby columns
select compress_chunk(show_chunks('segind'));
vacuum analyze segind;

-- queries using segmentby or orderby columns should use the index
:PREFIX select * from segind where b = 1;
:PREFIX select * from segind where time = '2024-11-08 10:32:28.436014-07';
:PREFIX select * from segind where b = 1 and time = '2024-11-08 10:32:28.436014-07';
:PREFIX select * from segind where a = 1;

-- cleanup
RESET enable_seqscan;
