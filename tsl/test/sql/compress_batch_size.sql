-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

set timescaledb.compression_batch_size_limit = 32767;

set max_parallel_workers_per_gather = 0;

create table batch_size(sb int, ts int, i2 int2, i4 int4, i8 int8, f4 float4, f8 float8,
    n numeric, u uuid, t text, b bool) with (tsdb.hypertable, tsdb.partition_column = 'ts',
        tsdb.chunk_interval = 100000)
;


insert into batch_size
select 1, x ts, x % 32768 i2, x i4, x i8, x f4, x f8, x n,
    '1d15b177-ebc8-4e9b-a3af-d2b7dc60e317' u, x t, x % 2 = 0 b
from generate_series(1, 99999) x
;

alter table batch_size set (tsdb.compress, tsdb.compress_segmentby = 'sb',
    tsdb.compress_orderby = 'ts');

select count(compress_chunk(x)) from show_chunks('batch_size') x;


insert into batch_size
select 2, ts, x % 32768 i2, x i4, x i8, x f4, x f8, x n,
    '5b5442d6-c29c-44ca-b4cf-40ab5eb49bbe' u, x t, x % 2 = 0 b
from (
    select xx ts, case when xx % 3 = 0 then null else xx end x
    from generate_series(100000, 199999) xx
) tt
;

select count(compress_chunk(x)) from show_chunks('batch_size') x;


insert into batch_size
select 3, ts, x % 32768 i2, x i4, x i8, x f4, x f8, x n,
    '3347c677-c1b3-436f-afb2-170e932e53cb' u, x t, x % 2 = 0 b
from (
    select xx ts, xx % 10 x
    from generate_series(200000, 299999) xx
) tt
;

select count(compress_chunk(x)) from show_chunks('batch_size') x;


insert into batch_size
select 4, x ts, x % 32768 i2, x i4, x i8, x f4, x f8, x n,
    '001b786c-423e-4f63-a375-479c4c368808' u, x t, x % 2 = 0 b
from generate_series(300000, 399999) x
;

alter table batch_size set (tsdb.compress,
    tsdb.compress_segmentby = 'sb, i2, i4, i8, f4, f8, n, u, t, b',
    tsdb.compress_orderby = 'ts')
;

select count(compress_chunk(x)) from show_chunks('batch_size') x;

select format('select sum(%s) from batch_size', variable)
from
    unnest(array['sb', 'ts', 'i2', 'i4', 'i8', 'f4', 'f8', 'n', 'b::int']) variable
order by 1
\gexec

select format('select count(*) from batch_size where %s > ''1''', variable)
from
    unnest(array['sb', 'ts', 'i2', 'i4', 'i8', 'f4', 'f8', 'n', 'b::int']) variable
order by 1
\gexec

select sum(i2) from batch_size group by b order by b;

select count(i4) from batch_size group by t order by count(i4) desc limit 10;

select sum(f8) from batch_size group by u, b order by sum(f8) desc limit 10;

drop table batch_size;

RESET timescaledb.compression_batch_size_limit;

reset max_parallel_workers_per_gather;


-- Test segmentwise recompression with large batches.
SET timescaledb.compression_batch_size_limit = 32767;
CREATE TABLE recompress_batch_test(segment int, ts int, value int)
    WITH (tsdb.hypertable, tsdb.partition_column = 'ts', tsdb.chunk_interval = 1000000);

INSERT INTO recompress_batch_test SELECT 1, x, x FROM generate_series(1, 99999) x;
ALTER TABLE recompress_batch_test SET (
    tsdb.compress, tsdb.compress_segmentby = 'segment', tsdb.compress_orderby = 'ts');
SELECT count(compress_chunk(x)) FROM show_chunks('recompress_batch_test') x;

INSERT INTO recompress_batch_test
    SELECT 2, x, CASE WHEN x % 3 = 0 THEN NULL ELSE x END FROM generate_series(100000, 199999) x;
SELECT count(compress_chunk(x)) FROM show_chunks('recompress_batch_test') x;

INSERT INTO recompress_batch_test SELECT 2, x, x % 10 FROM generate_series(100000, 199999) x;
SELECT count(compress_chunk(x)) FROM show_chunks('recompress_batch_test') x;

SELECT segment, count(*), sum(value) FROM recompress_batch_test GROUP BY segment ORDER BY segment;

DROP TABLE recompress_batch_test;

RESET timescaledb.compression_batch_size_limit;

