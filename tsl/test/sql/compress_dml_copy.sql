-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test various cases of COPY with decompression and different chunk layouts.

create table cdmlcopy(filler bigint, ts int, value float, metric text);

select create_hypertable('cdmlcopy', 'ts', chunk_time_interval => 1000);

alter table cdmlcopy add unique (metric, ts);

alter table cdmlcopy set (timescaledb.compress, timescaledb.compress_segmentby = 'metric');

\copy cdmlcopy from stdin
0	1	1.1	'metric1'
0	1	1.2	'metric2'
\.

select count(compress_chunk(x)) from show_chunks('cdmlcopy') x;

alter table cdmlcopy drop column filler;

\copy cdmlcopy from stdin
1001	1.1	'metric1'
1001	1.2	'metric2'
\.

select count(compress_chunk(x)) from show_chunks('cdmlcopy') x;

\set ON_ERROR_STOP 0
\copy cdmlcopy from stdin
1	1.1	'metric1'
1	1.2	'metric2'
\.

\copy cdmlcopy from stdin
1001	1.1	'metric1'
1001	1.2	'metric2'
\.
\set ON_ERROR_STOP 1


-- Also test the code path where the chunk insert state goes out of cache.
set timescaledb.max_open_chunks_per_insert = 1;

select count(compress_chunk(x)) from show_chunks('cdmlcopy') x;

\copy cdmlcopy from stdin
2	2.1	'metric1'
1002	2.2	'metric2'
2	2.2	'metric2'
1002	2.1	'metric1'
\.

reset timescaledb.max_open_chunks_per_insert;

select count(compress_chunk(x)) from show_chunks('cdmlcopy') x;

drop table cdmlcopy;
