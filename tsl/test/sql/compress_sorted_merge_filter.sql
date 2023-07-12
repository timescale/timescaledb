-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table batches(ts timestamp, id int);

select create_hypertable('batches', 'ts');

alter table batches set (timescaledb.compress, timescaledb.compress_segmentby = 'id');

truncate table batches;

insert into batches values ('2022-02-02 00:00:01', 1), ('2022-02-02 00:00:11', 1),
    ('2022-02-02 00:00:02', 2), ('2022-02-02 00:00:12', 2),
    ('2022-02-02 00:00:03', 3), ('2022-02-02 00:00:13', 3);

select compress_chunk(x, true) from show_chunks('batches') x;

analyze batches;

set timescaledb.debug_require_batch_sorted_merge to true;

select ts from batches where ts != '2022-02-02 00:00:02' order by ts;

set timescaledb.enable_decompression_sorted_merge to off;

select ts from batches where ts != '2022-02-02 00:00:02' order by ts;