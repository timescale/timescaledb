-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table batches(ts timestamp, id int);

select create_hypertable('batches', 'ts');

alter table batches set (timescaledb.compress, timescaledb.compress_segmentby = 'id');

insert into batches values ('2022-02-02 00:00:01', 1), ('2022-02-02 00:00:11', 1),
    ('2022-02-02 00:00:02', 2), ('2022-02-02 00:00:12', 2),
    ('2022-02-02 00:00:03', 3), ('2022-02-02 00:00:13', 3);

select compress_chunk(x, true) from show_chunks('batches') x;

analyze batches;

set timescaledb.debug_require_batch_sorted_merge to true;

select ts from batches where ts != '2022-02-02 00:00:02' order by ts;

-- check that the GUC actually works
\set ON_ERROR_STOP off
set timescaledb.enable_decompression_sorted_merge to off;
select ts from batches where ts != '2022-02-02 00:00:02' order by ts;
reset timescaledb.enable_decompression_sorted_merge;
\set ON_ERROR_STOP on


-- middle batch entirely filtered out
truncate table batches;

alter table batches add column filter bool;

insert into batches values ('2022-02-02 00:00:01', 1, true), ('2022-02-02 00:00:11', 1, true),
    ('2022-02-02 00:00:02', 2, true), ('2022-02-02 00:00:12', 2, true),
    ('2022-02-02 00:00:03', 3, false), ('2022-02-02 00:00:13', 3, false);

select compress_chunk(x, true) from show_chunks('batches') x;

analyze batches;

select ts from batches where filter order by ts;


-- last batch entirely filtered out
truncate table batches;

insert into batches values ('2022-02-02 00:00:01', 1, true), ('2022-02-02 00:00:02', 1, true),
    ('2022-02-02 00:00:03', 2, true), ('2022-02-02 00:00:04', 2, true),
    ('2022-02-02 00:00:05', 3, false), ('2022-02-02 00:00:06', 3, false);

select compress_chunk(x, true) from show_chunks('batches') x;

analyze batches;

select ts from batches where filter order by ts;


-- first batch entirely filtered out
truncate table batches;

insert into batches values ('2022-02-02 00:00:01', 1, false), ('2022-02-02 00:00:02', 1, false),
    ('2022-02-02 00:00:03', 2, true), ('2022-02-02 00:00:04', 2, true),
    ('2022-02-02 00:00:05', 3, true), ('2022-02-02 00:00:06', 3, true);

select compress_chunk(x, true) from show_chunks('batches') x;

analyze batches;

select ts from batches where filter order by ts;


-- Test case for EquivalenceMember from a join rel (with multiple em_relids).
select * from batches
join (
    select coalesce(t2.t, t1.t) t
    from (select now() + random() * interval '1s' t) t1
    left join (select now() + random() * interval '1s' t) t2
    on t1 = t2
) t3
on batches.ts = t3.t
order by ts;
