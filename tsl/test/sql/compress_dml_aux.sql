-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test handling of secondary DML queries (i.e. data-modifying CTE).


create table t1(ts timestamptz, device int, value float)
with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.compress_segmentby = 'device',
    tsdb.compress_orderby = 'ts')
;

insert into t1 values
('2021-01-01 01:01:01', 1, 1),
('2022-02-02 02:02:02', 2, 2)
;


create table t2(ts timestamptz, device int, value float)
with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.compress_segmentby = 'device',
    tsdb.compress_orderby = 'ts')
;

insert into t2 values
('2021-01-01 01:01:01', 1, 1),
('2023-03-03 03:03:03', 3, 3)
;

select max(value), sum(value) from t1;

select max(value), sum(value) from t2;

-- Comment out to generate reference
select count(compress_chunk(x)) from unnest(array['t1', 't2']) t, show_chunks(t) x;

vacuum full analyze t1;

vacuum full analyze t2;


-- modifying CTE, with filter, not referenced by main query
begin;

with v as (update t2 set value = value + 1 where device = 1 returning value)
update t1 set value = value + 1 where device = 1
;

select max(value), sum(value) from t1;

select max(value), sum(value) from t2;

rollback;


-- modifying CTE, no filter, not referenced by main query
begin;

with v as (update t1 set value = value + 1 returning value)
update t2 set value = value + 1 where device = 1
;

select max(value), sum(value) from t1;

select max(value), sum(value) from t2;

rollback;


-- modifying CTE, with filter, referenced by main query
begin;

with v as (update t1 set value = value + 1 where device = 1 returning value)
update t2 set value = value + (select max(value) from v) where device = 1
;

select max(value), sum(value) from t1;

select max(value), sum(value) from t2;

rollback;


-- modifying CTE, no filter, referenced by main query
begin;

with v as (update t1 set value = value + 1 returning value)
update t2 set value = value + (select max(value) from v) where device = 1
;

select max(value), sum(value) from t1;

select max(value), sum(value) from t2;

rollback;
