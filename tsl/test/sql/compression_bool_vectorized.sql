-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

drop table if exists t1;
set timescaledb.enable_bool_compression = on;
create table t1 (ts int, b bool);
select create_hypertable('t1','ts');
alter table t1 set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into t1 values (1, true);
insert into t1 values (2, false);
insert into t1 values (3, NULL);
insert into t1 values (4, true);
insert into t1 values (5, false);
insert into t1 values (6, NULL);
select compress_chunk(show_chunks('t1'));

select * from t1 order by 1;

select * from t1 where b is null order by 1;
select * from t1 where b = true order by 1;
select * from t1 where b = false order by 1;

select * from t1 where ts > 3 and b is null order by 1;
select * from t1 where ts > 3 and b = true order by 1;
select * from t1 where ts > 3 and b = false order by 1;

-- delete all null values and compress again
delete from t1 where b is null;
select compress_chunk(show_chunks('t1'));

select * from t1 order by 1;

select * from t1 where b is null order by 1;
select * from t1 where b = true order by 1;
select * from t1 where b = false order by 1;

select * from t1 where ts > 3 and b is null order by 1;
select * from t1 where ts > 3 and b = true order by 1;
select * from t1 where ts > 3 and b = false order by 1;
