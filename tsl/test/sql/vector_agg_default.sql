-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table t(a int);
select create_hypertable('t', 'a', chunk_time_interval => 1000);

insert into t select generate_series(1, 999);
alter table t set (timescaledb.compress);
select compress_chunk(show_chunks('t'));

alter table t add column b int default 7;
insert into t select x, 11 from generate_series(1001, 1999) x;
select compress_chunk(show_chunks('t'));

explain (costs off) select sum(b) from t;
select sum(b) from t;

select decompress_chunk(show_chunks('t'));
select sum(b) from t;

drop table t;
