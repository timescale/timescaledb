-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Tests for MERGE ... WHEN NOT MATCHED BY SOURCE on hypertables (PG17+).
-- A NOT MATCHED BY SOURCE action is only run by TimescaleDB when the MERGE is
-- routed through ModifyHypertable, which happens when it also has an INSERT
-- (NOT MATCHED BY TARGET) action. Each MERGE is run against both a plain table
-- and a hypertable and the results are compared.

-- Exact reproduction from the issue: the NOT MATCHED BY SOURCE DELETE must
-- remove the unmatched target row instead of being silently skipped.
create table merge_plain (ts timestamptz not null, k int, v int);
create table merge_ht (ts timestamptz not null, k int, v int);
select create_hypertable('merge_ht', 'ts', chunk_time_interval => interval '100 days');
insert into merge_plain values ('2024-01-01', 1, 10), ('2024-01-01', 2, 20);
insert into merge_ht select * from merge_plain;

create table merge_src (k int, v int);
insert into merge_src values (1, 100), (3, 300);

merge into merge_plain t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values ('2024-01-01', s.k, s.v)
when not matched by source then delete;
merge into merge_ht t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values ('2024-01-01', s.k, s.v)
when not matched by source then delete;

select k, v from merge_ht order by k;
select case when exists (table merge_ht except table merge_plain)
              or exists (table merge_plain except table merge_ht)
            then 'different' else 'same' end as result;

drop table merge_plain, merge_ht, merge_src;

-- All three match kinds together, with a conditional NOT MATCHED BY SOURCE,
-- spanning multiple chunks.
create table target_pg (time timestamptz not null, location int, temperature int);
create table target_ht (time timestamptz not null, location int, temperature int);
select create_hypertable('target_ht', 'time', chunk_time_interval => interval '1 day');
insert into target_pg
select time, location, 14
from generate_series('2021-01-01'::timestamptz, '2021-01-04', interval '1 day') time,
     generate_series(1, 4) location;
insert into target_ht select * from target_pg;

-- Source matches some target rows, misses others, and adds a new one.
create table source (location int, temperature int);
insert into source values (1, 80), (2, 80), (5, 80);

merge into target_pg t using source s on t.location = s.location
when matched then update set temperature = (t.temperature + s.temperature) / 2
when not matched by target then insert (time, location, temperature)
     values ('2021-01-01', s.location, s.temperature)
when not matched by source and t.location = 3 then delete
when not matched by source then update set temperature = 99;
merge into target_ht t using source s on t.location = s.location
when matched then update set temperature = (t.temperature + s.temperature) / 2
when not matched by target then insert (time, location, temperature)
     values ('2021-01-01', s.location, s.temperature)
when not matched by source and t.location = 3 then delete
when not matched by source then update set temperature = 99;

select case when exists (table target_ht except table target_pg)
              or exists (table target_pg except table target_ht)
            then 'different' else 'same' end as result;

drop table target_pg, target_ht, source;

-- INSERT takes the partitioning column from a source column.
create table merge_plain (ts timestamptz not null, k int, v int);
create table merge_ht (ts timestamptz not null, k int, v int);
select create_hypertable('merge_ht', 'ts', chunk_time_interval => interval '1 day');
insert into merge_plain values ('2024-01-01', 1, 10), ('2024-01-02', 2, 20), ('2024-01-03', 3, 30);
insert into merge_ht select * from merge_plain;

create table merge_src (ts timestamptz, k int, v int);
insert into merge_src values ('2024-02-01', 1, 100), ('2024-02-05', 9, 900);

merge into merge_plain t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values (s.ts, s.k, s.v)
when not matched by source then delete;
merge into merge_ht t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values (s.ts, s.k, s.v)
when not matched by source then delete;

select case when exists (table merge_ht except table merge_plain)
              or exists (table merge_plain except table merge_ht)
            then 'different' else 'same' end as result;

drop table merge_plain, merge_ht, merge_src;

-- Empty source: every target row is NOT MATCHED BY SOURCE.
create table merge_plain (ts timestamptz not null, k int, v int);
create table merge_ht (ts timestamptz not null, k int, v int);
select create_hypertable('merge_ht', 'ts', chunk_time_interval => interval '1 day');
insert into merge_plain values ('2024-01-01', 1, 10), ('2024-01-02', 2, 20), ('2024-01-03', 3, 30);
insert into merge_ht select * from merge_plain;

create table merge_src (ts timestamptz, k int, v int);

merge into merge_plain t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values (s.ts, s.k, s.v)
when not matched by source then delete;
merge into merge_ht t using merge_src s on t.k = s.k
when not matched by target then insert (ts, k, v) values (s.ts, s.k, s.v)
when not matched by source then delete;

select count(*) as merge_ht_rows from merge_ht;
select case when exists (table merge_ht except table merge_plain)
              or exists (table merge_plain except table merge_ht)
            then 'different' else 'same' end as result;

drop table merge_plain, merge_ht, merge_src;
