-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- github issue 5585
create table test (
    time timestamptz not null,
    a varchar(255) not null,
    b int,
    c int
);

SELECT create_hypertable('test', 'time');

insert into test values
('2020-01-01 00:00'::timestamptz, 'lat', 1, 2),
('2020-01-01 00:01'::timestamptz, 'lat', 1, 2),
('2020-01-01 00:01'::timestamptz, 'lat', 2, 2),
('2020-01-01 00:03'::timestamptz, 'lat', 1, 2),
('2020-01-01 00:01'::timestamptz, 'lon', 1, 2);

create table test_copy as select * from test;

-- compress the chunk
alter table test set (timescaledb.compress, timescaledb.compress_segmentby='a, b');
select compress_chunk(show_chunks('test'));
-- force an index scan
set enable_seqscan = 'off';
-- disable jit to avoid test flakiness
set jit = off;

explain (costs off) with query_params as (
	select distinct a, b
	from test_copy
	where test_copy.a IN ('lat', 'lon')
	   and test_copy.b IN (1)
)
select
    test.time,
    test.a = q.a as "this should never be false",
    test.a,
    test.b,
    test.c,
    q.*
from
test inner join query_params q
    on q.a = test.a and q.b = test.b
where test.time between '2020-01-01 00:00' and '2020-01-01 00:02'
order by test.time;

with query_params as (
	select distinct a, b
	from test_copy
	where test_copy.a IN ('lat', 'lon')
	   and test_copy.b IN (1)
)
select
    test.time,
    test.a = q.a as "this should never be false",
    test.a,
    test.b,
    test.c,
    q.*
from
test inner join query_params q
    on q.a = test.a and q.b = test.b
where test.time between '2020-01-01 00:00' and '2020-01-01 00:02'
order by test.time;

reset enable_seqscan;
reset jit;

drop table test;
drop table test_copy;
