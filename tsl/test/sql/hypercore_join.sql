-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

-- We disable columnar scan for these tests since we have a dedicated
-- test for this.
set timescaledb.enable_columnarscan to false;

set enable_memoize to false;

-- Create a hypercore with a few rows and use the big table to join
-- with it. This should put the hypercore as the inner relation and
-- trigger rescans.
create table the_hypercore (
       updated_at timestamptz not null unique,
       device_id int,
       height float
);
create index on the_hypercore (device_id);
select from create_hypertable('the_hypercore', 'updated_at');

-- Fill the table with some data, but less than a single chunk, so
-- that we will get it as an inner relation in the nested loop join.
insert into the_hypercore
select t, ceil(random()*5), random()*40
from generate_series('2022-06-01'::timestamptz, '2022-06-10', '1 hour') t;

-- Run joins before making it a hypercore to have something to
-- compare with.
select * into expected_inner from :chunk1 join the_hypercore using (device_id);

select created_at, updated_at, o.device_id, i.humidity, o.height
  into expected_left
  from :chunk1 i left join the_hypercore o
    on i.created_at = o.updated_at and i.device_id = o.device_id;

alter table the_hypercore set (
      timescaledb.compress,
      timescaledb.compress_segmentby = '',
      timescaledb.compress_orderby = 'updated_at desc'
);
select compress_chunk(show_chunks('the_hypercore'), compress_using => 'hypercore');

vacuum analyze the_hypercore;

-- Test a merge join. We explicitly set what join methods to enable
-- and disable to avoid flaky tests.
set enable_mergejoin to true;
set enable_hashjoin to false;
set enable_nestloop to false;

\set jointype merge
\ir include/hypercore_join_test.sql

-- Test nested loop join.
set enable_mergejoin to false;
set enable_hashjoin to false;
set enable_nestloop to true;

\set jointype nestloop
\ir include/hypercore_join_test.sql

-- Test a hash join.
set enable_mergejoin to false;
set enable_hashjoin to true;
set enable_nestloop to false;

\set jointype hash
\ir include/hypercore_join_test.sql

drop table expected_inner, expected_left;
