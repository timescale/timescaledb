-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO queries
\set saved_table :the_table _saved

create table :the_table(created_at timestamptz not null unique, value :the_type);

select create_hypertable(:'the_table', by_range('created_at'));

alter table :the_table set (
      timescaledb.compress,
      timescaledb.compress_segmentby = '',
      timescaledb.compress_orderby = 'created_at'
);

select setseed(1);
\set ECHO all

-- Insert some data to produce at least two chunks.
\set ECHO queries
insert into :the_table(created_at, value)
select t, :the_generator
from generate_series('2022-06-01'::timestamp, '2022-06-10', '1 minute') t;
\set ECHO all

-- Save away the table so that we can make sure that a hyperstore
-- table and a heap table produce the same result.
create table :saved_table as select * from :the_table;

-- Compress the rows in the hyperstore.
select compress_chunk(show_chunks(:'the_table'), compress_using => 'hyperstore');

-- This part of the include file will run a query with the aggregate
-- provided by the including file and test that using a hyperstore
-- with compressed rows and a normal table produces the same result
-- for the query with the given aggregate.
\set ECHO queries
with
  lhs as (
      select date_trunc('hour', created_at) as created_at,
      	     :the_aggregate as :"the_aggregate"
      from :the_table where (:the_clause) group by date_trunc('hour', created_at)
  ),
  rhs as (
      select date_trunc('hour', created_at) as created_at,
      	     :the_aggregate as :"the_aggregate"
      from :saved_table where (:the_clause) group by date_trunc('hour', created_at)
  )
select lhs.*, rhs.*
from lhs full join rhs using (created_at)
where lhs.created_at is null or rhs.created_at is null;

drop table :the_table;
drop table :saved_table;
\set ECHO all

