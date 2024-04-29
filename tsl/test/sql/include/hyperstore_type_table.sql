-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set saved_table :the_table _saved

create table :the_table(created_at timestamptz not null unique, value :the_type);

select create_hypertable(:'the_table', by_range('created_at'));

alter table :the_table set (
      timescaledb.compress,
      timescaledb.compress_segmentby = '',
      timescaledb.compress_orderby = 'created_at'
);

select setseed(1);

insert into :the_table(created_at, value)
select t, :the_generator
from generate_series('2022-06-01'::timestamp, '2022-06-10', '1 minute') t;

create table :saved_table as select * from :the_table;

select twist_chunk(show_chunks(:'the_table'));

select lhs.*, rhs.*
from (select :the_aggregate as :"the_aggregate" from :the_table where (:the_clause)) lhs,
     (select :the_aggregate as :"the_aggregate" from :saved_table where (:the_clause)) rhs;

drop table :the_table;
drop table :saved_table;

