-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

create table qualdef(a int, b int);
select create_hypertable('qualdef', 'a', chunk_time_interval => 1000);

insert into qualdef select x, x % 5 from generate_series(1, 999) x;
alter table qualdef set (timescaledb.compress);
select compress_chunk(show_chunks('qualdef'));

alter table qualdef add column i2 int2 default 7;
alter table qualdef add column i4 int4 default 8;
alter table qualdef add column i8 int8 default 9;
alter table qualdef add column f4 float4 default 10;
alter table qualdef add column f8 float8 default 11;

-- Test some error cases with stable casts in default while we're at it.
\set ON_ERROR_STOP 0
alter table qualdef add column tstz timestamptz default timestamp '2021-01-01';
alter table qualdef add column tstz timestamptz default '2021-01-01 00:00:00'::timestamp;
\set ON_ERROR_STOP 1
alter table qualdef add column tstz timestamptz default '2021-01-01 00:00:00+00';

\set ON_ERROR_STOP 0
alter table qualdef add column ts timestamp default tstz;
alter table qualdef add column ts timestamp default timestamptz '2021-01-01 00:00:00';
\set ON_ERROR_STOP 1
alter table qualdef add column ts timestamp default timestamp '2021-01-01 00:00:00';

alter table qualdef add column d date default '2021-01-01';
alter table qualdef add column u uuid default 'b1af1cc0-c96c-4bbc-9b96-d25e5ff277cd';
alter table qualdef add column l bool default true;
alter table qualdef add column ln bool default null;
alter table qualdef add column lf bool default false;
alter table qualdef add column t text default 'x';
insert into qualdef select x, x % 5, 11 from generate_series(1001, 1999) x;
select compress_chunk(show_chunks('qualdef'));

vacuum analyze qualdef;


set timescaledb.debug_require_vector_qual to require;
--/* Uncomment to generate reference. */ set timescaledb.enable_bulk_decompression to off; set timescaledb.debug_require_vector_qual to forbid;

select count(*) from qualdef where i2 = 7;
select count(*) from qualdef where i4 = 8;
select count(*) from qualdef where i8 = 9;
select count(*) from qualdef where f4 = 10;
select count(*) from qualdef where f8 = 11;
select count(*) from qualdef where ts = '2021-01-01 00:00:00';
select count(*) from qualdef where tstz = '2021-01-01 00:00:00+00'::timestamptz;
select count(*) from qualdef where d = '2021-01-01'::date;
select count(*) from qualdef where u = 'b1af1cc0-c96c-4bbc-9b96-d25e5ff277cd'::uuid;
select count(*) from qualdef where l;
select count(*) from qualdef where ln;
select count(*) from qualdef where lf;
select count(*) from qualdef where t = 'x';

reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bulk_decompression;


-- Also test some scalar handling in Postgres qual evaluation in columnar aggregation node
create or replace function bool_identity(a bool) returns bool
language plpgsql immutable strict parallel safe as $$
begin
return a;
end;
$$;

set timescaledb.debug_require_vector_agg to require;
set timescaledb.debug_require_vector_qual to forbid;
--/* Uncomment to generate reference. */ set timescaledb.enable_bulk_decompression to off; set timescaledb.debug_require_vector_agg to forbid;

select count(*) from qualdef where l = (i2 = 7)::bool;
select count(*) from qualdef where ln = (i2 = 7)::bool;
select count(*) from qualdef where bool_identity(l);
select count(*) from qualdef where bool_identity(ln);
select count(*) from qualdef where bool_identity(lf);

reset timescaledb.debug_require_vector_agg;


-- Some expressions are not supported at the moment.
set timescaledb.debug_require_vector_agg to forbid;

select count(*) from qualdef where bool_identity(ln) or bool_identity(lf);

reset timescaledb.debug_require_vector_agg;

reset timescaledb.enable_bulk_decompression;
