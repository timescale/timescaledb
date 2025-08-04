-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
create or replace function mix(x anyelement) returns float8 as $$
    select hashfloat8(x::float8) / pow(2, 32)
$$ language sql;

create table bloom(x int, value text, u uuid, ts timestamp);
select create_hypertable('bloom', 'x');

insert into bloom
select x, md5(x::text),
    case when x = 7134 then '90ec9e8e-4501-4232-9d03-6d7cf6132815'
        else '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid end,
    '2021-01-01'::timestamp + (interval '1 hour') * x
from generate_series(1, 10000) x;

create index on bloom using brin(value text_bloom_ops);
create index on bloom using brin(u uuid_bloom_ops);
create index on bloom using brin(ts timestamp_minmax_ops);

alter table bloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');
select count(compress_chunk(x)) from show_chunks('bloom') x;
vacuum full analyze bloom;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'bloom') limit 1)
\gset

\d+ :chunk

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = md5(7248::text);

select count(*) from bloom where value = md5(7248::text);


-- The join condition is not pushed down to the compressed scan for some reason.
set enable_mergejoin to off;
set enable_hashjoin to off;
set enable_material to off;

explain (analyze, verbose, costs off, timing off, summary off)
with query(value) as materialized (values (md5(3516::text)), (md5(9347::text)),
    (md5(5773::text)))
select count(*) from bloom natural join query;
;

with query(value) as materialized (values (md5(3516::text)), (md5(9347::text)),
    (md5(5773::text)))
select count(*) from bloom natural join query;
;

reset enable_mergejoin;
reset enable_hashjoin;
reset enable_material;


-- Stable expression that yields null
set timescaledb.enable_chunk_append to off;

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value =
    case when now() < '1970-01-01' then md5(2345::text) else null end
;

reset timescaledb.enable_chunk_append;


-- Stable expression that yields not null
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value =
    case when now() < '1970-01-01' then md5(2345::text) else md5(5837::text) end
;


-- Stable expression on minmax index
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where x <
    case when now() < '1970-01-01' then 1 else 1000 end
;


-- Parameter on minmax index
set timescaledb.enable_chunk_append to off;
set plan_cache_mode to 'force_generic_plan';
prepare p as
select count(*) from bloom where x < $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(1000);

deallocate p;


-- Parameter on bloom index
prepare p as
select count(*) from bloom where value = $1;

explain (analyze, verbose, costs off, timing off, summary off)
execute p(md5('2345'));

-- Null parameter on bloom index
explain (analyze, verbose, costs off, timing off, summary off)
execute p(null);

reset timescaledb.enable_chunk_append;

deallocate p;


-- Function of parameter on bloom index
prepare p as
select count(*) from bloom where value = md5($1);

explain (analyze, verbose, costs off, timing off, summary off)
execute p('2345');

deallocate p;

reset plan_cache_mode;
reset timescaledb.enable_chunk_append;


-- Scalar array operations are not yet supported
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where x < any(array[1000, 2000]::int[]);

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = any(array[md5('1000'), md5('2000')]);


-- UUID uses bloom
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '90ec9e8e-4501-4232-9d03-6d7cf6132815';

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '6c1d0998-05f3-452c-abd3-45afe72bbcab';

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where u = '6c1d0998-05f3-452c-abd3-45afe72bbcac';


-- Timestamp uses minmax
explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where ts between '2021-01-07' and '2021-01-14';


-- Test some corner cases
create table corner(ts int, s text, c text);
select create_hypertable('corner', 'ts');
alter table corner set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');

-- Detoasting the second argument of "bloom filter contains" function.
insert into corner values (1, repeat('long', 100), 'short');

-- Null bloom filter.
insert into corner values(2, 'normal', null);

-- A match and a short varlena header in the bloom filter.
insert into corner values(3, 'match', 'match');

-- Long varlena header in the bloom filter.
insert into corner select 4, 'longheader', generate_series(1, 1000)::text;

create index on corner(c);

select count(compress_chunk(x)) from show_chunks('corner') x;

vacuum full analyze corner;

explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'short';

-- Cross-type equality operator.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'short'::name;

-- Comparison with segmentby.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = s;

-- Can push down only some parts of the expression but not the others, so the
-- pushdown shouldn't work in this case.
explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = s or c = random()::text;

explain (analyze, verbose, costs off, timing off, summary off)
select * from corner where c = 'test'
    or c = case when now() > '1970-01-01' then 'test2' else random()::text end
;

-- Unsupported operator
explain (costs off)
select * from corner where c > s;


-- Test a bad hash function.
create type badint;

create or replace function badintin(cstring) returns badint
  as 'int8in' language internal strict immutable parallel safe;
create or replace function badintout(badint) returns cstring
  as 'int8out' language internal strict immutable parallel safe;
create or replace function badinteq(badint, badint) returns bool
  as 'int8eq' language internal strict immutable parallel safe;

create type badint (like = int8, input = badintin, output = badintout);

create cast (bigint as badint) without function as implicit;
create cast (badint as bigint) without function as implicit;

create operator = (procedure = badinteq, leftarg = badint, rightarg = badint);

-- The btree opfamily equality operator must be in sync with the potential hash family.
-- If we don't create it explicitly, Postgres will choose the int8 family because
-- it's binary compatible, and the equality operator won't match.
create function badint_cmp(left badint, right badint) returns integer
  as 'btint8cmp' language internal immutable parallel safe strict;

create function badint_lt(a badint, b badint) returns boolean as 'int8lt' language internal immutable parallel safe strict;
create function badint_le(a badint, b badint) returns boolean as 'int8le' language internal immutable parallel safe strict;
create function badint_gt(a badint, b badint) returns boolean as 'int8gt' language internal immutable parallel safe strict;
create function badint_ge(a badint, b badint) returns boolean as 'int8ge' language internal immutable parallel safe strict;

create operator <  (leftarg = badint, rightarg = badint, procedure = badint_lt );
create operator <= (leftarg = badint, rightarg = badint, procedure = badint_le );
create operator >= (leftarg = badint, rightarg = badint, procedure = badint_ge );
create operator >  (leftarg = badint, rightarg = badint, procedure = badint_gt );

create operator class badint_btree_ops
  default for type badint using btree
as
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 >,
  function 1 badint_cmp(badint, badint)
;

create table badtable(ts int, s int, b badint);

select create_hypertable('badtable', 'ts');

alter table badtable set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');

insert into badtable select generate_series(1, 10000), 0, 0::int8;

insert into badtable select generate_series(1, 10000), 1, 1::int8;

insert into badtable select generate_series(1, 10000), -1, -1::int8;

insert into badtable select x, 2, x::int8 from generate_series(1, 10000) x;

insert into badtable select x, 3, (pow(2, 32) * x)::int8 from generate_series(1, 10000) x;

insert into badtable select x, 4, (16834 * x)::int8 from generate_series(1, 10000) x;

insert into badtable select x, 5, (4096 * x)::int8 from generate_series(1, 10000) x;

create index on badtable(b);

-- First, try compressing w/o the hash function. We shouldn't get a bloom filter
-- index.
select count(compress_chunk(x)) from show_chunks('badtable') x;

vacuum full analyze badtable;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'badtable') limit 1)
\gset

\d+ :chunk

select count(decompress_chunk(x)) from show_chunks('badtable') x;

-- Then, create the hash functions.
-- The simple hash is actually used for dictionary compression, so we have to
-- avoid overflows there.
create or replace function badint_identity_hash(val badint)
  returns int language sql immutable strict
as $$ select (val & ((1::bigint << 31) - 1))::int4; $$;

create or replace function badint_identity_hash_extended(val badint, seed bigint)
  returns bigint language sql immutable strict
as $$ select val; $$;

create operator class badint_hash_ops
  default for type badint using hash
as
  operator 1 = (badint, badint),
  function 1 badint_identity_hash(badint),
  function 2 badint_identity_hash_extended(badint, bigint)
;

-- Recompress after creating the hash functions
select count(compress_chunk(x)) from show_chunks('badtable') x;

vacuum full analyze badtable;

-- Verify that we actually got the bloom filter index.
select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'badtable') limit 1)
\gset

\d+ :chunk

-- Check index pushdown with one value.
explain (analyze, costs off, timing off, summary off)
select * from badtable where b = 1000::int8::badint;


-- Check index pushdown and absence of negatives with multiple value. The query
-- shape is a little weird to achieve the parameterized compressed scan, for
-- joins it doesn't work at the moment due to general problem with parameterized
-- DecompressChunk there.
explain (analyze, costs off, timing off, summary off)
with v_int(b) as (values (0), (1), (-1), (2), (4), (8), (1024),
    (pow(2, 32) * 2), (pow(2, 32) * 1024)),
v_badint as materialized (select b::int8::badint from v_int)
select exists (select * from badtable where b = v_badint.b) from v_badint;
;

with v_int(b) as (values (0), (1), (-1), (2), (4), (8), (1024),
    (pow(2, 32) * 2), (pow(2, 32) * 1024)),
v_badint as materialized (select b::int8::badint from v_int)
select exists (select * from badtable where b = v_badint.b) from v_badint;
;


-- Now, repeat the entire exercise with an even worse hash of constant zero.
select count(decompress_chunk(x)) from show_chunks('badtable') x;

create or replace function badint_identity_hash(val badint)
  returns int language sql immutable strict
as $$ select 0::int4; $$;

create or replace function badint_identity_hash_extended(val badint, seed bigint)
  returns bigint language sql immutable strict
as $$ select 0; $$;

select count(compress_chunk(x)) from show_chunks('badtable') x;

vacuum full analyze badtable;

explain (analyze, costs off, timing off, summary off)
select * from badtable where b = 1000::int8::badint;

explain (analyze, costs off, timing off, summary off)
with v_int(b) as (values (0), (1), (-1), (2), (4), (8), (1024),
    (pow(2, 32) * 2), (pow(2, 32) * 1024)),
v_badint as materialized (select b::int8::badint from v_int)
select exists (select * from badtable where b = v_badint.b) from v_badint;
;

with v_int(b) as (values (0), (1), (-1), (2), (4), (8), (1024),
    (pow(2, 32) * 2), (pow(2, 32) * 1024)),
v_badint as materialized (select b::int8::badint from v_int)
select exists (select * from badtable where b = v_badint.b) from v_badint;
;


-- Test a non-by-value 8-byte type.
create table byref(ts int, x macaddr8);
select create_hypertable('byref', 'ts');
create index on byref(x);
alter table byref set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts');

create function float8tomacaddr8(x float8) returns macaddr8 as $$
    select to_hex(right(float8send(x)::text, -1)::bit(64)::bigint)::macaddr8;
$$ language sql immutable parallel safe strict;

insert into byref select x, float8tomacaddr8(mix(x)) from generate_series(1, 10000) x;
select count(compress_chunk(x)) from show_chunks('byref') x;
vacuum analyze byref;

explain (analyze, verbose, costs off, timing off, summary off)
select * from byref where x = float8tomacaddr8(mix(1));


-- Test an array type.
create table arraybloom(x int, value int[], ts timestamp);
select create_hypertable('arraybloom', 'x');

insert into arraybloom
select x, array[x],
    '2021-01-01'::timestamp + (interval '1 hour') * x
from generate_series(1, 10000) x;

create index on arraybloom(value);

alter table arraybloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'x');
select count(compress_chunk(x)) from show_chunks('arraybloom') x;
vacuum full analyze arraybloom;

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from arraybloom where value = array[7248::int];

select count(*) from arraybloom where value = array[7248::int];


-- Cleanup
drop table bloom;
drop table corner;
drop table badtable;
drop table byref;
drop table arraybloom;

