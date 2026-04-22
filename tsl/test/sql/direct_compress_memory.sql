-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that direct compress INSERT memory usage doesn't grow with the
-- number of chunk transitions within a single INSERT.

\c :TEST_DBNAME :ROLE_SUPERUSER;

-- Helper function that returns the amount of memory currently allocated in a
-- given memory context.
create or replace function ts_debug_allocated_bytes(text = 'PortalContext') returns bigint
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;

set timescaledb.enable_direct_compress_insert = true;

create table dc_mem(time timestamptz not null, device text, value float8);
select create_hypertable('dc_mem', 'time', chunk_time_interval => interval '1 day');
alter table dc_mem set (timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time');

create table dc_mem_log(nchunks int, bytes bigint);

-- Insert into alternating chunks to test chunk changes with direct compress.
select format('
with ins as (
    insert into dc_mem
    select ''2025-01-01''::timestamptz + (i %% 2) * (interval ''1 day''), ''dev1'', random()
    from generate_series(1, %1$s) i returning 1),
mem as (select %1$s as n, count(*), ts_debug_allocated_bytes() as b from ins)
insert into dc_mem_log select n, b from mem;
', unnest(array[50, 100, 200, 400, 800]))
\gexec
;

-- Check if we have memory usage growth with the number of chunks processed.
select * from dc_mem_log where (
    select regr_slope(bytes, nchunks) / regr_intercept(bytes, nchunks)::float > 0.01
    from dc_mem_log)
;

truncate dc_mem_log;

-- Insert into different chunks to test direct compress with chunk count growth.
select format('
with ins as (
    insert into dc_mem
    select ''2025-01-01''::timestamptz + i * (interval ''1 day''), ''dev1'', random()
    from generate_series(1, %1$s) i returning 1),
mem as (select %1$s as n, count(*), ts_debug_allocated_bytes() as b from ins)
insert into dc_mem_log select n, b from mem;
', unnest(array[50, 100, 200, 400, 800]))
\gexec
;

-- Check if we have memory usage growth with the number of chunks processed.
-- Here we actually expect growth, because the number of chunks increases, and
-- each chunk and its indexes and attributes get the respective catalog cache
-- entries. On PG 17 it is about 11kB per chunk.

select regr_slope(bytes, nchunks), regr_intercept(bytes, nchunks) from dc_mem_log
having regr_slope(bytes, nchunks) > 15000
;

select * from dc_mem_log where (select regr_slope(bytes, nchunks) from dc_mem_log) > 15000;

truncate dc_mem_log;
