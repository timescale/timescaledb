-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that transaction memory usage with COPY doesn't grow.
-- We need memory usage in PortalContext after the completion of the query, so
-- we'll have to log it from a trigger that runs after the query is completed.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

create table uk_price_paid(price integer, "date" date, postcode1 text, postcode2 text, type smallint, is_new bool, duration smallint, addr1 text, addr2 text, street text, locality text, town text, district text, country text, category smallint);
-- Aim to about 100 partitions, the data is from 1995 to 2022.
select create_hypertable('uk_price_paid', 'date', chunk_time_interval => interval '90 day');

-- This is where we log the memory usage.
create table portal_memory_log(id serial, bytes bigint);

-- Returns the amount of memory currently allocated in a given
-- memory context. Only works for PortalContext, and doesn't work for PG 12.
create or replace function ts_debug_allocated_bytes(text) returns bigint
    as :MODULE_PATHNAME, 'ts_debug_allocated_bytes'
    language c strict volatile;

-- Log current memory usage into the log table.
create function log_memory() returns trigger as $$
    begin
        insert into portal_memory_log
            values (default, ts_debug_allocated_bytes('PortalContext'));
        return new;
    end;
$$ language plpgsql;

-- Add a trigger that runs after completion of each INSERT/COPY and logs the
-- current memory usage.
create trigger check_update after insert on uk_price_paid
    for each statement execute function log_memory();

-- Memory leaks often happen on cache invalidation, so make sure they are
-- invalidated often and independently (at co-prime periods).
set timescaledb.max_open_chunks_per_insert = 2;
set timescaledb.max_cached_chunks_per_hypertable = 3;

-- Try increasingly larger data sets by concatenating the same file multiple
-- times. First, INSERT into an uncompressed table.
create table uk_price_paid_one(like uk_price_paid);
\copy uk_price_paid_one from program 'bash -c "cat <(zcat < data/prices-10k-random-1.tsv.gz)"';

truncate uk_price_paid;
insert into uk_price_paid select * from uk_price_paid_one;

truncate portal_memory_log;
alter sequence portal_memory_log_id_seq restart with 1;

-- Don't use joins here because they might materialize a subquery which will
-- lead to weird memory usage changes.
insert into uk_price_paid select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;

-- Use linear regression to check that the memory usage doesn't significanlty
-- increase with increasing the insert size.
-- Note that there's going to be some linear planning and parsing overhead for
-- bigger queries, unlike the COPY. It can constitute a sizable percentage of
-- the total memory usage, so we'll check the absolute value as well.
\set parsing_overhead_bytes 50000
select * from portal_memory_log where (
    select regr_slope(bytes, id - 1) / regr_intercept(bytes, id - 1)::float > 0.05
        and regr_slope(bytes, id - 1) > :parsing_overhead_bytes
        from portal_memory_log
);


-- INSERT into a compressed table.
truncate uk_price_paid;
insert into uk_price_paid select * from uk_price_paid_one;
alter table uk_price_paid set (timescaledb.compress, timescaledb.compress_orderby = 'date');
select count(compress_chunk(chunk)) from show_chunks('uk_price_paid') chunk;

truncate portal_memory_log;
alter sequence portal_memory_log_id_seq restart with 1;

-- Don't use joins here because they might materialize a subquery which will
-- lead to weird memory usage changes.
insert into uk_price_paid select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;
insert into uk_price_paid select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one union all select * from uk_price_paid_one;

select * from portal_memory_log where (
    select regr_slope(bytes, id - 1) / regr_intercept(bytes, id - 1)::float > 0.05
        and regr_slope(bytes, id - 1) > :parsing_overhead_bytes
        from portal_memory_log
);
