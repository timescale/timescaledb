# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    create table readings (time timestamptz, device int, msg text);
    select create_hypertable('readings', 'time', chunk_time_interval => interval '1 hour');
           insert into readings values ('2024-01-01 01:00', 1, 'one'), ('2024-01-01 01:01', 2, 'two'), ('2024-01-01 02:00', 3, 'three'), ('2024-01-01 02:00', 4, 'four'), ('2024-01-01 03:30', 5, 'five'), ('2024-01-01 04:40', 6, 'six');
    alter table readings set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');

    create or replace procedure merge_two_chunks(hypertable regclass, concurrent boolean = false) as $$
    declare
        chunks_arr regclass[];
    begin
        with chunks as (
            select cl.oid as oid
               from pg_class cl
               join pg_inherits inh
               on (cl.oid = inh.inhrelid)
               where inh.inhparent = hypertable
               limit 2
        )
        select array_agg(ch.oid) into chunks_arr
            from chunks ch;

        if concurrent then
            call merge_chunks_concurrently(variadic chunks_arr);
        else
            call merge_chunks(variadic chunks_arr);
        end if;
    end;
    $$ LANGUAGE plpgsql;

    create or replace procedure drop_one_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select cl.oid into chunk
           from pg_class cl
           join pg_inherits inh
           on (cl.oid = inh.inhrelid)
           where inh.inhparent = hypertable
           order by cl.oid
           limit 1;
        execute format('drop table %s cascade', chunk);
     end;
    $$ LANGUAGE plpgsql;

    create or replace procedure lock_chunk(hypertable regclass, off int = 1, lock_mode text = 'row exclusive') as $$
    declare
        chunk regclass;
    begin
        execute format('select ch from show_chunks(%L) ch offset %s limit 1', hypertable, off) into chunk;
        execute format('lock %s in %s mode', chunk, lock_mode);
    end;
    $$ LANGUAGE plpgsql;

    create or replace function getchunk(hypertable regclass, off int = 0) returns regclass as $$
    declare
        chunk regclass;
    begin
        execute format('select ch from show_chunks(%L) ch offset %s limit 1', hypertable, off) into chunk;
        return chunk;
    end;
    $$ LANGUAGE plpgsql;


    reset client_min_messages;
}

teardown {
    drop table readings;
}

# Waitpoints
session "wp"
step "wp_before_heap_swap_on" { SELECT debug_waitpoint_enable('merge_chunks_before_heap_swap'); }
step "wp_before_heap_swap_off" { SELECT debug_waitpoint_release('merge_chunks_before_heap_swap'); }

step "wp_before_first_commit_on" { SELECT debug_waitpoint_enable('merge_chunks_before_first_commit'); }
step "wp_before_first_commit_off" { SELECT debug_waitpoint_release('merge_chunks_before_first_commit'); }

step "wp_after_first_commit_on" { SELECT debug_waitpoint_enable('merge_chunks_after_first_commit'); }
step "wp_after_first_commit_off" { SELECT debug_waitpoint_release('merge_chunks_after_first_commit'); }

step "wp_fail_merge_on" { SELECT debug_waitpoint_enable('merge_chunks_fail'); }
step "wp_fail_merge_off" { SELECT debug_waitpoint_release('merge_chunks_fail'); }

session "s1"
setup	{
    set local lock_timeout = '5000ms';
    set local deadlock_timeout = '100ms';
}

# The transaction will not "pick" a snapshot until the first query, so
# do a simple select on pg_class to pick one for the transaction. We
# don't want to query any tables involved in the test since that will
# grab locks on them.
step "s1_begin" {
    start transaction isolation level repeatable read;
    select count(*) > 0 from pg_class;
}

step "s1_show_chunks" { select count(*) from show_chunks('readings'); }
step "s1_show_data" {
    select * from readings order by time desc, device;
}
step "s1_show_merge" {
    select count(*) from _timescaledb_catalog.chunk_rewrite;
}
step "s1_row_exclusive_lock" { call lock_one_chunk('readings'); }
step "s1_commit" { commit; }

session "s2"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s2_show_chunks" { select count(*) from show_chunks('readings'); }
step "s2_merge_chunks" {
    call merge_two_chunks('readings');
}
step "s2_merge_chunks_concurrently" {
    call merge_two_chunks('readings', concurrent => true);
}

step "s2_lock_chunk" {
    begin;
    call lock_chunk('readings', 0);
}

step "s2_lock_chunk_access_exclusive" {
    begin;
    call lock_chunk('readings', 0, 'access exclusive');
}

step "s2_commit" {
    commit;
}

session "s3"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s3_begin" {
    start transaction isolation level read committed;
    select count(*) > 0 from pg_class;
}
step "s3_show_data" {
    select * from readings order by time desc, device;
    select count(*) as num_device_all, count(*) filter (where device=1) as num_device_1, count(*) filter (where device=5) as num_device_5 from readings;
}
step "s3_show_chunks" { select count(*) from show_chunks('readings'); }
step "s3_merge_chunks" {
    call merge_two_chunks('readings');
}
step "s3_merge_chunks_concurrently" {
    call merge_two_chunks('readings', concurrent => true);
}

#step "s3_compress_chunk" {
#    call convert_to_columnstore(getchunk('readings' , 0));
#}

# TODO test 4 cases for inserts.
#
# 1. Insert into merged chunk to be dropped
# 2. Insert into merged chunk that remains
# 3. Insert into non-merge chunk
# 4. Insert into chunk that does not exist and is created
#
# Repeat above for update/delete/copy

step "s3_insert_chunk_to_be_dropped" {
    insert into readings values ('2024-01-01 02:10', 6, 's3 dropped chunk'), ('2024-01-01 02:12', 7, 's3 dropped chunk');
}

step "s3_drop_chunks" {
    call drop_one_chunk('readings');
}

step "s3_commit" { commit; }

session "s4"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s4_insert_result_chunk" {
    insert into readings values ('2024-01-01 01:10', 8, 's4 result chunk');
}

session "s5"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s5_show_merge_rels" {
    select count(*) from _timescaledb_catalog.chunk_rewrite;
}

step "s5_modify" {
    delete from readings where device=1;
    insert into readings values ('2024-01-01 01:05', 5, 's5 modify');
}

step "s5_merge_1_2_concurrently" {
    call merge_chunks(getchunk('readings', 0), getchunk('readings', 1), concurrently => true);
}

step "s5_merge_cleanup" {
    set client_min_messages = DEBUG1;
    call _timescaledb_functions.chunk_rewrite_cleanup();
    select count(*) from _timescaledb_catalog.chunk_rewrite;
    reset client_min_messages;
}

session "s6"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s6_insert_chunk_not_merged" {
    insert into readings values ('2024-01-01 03:20', 9, 's6 chunk not merged');
}

step "s6_merge_3_4_concurrently" {
    call merge_chunks(getchunk('readings', 2), getchunk('readings', 3), concurrently => true);
}

session "s7"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s7_insert_new_chunk" {
    insert into readings values ('2024-01-01 04:04', 11, 's7 new chunk');
}

step "s7_fail_merge" {
    call merge_chunks(getchunk('readings', 0), getchunk('readings', 1), concurrently => true);
}

# Run 4 backends:
#
# s1: will read data in REPEATABLE READ (should not see changes after merge)
# s2: will merge chunks
# s3: will read data in READ COMMITTED (should see changes immediately after merge)
# s4: will modify data during TX s1 and s3 but before merge
#
# Expectation: s1 should see the original data as it was before s4
# modifications and merge while s3 should see the changes
permutation "s2_show_chunks" "s3_show_data" "s1_begin" "s3_begin" "s5_modify" "s2_merge_chunks" "s1_show_chunks" "s3_show_chunks" "s1_show_data" "s3_show_data" "s1_commit" "s1_show_data" "s3_commit"

# Merge chunks with AccessExclusiveLock (default). s2_merge_chunks
# need to wait for readers to finish before even starting merge
permutation "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_commit" "s1_show_data" "s1_show_chunks"

# Merge blocks until the reader/writer is finished.

permutation "wp_before_heap_swap_on" "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_row_exclusive_lock" "wp_before_heap_swap_off" "s1_commit" "s1_show_data" "s1_show_chunks"

# Same as above, but the merger takes locks before the reader/writer
# so the reader/writer has to wait.
permutation "wp_before_heap_swap_on" "s2_show_chunks" "s1_begin" "s2_merge_chunks" "s1_show_data"  "wp_before_heap_swap_off" "s1_commit" "s1_show_data" "s1_show_chunks"

# Test concurrent merges
permutation "wp_before_heap_swap_on" "s2_merge_chunks" "s3_merge_chunks" "wp_before_heap_swap_off" "s1_show_data" "s1_show_chunks"

# Test concurrent compress_chunk(). This will deadlock because
# compress_chunk() takes chunk locks in a different order; it takes a
# lock on a chunk slice first, before taking the lock on the chunk itself.
# It probably should take a lock on the chunk table first.
# The test is disabled because with a deadlock the output can be non-deterministic.

#permutation "wp_before_heap_swap_on" "s2_merge_chunks" "s3_compress_chunk" "wp_before_heap_swap_off" "s1_show_data" "s1_show_chunks"

# Test concurrent DROP TABLE on chunk (currently deadlocks because of locks on parent table)
permutation "wp_before_heap_swap_on" "s2_merge_chunks" "s3_drop_chunks" "wp_before_heap_swap_off" "s1_show_data" "s1_show_chunks"

# Concurrent reads and various inserters during a concurrent merge of
# two chunks. The sessions include:
#
# 1. Merger
# 2. Reader
# 3. Inserter into merged chunk 1 (blocked)
# 4. Inserter into merged chunk 2 (blocked)
# 5. Inserter into chunk 2, which is not being merged (not blocked)
# 6. Inserter into new chunk (which didn't exist) (not blocked)
permutation "wp_before_heap_swap_on" "wp_before_first_commit_on" "s2_merge_chunks_concurrently" "s3_insert_chunk_to_be_dropped" "s4_insert_result_chunk" "s6_insert_chunk_not_merged" "s7_insert_new_chunk" "s1_show_chunks" "s1_show_data" "s5_show_merge_rels" "wp_before_first_commit_off" "s1_show_merge" "wp_before_heap_swap_off" "s1_show_data" "s1_show_chunks" "s1_show_merge"

# Two concurrent merges of same chunks. One will fail since the the
# first merge drops one of the chunks.
permutation "s1_show_chunks" "wp_after_first_commit_on" "s2_merge_chunks_concurrently" "s3_merge_chunks_concurrently" "wp_after_first_commit_off" "s1_show_chunks"

# Two concurrent merges of different chunks. Should not block each
# other. Use a lock on one chunk to ensure the first merge doesn't
# complete before the other runs.
permutation "s1_show_chunks" "s2_lock_chunk" "s5_merge_1_2_concurrently" "s6_merge_3_4_concurrently" "s2_commit" "s1_show_chunks"

# Test that failed merge is cleaned up. Also test that cleanup function does not clean up stuff currently running
permutation "wp_fail_merge_on" "s1_show_chunks" "s7_fail_merge" "wp_fail_merge_off" "s5_show_merge_rels" "s5_merge_1_2_concurrently" "s1_show_chunks" "s5_show_merge_rels"

permutation "wp_fail_merge_on" "s1_show_chunks" "s7_fail_merge" "wp_fail_merge_off" "s5_show_merge_rels" "wp_after_first_commit_on" "s6_merge_3_4_concurrently" "s5_merge_cleanup" "wp_after_first_commit_off" "s5_show_merge_rels"

# Verify that no one can get a higher lock than merge in-between merge transactions
permutation "wp_after_first_commit_on" "s5_merge_1_2_concurrently" "s2_lock_chunk_access_exclusive" "wp_after_first_commit_off" "s2_commit"
