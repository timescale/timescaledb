# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    create table readings (time timestamptz, device int, temp float);
    select create_hypertable('readings', 'time', chunk_time_interval => interval '1 hour');
           insert into readings values ('2024-01-01 01:00', 1, 1.0), ('2024-01-01 01:01', 2, 2.0), ('2024-01-01 02:00', 3, 3.0), ('2024-01-01 02:00', 4, 4.0);
    alter table readings set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
           
    create or replace procedure merge_all_chunks(hypertable regclass) as $$
    declare
        chunks_arr regclass[];
    begin
        select array_agg(cl.oid) into chunks_arr
               from pg_class cl
               join pg_inherits inh
               on (cl.oid = inh.inhrelid)
               where inh.inhparent = hypertable;
        
        call merge_chunks(variadic chunks_arr);
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
           limit 1;
        execute format('drop table %s cascade', chunk);
     end;
    $$ LANGUAGE plpgsql;
   
    create or replace procedure lock_one_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select ch into chunk from show_chunks(hypertable) ch offset 1 limit 1;
        execute format('lock %s in row exclusive mode', chunk);
    end;
    $$ LANGUAGE plpgsql;

    reset timescaledb.merge_chunks_lock_upgrade_mode;
}

teardown {
    drop table readings;
}

session "s1"
setup	{
    set local lock_timeout = '5000ms';
    set local deadlock_timeout = '10ms';
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
    select count(*) as num_device_all, count(*) filter (where device=1) as num_device_1, count(*) filter (where device=5) as num_device_5 from readings;
}
step "s1_row_exclusive_lock" { call lock_one_chunk('readings'); }
step "s1_commit" { commit; }

session "s2"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
    reset timescaledb.merge_chunks_lock_upgrade_mode;
}

step "s2_show_chunks" { select count(*) from show_chunks('readings'); }
step "s2_merge_chunks" {
    call merge_all_chunks('readings');
}

step "s2_set_lock_upgrade" {
    set timescaledb.merge_chunks_lock_upgrade_mode='upgrade';
}
step "s2_set_lock_upgrade_conditional" {
    set timescaledb.merge_chunks_lock_upgrade_mode='conditional';
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
    call merge_all_chunks('readings');
}
step "s3_compress_chunks" {
    select compress_chunk(show_chunks('readings'));
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

step "s4_modify" {
    delete from readings where device=1;
    insert into readings values ('2024-01-01 01:05', 5, 5.0);
}

step "s4_wp_enable" { SELECT debug_waitpoint_enable('merge_chunks_before_heap_swap'); }
step "s4_wp_release" { SELECT debug_waitpoint_release('merge_chunks_before_heap_swap'); }

# Run 4 backends:
#
# s1: will read data in REPEATABLE READ (should not see changes after merge)
# s2: will merge chunks
# s3: will read data in READ COMMITTED (should see changes immediately after merge)
# s4: will modify data during TX s1 and s3 but before merge
#
# Expectation: s1 should see the original data as it was before s4
# modifications and merge while s3 should see the changes
permutation "s2_show_chunks" "s3_show_data" "s1_begin" "s3_begin" "s4_modify" "s2_merge_chunks" "s1_show_chunks" "s3_show_chunks" "s1_show_data" "s3_show_data" "s1_commit" "s1_show_data" "s3_commit"

# Merge chunks with AccessExclusiveLock (default). s2_merge_chunks
# need to wait for readers to finish before even starting merge
permutation "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_commit" "s1_show_data" "s1_show_chunks"

# Merge chunks with lock upgrade. s2_merge_chunks can merge
# concurrently with readers but need to wait for readers to finish
# before doing the heap swap.
permutation "s2_set_lock_upgrade" "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_commit" "s1_show_data" "s1_show_chunks"

# Same as the above, but it will deadlock because a reader takes a
# heavier lock.
permutation "s2_set_lock_upgrade" "s4_wp_enable" "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_row_exclusive_lock" "s4_wp_release" "s1_commit" "s1_show_data" "s1_show_chunks"

# Same as above but with a conditional lock. The merge process should
# fail with an error saying it can't take the lock needed for the
# merge.
permutation "s2_set_lock_upgrade_conditional" "s4_wp_enable" "s2_show_chunks" "s1_begin" "s1_show_data" "s2_merge_chunks" "s1_show_data" "s1_row_exclusive_lock" "s4_wp_release" "s1_commit" "s1_show_data" "s1_show_chunks"

# Test concurrent merges
permutation "s4_wp_enable" "s2_merge_chunks" "s3_merge_chunks" "s4_wp_release" "s1_show_data" "s1_show_chunks"

# Test concurrent compress_chunk()
permutation "s4_wp_enable" "s2_merge_chunks" "s3_compress_chunks" "s4_wp_release" "s1_show_data" "s1_show_chunks"

# Test concurrent drop table 
permutation "s4_wp_enable" "s2_merge_chunks" "s3_drop_chunks" "s4_wp_release" "s1_show_data" "s1_show_chunks"
