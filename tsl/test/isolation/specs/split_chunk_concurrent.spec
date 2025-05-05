# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    create extension if not exists pageinspect;
    create table readings (time timestamptz, device int, temp float) with (fillfactor = 30);
    select create_hypertable('readings', 'time', chunk_time_interval => interval '1 week');
    insert into readings values ('2024-01-04 01:00', 1, 1.0), ('2024-01-05 02:00', 2, 2.0), ('2024-01-08 02:00', 3, 3.0), ('2024-01-11 01:01', 4, 4.0), ('2024-01-15 02:00', 5, 5.0);
    alter table readings set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
    create index on readings (device);

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

    create or replace procedure split_first_chunk(hypertable regclass) as $$
    declare
        chunk regclass;
    begin
        select cl.oid into chunk
           from pg_class cl
           join pg_inherits inh
           on (cl.oid = inh.inhrelid)
           where inh.inhparent = hypertable
           limit 1;
        execute format('call split_chunk(%L)', chunk);
    end;
    $$ LANGUAGE plpgsql;

    create or replace procedure show_all_chunks(hypertable regclass) as $$
    declare
       chunk regclass;
       chunk_info timescaledb_information.chunks;
       n int = 0;
       row record;
    begin
        for chunk_info in
            select *
                   from timescaledb_information.chunks
                   where format('%I.%I', hypertable_schema, hypertable_name)::regclass = hypertable
        loop
            raise notice '---- chunk % [ % : % ]', n,  chunk_info.range_start, chunk_info.range_end;
            for row in
                execute format('select * from %I.%I', chunk_info.chunk_schema, chunk_info.chunk_name)
            loop
                raise notice '%', row;
            end loop;

            n = n + 1;
        end loop;
    end;
    $$ LANGUAGE plpgsql;
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
    select count(*) > 0 from pg_stat_activity;
}

step "s1_commit" { commit; }

# Insert two tuples that each go into different parts of the split chunk
step "s1_insert_into_splitting_chunk" {
    insert into readings values ('2024-01-05 01:05', 10, 10.0), ('2024-01-09 01:05', 11, 11.0);
}

step "s1_insert_into_existing_chunk" {
    insert into readings values ('2024-01-12 01:05', 12, 12.0);
}

step "s1_insert_into_new_chunk" {
    insert into readings values ('2024-01-18 10:00', 13, 13.0);
}

step "s1_update_splitting_chunk" {
    update readings set device = 1 where device = 2;
}

step "s1_delete_from_splitting_chunk" {
    delete from readings where device = 1;
}

session "s2"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s2_begin" {
    start transaction isolation level repeatable read;
    select count(*) > 0 from pg_stat_activity;
}

step "s2_commit" { commit; }

step "s2_split_chunk" {
    call split_first_chunk('readings');
}

step "s2_insert_into_splitting_chunk" {
    insert into readings values ('2024-01-05 01:05', 15, 15.0);
}

session "s3"
setup	{
    set local lock_timeout = '500ms';
    set local deadlock_timeout = '100ms';
}

step "s3_query_data" {
    call show_all_chunks('readings');
}

step "s3_wp_before_routing_on" { select debug_waitpoint_enable('split_chunk_before_tuple_routing'); }
step "s3_wp_before_routing_off" { select debug_waitpoint_release('split_chunk_before_tuple_routing'); }

step "s3_wp_at_end_on" { select debug_waitpoint_enable('split_chunk_at_end'); }
step "s3_wp_at_end_off" { select debug_waitpoint_release('split_chunk_at_end'); }

step "s4_begin" {
    start transaction isolation level repeatable read;
    select count(*) > 0 from pg_stat_activity;
}

step "s4_query" {
    select * from readings order by time, device;
}

step "s4_commit" { commit; }

# Concurrent insert into existing chunk while another chunk is being
# split. The inserting process should not be blocked.
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_existing_chunk" "s3_wp_at_end_off" "s3_query_data"
permutation "s3_query_data" "s3_wp_before_routing_on" "s2_split_chunk" "s1_insert_into_existing_chunk" "s3_wp_before_routing_off" "s3_query_data"

# Concurrent insert into new chunk while another chunk is being
# split. The inserting process is blocked because the split process
# takes ShareUpdateExclusive lock on the hypertable root to attach the
# new chunk from the split.
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_new_chunk" "s3_wp_at_end_off" "s3_query_data"
permutation "s3_query_data" "s3_wp_before_routing_on" "s2_split_chunk" "s1_insert_into_new_chunk" "s3_wp_before_routing_off" "s3_query_data"


# Concurrent insert into chunk being split. The inserting process
# should be blocked and the two inserted tuples should end up in their
# corresponding parts of the split chunk.
permutation "s3_query_data" "s3_wp_at_end_on" "s2_split_chunk" "s1_insert_into_splitting_chunk" "s3_wp_at_end_off" "s3_query_data"
permutation "s3_query_data" "s3_wp_before_routing_on" "s2_split_chunk" "s1_insert_into_splitting_chunk" "s3_wp_before_routing_off" "s3_query_data"

# Concurrent insert into chunk being split. The inserting process
# locks the chunk first so the split should be blocked.
permutation "s3_query_data" "s1_begin" "s1_insert_into_splitting_chunk" "s2_split_chunk" "s1_commit" "s3_query_data"

# Delete from splitting chunk
permutation "s4_begin" "s3_query_data" "s1_begin" "s1_delete_from_splitting_chunk" "s2_split_chunk" "s1_commit" "s3_query_data" "s4_query" "s4_commit" "s4_query"
permutation "s4_begin" "s3_query_data" "s1_delete_from_splitting_chunk" "s2_split_chunk" "s3_query_data" "s4_query" "s4_commit" "s4_query"

# Delete and update on splitting chunk and concurrent query in
# repeatable read. The querying process should see the old data after
# split (including deleted tuple). After commit it sees the update/delete.
permutation "s3_query_data" "s4_begin" "s1_delete_from_splitting_chunk" "s2_split_chunk" "s3_query_data" "s4_query" "s4_commit" "s4_query" "s3_query_data"
permutation "s3_query_data" "s4_begin" "s1_update_splitting_chunk" "s2_split_chunk" "s3_query_data" "s4_query" "s4_commit" "s4_query" "s3_query_data"

# Insert into splitting chunk by splitting process
permutation "s2_begin" "s2_insert_into_splitting_chunk" "s2_split_chunk" "s3_query_data" "s2_commit"
