# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
    create table metrics (
	metric_id serial,
	created_at timestamptz not null,
	device_id int,
	twist float
    );
    select from create_hypertable('metrics', by_range('created_at'));

    select setseed(1);

    insert into metrics (created_at, device_id, twist)
    select t, ceil(random()*30), random()*100
    from generate_series('2022-06-01'::timestamptz, '2022-07-01', '60s') t;

    alter table metrics set (timescaledb.compress);

    do $$
    declare
      chunk regclass;
    begin
       for chunk in select ch from show_chunks('metrics') ch loop
          execute format('alter table %s set access method hyperstore', chunk);
       end loop;
    end;
    $$;
}

teardown {
    drop table metrics;
}

session "s1"
step "s1_start_transaction" {
    start transaction;
}
step "s1_select_for_update" {
    select _timescaledb_debug.is_compressed_tid(ctid), * from metrics where metric_id = 1234 for update;
} 
step "s1_commit" {
    commit;
}
step "s1_select" {
    select * from metrics where metric_id = 1234;
} 

session "s2"
step "s2_update" {
    update metrics set twist = 200.0 where metric_id = 1234;
}
step "s2_select" {
    select * from metrics where metric_id = 1234;
}

permutation "s1_start_transaction" "s1_select_for_update" "s2_update" "s1_select" "s1_commit" "s2_select" "s1_select"
