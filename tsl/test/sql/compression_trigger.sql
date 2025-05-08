-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This is copied from hypercore_trigger.sql

set client_min_messages to warning;

create table readings(
       metric_id serial,
       created_at timestamptz not null unique,
       location_id smallint,	--segmentby attribute with index
       owner_id bigint,		--segmentby attribute without index
       device_id bigint,	--non-segmentby attribute
       temp float8,
       humidity float4
);

select create_hypertable('readings', by_range('created_at'));

select setseed(1);

insert into readings(created_at, location_id, device_id, owner_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), ceil(random() * 5), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t;

alter table readings set (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'created_at',
	  timescaledb.compress_segmentby = 'location_id, owner_id'
);

select compress_chunk(show_chunks('readings'));

create table saved_rows (like readings, new_row bool not null, kind text);

create function save_transition_table() returns trigger as $$
begin
   case tg_op
   	when 'INSERT' then
	     insert into saved_rows select n.*, true, tg_op from new_table n;
	when 'DELETE' then
	     insert into saved_rows select o.*, false, tg_op from old_table o;
	when 'UPDATE' then
	     insert into saved_rows select n.*, true, tg_op from new_table n;
	     insert into saved_rows select o.*, false, tg_op from old_table o;
   end case;
   return null;
end;
$$ language plpgsql;

create trigger save_insert_transition_table_trg
       after insert on readings
       referencing new table as new_table
       for each statement execute function save_transition_table();

insert into readings(created_at, location_id, device_id, owner_id, temp, humidity)
values ('2022-06-01 00:01:23', 999, 666, 111, 3.14, 3.14),
       ('2022-06-01 00:02:23', 999, 666, 112, 3.14, 3.14);

select * from saved_rows order by metric_id;

truncate saved_rows;

select compress_chunk(show_chunks('readings'));

copy readings(created_at, location_id, device_id, owner_id, temp, humidity) from stdin with (format csv);
"2022-06-01 00:01:35",999,666,111,3.14,3.14
\.

select * from saved_rows order by metric_id;

truncate saved_rows;

select compress_chunk(show_chunks('readings'));

create trigger save_update_transition_table_trg
       after update on readings
       referencing new table as new_table old table as old_table
       for each statement execute function save_transition_table();

select * from readings where location_id = 999 order by metric_id;

update readings set humidity = 99.99 where location_id = 999;

select * from saved_rows order by metric_id;

truncate saved_rows;

select compress_chunk(show_chunks('readings'));

-- This is not supported since it is possible to delete an entire
-- segment without executing the trigger.
\set ON_ERROR_STOP 0
create trigger save_delete_transition_table_trg
       after delete on readings
       referencing old table as old_table
       for each statement execute function save_transition_table();
\set ON_ERROR_STOP 1

-- Test that we get an error when enabling compression and have a
-- delete trigger with a transition table. We allow transition tables
-- for update and insert triggers.
create table test2(
       created_at timestamptz not null unique,
       location_id bigint,
       temp float8
);

select create_hypertable('test2', by_range('created_at'));

create trigger save_test2_insert_trg
       after insert on test2
       referencing new table as new_table
       for each statement execute function save_transition_table();
create trigger save_test2_update_trg
       after update on test2
       referencing new table as new_table old table as old_table
       for each statement execute function save_transition_table();
create trigger save_test2_delete_trg
       after delete on test2
       referencing old table as old_table
       for each statement execute function save_transition_table();


-- This should fail
\set ON_ERROR_STOP 0
alter table test2 set (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'created_at',
	  timescaledb.compress_segmentby = 'location_id'
);
\set ON_ERROR_STOP 1

-- drop the delete trigger
drop trigger save_test2_delete_trg on test2;

-- This should now succeed.
alter table test2 set (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'created_at',
	  timescaledb.compress_segmentby = 'location_id'
);
