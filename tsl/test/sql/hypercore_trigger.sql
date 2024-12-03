-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

create table saved_rows (like :chunk1, new_row bool not null, kind text);
create table count_stmt (inserts int, updates int, deletes int);

create function save_row() returns trigger as $$
begin
   if new is not null then
       insert into saved_rows select new.*, true, tg_op;
   end if;
   if old is not null then
       insert into saved_rows select old.*, false, tg_op;
   end if;
   return new;
end;
$$ language plpgsql;

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

create function count_ops() returns trigger as $$
begin
   insert into count_stmt values (
   	  (tg_op = 'INSERT')::int,
	  (tg_op = 'UPDATE')::int,
	  (tg_op = 'DELETE')::int
   );
   return null;
end;
$$ language plpgsql;

create function notify_action() returns trigger as $$
begin
   raise notice 'table % was truncated', tg_table_name;
   return null;
end;
$$ language plpgsql;

-- Compress all the chunks and make sure that they are compressed
select compress_chunk(show_chunks(:'hypertable'), hypercore_use_access_method => true);
select chunk_name, compression_status from chunk_compression_stats(:'hypertable');

with the_info as (
     select min(created_at) min_created_at,
     	    max(created_at) max_created_at
       from :hypertable
)
select min_created_at,
       max_created_at,
       '1m'::interval + min_created_at + (max_created_at - min_created_at) as mid_created_at,
       '1m'::interval + max_created_at + (max_created_at - min_created_at) as post_created_at
  from the_info \gset

-- Insert a bunch of rows to make sure that we have a mix of
-- uncompressed, partially compressed, and fully compressed
-- chunks. Note that there is is an overlap between the compressed and
-- uncompressed start and end timestamps.
insert into :hypertable (created_at, location_id, device_id, owner_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), ceil(random() * 5), random()*40, random()*100
from generate_series(:'mid_created_at'::timestamptz, :'post_created_at', '15m') t;

-- Create a table with some samples that we can re-use to generate conflicts.
create table sample (like :chunk1 including generated including defaults including constraints);
insert into sample(created_at, location_id, device_id, owner_id, temp, humidity)
  values
	('2022-06-01 00:01:23', 999, 666, 111, 3.14, 3.14),
	('2022-06-01 00:02:23', 999, 666, 112, 3.14, 3.14),
	('2022-06-01 00:03:23', 999, 666, 113, 3.14, 3.14),
	('2022-06-01 00:04:23', 999, 666, 114, 3.14, 3.14);

-- Start by testing insert triggers for both statements and rows. In
-- this case, the trigger will just save away the rows into a separate
-- table and check that we get the same number of rows with the same
-- values.
create trigger save_insert_row_trg
       before insert on :chunk1
       for each row execute function save_row();
create trigger count_inserts_trg
       before insert on :chunk1
       for each statement execute function count_ops();

insert into :chunk1(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample limit 2;

select * from saved_rows where kind = 'INSERT';
select sum(inserts), sum(updates), sum(deletes) from count_stmt;

truncate saved_rows, count_stmt;

merge into :chunk1 c using sample s on c.created_at = s.created_at
when not matched then insert values (s.*);

select * from saved_rows;
select sum(inserts), sum(updates), sum(deletes) from count_stmt;

truncate saved_rows, count_stmt;

drop trigger save_insert_row_trg on :chunk1;
drop trigger count_inserts_trg on :chunk1;

-- Run update and upsert tests
create trigger save_update_row_trg
       before update on :chunk1
       for each row execute function save_row();
create trigger count_update_trg
       before update on :chunk1
       for each statement execute function count_ops();

update :chunk1 set temp = 9.99 where device_id = 666;

select * from saved_rows where kind = 'UPDATE';
select sum(inserts), sum(updates), sum(deletes) from count_stmt;

truncate saved_rows, count_stmt;

-- Upsert with conflicts on previously inserted rows
insert into :chunk1(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample
on conflict (created_at) do update set temp = 6.66, device_id = 666;

select * from saved_rows where kind = 'UPDATE';
select sum(inserts), sum(updates), sum(deletes) from count_stmt;

truncate saved_rows, count_stmt;

drop trigger save_update_row_trg on :chunk1;
drop trigger count_update_trg on :chunk1;

-- Run delete tests
create trigger save_delete_row_trg
       before delete on :chunk1
       for each row execute function save_row();
create trigger count_delete_trg
       before delete on :chunk1
       for each statement execute function count_ops();

delete from :chunk1 where device_id = 666;

select * from saved_rows where kind = 'DELETE';
select sum(inserts), sum(updates), sum(deletes) from count_stmt;

truncate saved_rows, count_stmt;

drop trigger save_delete_row_trg on :chunk1;
drop trigger count_delete_trg on :chunk1;

-- Remove duplicates from readings table so that we can run the tests
-- below.
delete from readings where created_at in (select created_at from sample);

create trigger save_insert_transition_table_trg
       after insert on :chunk1
       referencing new table as new_table
       for each statement execute function save_transition_table();

insert into :chunk1(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample
order by created_at limit 2;

select * from saved_rows;

truncate saved_rows;

copy :chunk1(created_at, location_id, device_id, owner_id, temp, humidity) from stdin with (format csv);
"2022-06-01 00:01:35",999,666,111,3.14,3.14
\.

select * from saved_rows;

truncate saved_rows;

create trigger save_update_transition_table_trg
       after update on :chunk1
       referencing new table as new_table old table as old_table
       for each statement execute function save_transition_table();

select * from :chunk1 where location_id = 999;

update :chunk1 set humidity = 99.99 where location_id = 999;

select * from saved_rows;

truncate saved_rows;

create trigger save_delete_transition_table_trg
       after delete on :chunk1
       referencing old table as old_table
       for each statement execute function save_transition_table();

select * from :chunk1 where location_id = 999;

delete from :chunk1 where location_id = 999;

select * from saved_rows;

truncate saved_rows;

-- Check truncate trigger
create trigger notify_truncate
       after truncate on :chunk1
       for each statement execute function notify_action();

truncate :chunk1;
