-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

-- To generate plans consistently.
set max_parallel_workers_per_gather to 0;

-- Create a function that uses a cursor to scan the the Hypercore
-- table. This should work equivalent to a query on the same table.
create function location_humidity_for(
       in p_owner integer,
       out p_location integer,
       out p_humidity float)
returns setof record as
$$
declare
    location_record record;
    location_cursor cursor for
      select location_id,
      	     avg(humidity) as avg_humidity
      from readings
      where owner_id = p_owner
      group by location_id;
begin
    open location_cursor;

    loop
        fetch next from location_cursor into location_record;
        exit when not found;

        p_location = location_record.location_id;
        p_humidity = location_record.avg_humidity;
        return next;
    end loop;

    close location_cursor;
end;
$$
language plpgsql;

select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hypercore');

-- Compare executing the function with a cursor with a query fetching
-- the same data directly from the hypertable.
select p_location, lhs.p_humidity, rhs.p_humidity
  from (select * from location_humidity_for(1)) lhs
  join (select location_id as p_location,
               avg(humidity) as p_humidity
          from :hypertable
         where owner_id = 1
        group by location_id) rhs
  using (p_location)
 where lhs.p_humidity != rhs.p_humidity
order by p_location;

-- Create a function that will use a cursor to iterate through a table
-- and update the humidity for a location using a cursor.
create function update_location_humidity(
       in p_location integer,
       in p_humidity float)
returns setof record as
$$
declare
    location_record record;
    location_cursor cursor for
      select location_id, humidity from readings where location_id = p_location;
begin
    open location_cursor;

    loop
        move next in location_cursor;
        exit when not found;
	update readings set humidity = p_humidity where current of location_cursor;
    end loop;

    close location_cursor;
end;
$$
language plpgsql;

set timescaledb.max_tuples_decompressed_per_dml_transaction to 0;
create table saved as select * from :hypertable;

-- These two should generate the same result
update saved set humidity = 100.0 where location_id = 10;
select update_location_humidity(10, 100.0);

-- This should show no rows, but if there are differences we limit
-- this to 10 rows to not waste electrons.
--
-- Note that update of compressed tables through a cursor does not
-- work for all compressed tables right now because of the way the
-- local ExecModifyTable is implemented, so this will show rows.
select metric_id, lhs.humidity, rhs.humidity
  from saved lhs full join :hypertable rhs using (metric_id)
 where lhs.humidity != rhs.humidity
order by metric_id limit 10;

drop function location_humidity_for;
drop function update_location_humidity;


-- Test cursor going backwards
create table backward_cursor (time timestamptz, location_id bigint, temp float8);
select create_hypertable('backward_cursor', 'time', create_default_indexes=>false);
alter table backward_cursor set (timescaledb.compress, timescaledb.compress_segmentby='location_id', timescaledb.compress_orderby='time asc');
insert into backward_cursor values ('2024-01-01 01:00', 1, 1.0), ('2024-01-01 02:00', 1, 2.0), ('2024-01-01 03:00', 2, 3.0), ('2024-01-01 04:00', 2, 4.0);
select compress_chunk(ch, compress_using=>'hypercore') from show_chunks('backward_cursor') ch;
insert into backward_cursor values ('2024-01-01 05:00', 3, 5.0), ('2024-01-01 06:00', 3, 6.0);

begin;
-- This needs to be a simple scan on top of the baserel, without a
-- materialization. For scan nodes that don't support backwards scans,
-- or where a sort or similar happens, the query is typically
-- materialized first, thus not really testing the TAMs ability to do
-- backwards scanning.
explain (costs off)
declare curs1 cursor for
select _timescaledb_debug.is_compressed_tid(ctid), * from backward_cursor;
declare curs1 cursor for
select _timescaledb_debug.is_compressed_tid(ctid), * from backward_cursor;

-- Immediately fetching backward should return nothing
fetch backward 1 from curs1;

-- Now read some values forward
fetch forward 1 from curs1;
fetch forward 1 from curs1;
-- The next fetch should move into a new segment with location_id=2
fetch forward 1 from curs1;
-- Last compressed entry
fetch forward 1 from curs1;
-- Now should move into non-compressed
fetch forward 1 from curs1;
-- Last entry in non-compressed
fetch forward 1 from curs1;
-- Should return nothing since at end
fetch forward 1 from curs1;
-- Now move backwards
fetch backward 1 from curs1;
-- Now backwards into the old segment
fetch backward 5 from curs1;
-- Next fetch should return nothing since at start
fetch backward 1 from curs1;
-- Fetch first value again
fetch forward 1 from curs1;
-- Jump to last value
fetch last from curs1;
-- Back to first
fetch first from curs1;
-- Get the values at position 2 and 5 from the start
fetch absolute 2 from curs1;
fetch absolute 5 from curs1;
-- Get the value at position 3 from the end (which should be 4 from
-- the start)
fetch absolute -3 from curs1;
commit;
