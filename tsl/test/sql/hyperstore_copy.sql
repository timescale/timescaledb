-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hyperstore.sql

create view amrels as
select cl.oid::regclass as rel, am.amname, inh.inhparent::regclass as relparent
  from pg_class cl
  inner join pg_am am on (cl.relam = am.oid)
  left join pg_inherits inh on (inh.inhrelid = cl.oid);

-- Compress the chunks and check that the counts are the same
select location_id, count(*) into orig from :hypertable GROUP BY location_id;
select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');
select location_id, count(*) into comp from :hypertable GROUP BY location_id;
select * from orig join comp using (location_id) where orig.count != comp.count;
drop table orig, comp;

-- Check that all chunks are compressed
select chunk_name, compression_status from chunk_compression_stats(:'hypertable');

-- Check that a simple copy will work. This will insert into uncompressed region.

copy :hypertable(created_at, location_id, device_id, temp, humidity) from stdin;
2022-06-01 00:00:02	1	1	1.0	1.0
2022-06-01 00:00:03	1	1	1.0	1.0
2022-06-01 00:00:04	1	1	1.0	1.0
2022-06-01 00:00:05	1	1	1.0	1.0
\.

select created_at, location_id, device_id, temp, humidity
from :hypertable where created_at between '2022-06-01 00:00:01' and '2022-06-01 00:00:09';

select created_at, location_id, device_id, temp, humidity into orig
from :hypertable where created_at between '2022-06-01 00:00:01' and '2022-06-01 00:00:09';

-- Insert a batch of rows conflicting with compressed rows
\set ON_ERROR_STOP 0
copy :hypertable(created_at, location_id, device_id, temp, humidity) from stdin;
2022-06-01 00:00:00	1	1	1.0	1.0
2022-06-01 00:00:10	1	1	1.0	1.0
\.
\set ON_ERROR_STOP 1

-- Insert a batch of rows conflicting with non-compressed rows
\set ON_ERROR_STOP 0
copy :hypertable(created_at, location_id, device_id, temp, humidity) from stdin;
2022-06-01 00:00:01	1	1	1.0	1.0
2022-06-01 00:00:02	1	1	1.0	1.0
2022-06-01 00:00:06	1	1	1.0	1.0
\.
\set ON_ERROR_STOP 1

select created_at, location_id, device_id, temp, humidity into curr
from :hypertable where created_at between '2022-06-01 00:00:01' and '2022-06-01 00:00:09';

select * from orig join curr using (created_at) where row(orig) != row(curr);

-- Read data from a file and make sure that it is inserted properly.
create table copy_test1(
       metric_id int,
       created_at timestamptz,
       device_id int,
       temp float,
       humidity float
);
select create_hypertable('copy_test1', 'created_at');
alter table copy_test1 set access method hyperstore;
\copy copy_test1 from 'data/magic.csv' with csv header
select * from copy_test1 order by metric_id;

-- Here I wanted to write a subset of the data to a csv file and then
-- load it again, but this statement fails.
--
-- select * into subset from :hypertable where device_id between 0 and 10;

-- Testing copy into a table that has the access method set but has
-- already existing (compressed) heap-chunks in the table.
CREATE TABLE test1 (timec timestamptz , i integer, b bigint, t text);
SELECT table_name from create_hypertable('test1', 'timec');
INSERT INTO test1 SELECT q, 10, 11, 'hello'
FROM generate_series( '2020-01-03 10:00:00-05', '2020-01-03 12:00:00-05' , '5 min'::interval) q;
alter table test1 set (timescaledb.compress_segmentby = 'b', timescaledb.compress_orderby = 'timec DESC');
SELECT compress_chunk(show_chunks('test1'));

select count(*) from test1;

select * from amrels where relparent = 'test1'::regclass;

alter table test1 set access method hyperstore;

copy test1 from stdin delimiter ',';
2020-01-02 11:16:00-05,11,16,copy
\.

select count(*) from test1;
