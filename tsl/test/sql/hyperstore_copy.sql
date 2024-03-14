-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hyperstore.sql

-- Compress the chunks and check that the counts are the same
select location_id, count(*) into orig from :hypertable GROUP BY location_id;
select twist_chunk(show_chunks(:'hypertable'));
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
