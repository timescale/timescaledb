-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE VIEW compressed_chunk_info_view AS
SELECT
   h.schema_name AS hypertable_schema,
   h.table_name AS hypertable_name,
   c.schema_name as chunk_schema,
   c.table_name as chunk_name,
   c.status as chunk_status,
   comp.schema_name as compressed_chunk_schema,
   comp.table_name as compressed_chunk_name
FROM
   _timescaledb_catalog.hypertable h JOIN
  _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
   LEFT JOIN _timescaledb_catalog.chunk comp
ON comp.id = c.compressed_chunk_id
;

-- test1: one affected segment, one unaffected
create table mytab (time timestamptz not null, a int, b int, c int);

SELECT create_hypertable('mytab', 'time', chunk_time_interval => interval '1 day');

insert into mytab values 
('2023-01-01 21:56:20.048355+02'::timestamptz, 2, NULL, 2),
('2023-01-01 21:56:20.048355+02'::timestamptz, 3, NULL, 3); --same chunk diff segment

alter table mytab set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');


select show_chunks as chunk_to_compress from show_chunks('mytab') limit 1 \gset 

select compress_chunk(:'chunk_to_compress');

insert into mytab values ('2023-01-01 19:56:20.048355+02'::timestamptz, 2, NULL, 2);

select * from :chunk_to_compress;

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name
from compressed_chunk_info_view where hypertable_name = 'mytab' \gset

select * from :compressed_chunk_name;

select recompress_chunk_experimental(:'chunk_to_compress');

-- verify that metadata count looks good
select * from :compressed_chunk_name;

-- verify that initial data is returned as expected
select * from :chunk_to_compress;

-- looks like meta_seq_number is not restarting from 10 as it should do. Fix that.
-- need to test what happens with merge chunks

-- ------------- test2 . only one segment exists and only one segment affected ---------

-- create table mytab (time timestamptz not null, a int, b int, c int);

-- SELECT create_hypertable('mytab', 'time', chunk_time_interval => interval '1 day');

-- insert into mytab values 
-- ('2023-01-01 21:56:20.048355+02'::timestamptz, 2, NULL, 2),
-- ('2023-01-01 21:56:10.048355+02'::timestamptz, 2, NULL, 2), --same chunk same segment
-- ('2023-01-03 20:56:20.048355+02'::timestamptz, 3, NULL, 3);

-- alter table mytab set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');

-- select compress_chunk('_timescaledb_internal._hyper_1_1_chunk');

-- insert into mytab values ('2023-01-01 19:56:20.048355+02'::timestamptz, 2, NULL, 2);

-- select * from _timescaledb_internal._hyper_1_1_chunk;

-- select recompress_chunk_experimental('_timescaledb_internal._hyper_1_1_chunk');