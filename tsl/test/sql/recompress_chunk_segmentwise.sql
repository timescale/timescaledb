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

------------- only one segment exists and only one segment affected ---------

create table mytab_oneseg (time timestamptz not null, a int, b int, c int);

SELECT create_hypertable('mytab_oneseg', 'time', chunk_time_interval => interval '1 day');

insert into mytab_oneseg values 
('2023-01-01 21:56:20.048355+02'::timestamptz, 2, NULL, 2),
('2023-01-01 21:56:10.048355+02'::timestamptz, 2, NULL, 2); --same chunk same segment

alter table mytab_oneseg set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');

select show_chunks as chunk_to_compress_1 from show_chunks('mytab_oneseg') limit 1 \gset 

select compress_chunk(:'chunk_to_compress_1');

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name_1
from compressed_chunk_info_view where hypertable_name = 'mytab_oneseg' \gset

SELECT ctid, * FROM :compressed_chunk_name_1;

insert into mytab_oneseg values ('2023-01-01 19:56:20.048355+02'::timestamptz, 2, NULL, 2);

select _timescaledb_internal.recompress_chunk_segmentwise(:'chunk_to_compress_1');

-- check the ctid of the rows in the recompressed chunk to verify that we've written new data
SELECT ctid, * FROM :compressed_chunk_name_1;

---------------- test1: one affected segment, one unaffected --------------
-- unaffected segment will still be recompressed in a future PR we want to avoid doing this
create table mytab_twoseg (time timestamptz not null, a int, b int, c int);

SELECT create_hypertable('mytab_twoseg', 'time', chunk_time_interval => interval '1 day');

insert into mytab_twoseg values 
('2023-01-01 21:56:20.048355+02'::timestamptz, 2, NULL, 2),
('2023-01-01 21:56:20.048355+02'::timestamptz, 3, NULL, 3), --same chunk diff segment
('2023-01-01 21:57:20.048355+02'::timestamptz, 3, NULL, 3);

alter table mytab_twoseg set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');

select show_chunks as chunk_to_compress_2 from show_chunks('mytab_twoseg') limit 1 \gset 

select compress_chunk(:'chunk_to_compress_2');

insert into mytab_twoseg values ('2023-01-01 19:56:20.048355+02'::timestamptz, 2, NULL, 2);

select * from :chunk_to_compress_2;

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name_2
from compressed_chunk_info_view where hypertable_name = 'mytab_twoseg' \gset

select ctid, * from :compressed_chunk_name_2;

select _timescaledb_internal.recompress_chunk_segmentwise(:'chunk_to_compress_2');

-- verify that metadata count looks good
select ctid, * from :compressed_chunk_name_2;

-- verify that initial data is returned as expected
select * from :chunk_to_compress_2;

----------------- more than one batch per segment ----------------------
-- test that metadata sequence number is correct
create table mytab2(time timestamptz not null, a int, b int, c int);

select create_hypertable('mytab2', 'time', chunk_time_interval => interval '1 week');

insert into mytab2 (time, a, c) select t,s,s from 
generate_series('2023-01-01 00:00:00+00'::timestamptz, '2023-01-01 00:00:00+00'::timestamptz + interval '1 day', interval '30 sec') t cross join generate_series(0,2, 1) s;

alter table mytab2 set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');

select compress_chunk(c) from show_chunks('mytab2') c;  

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name_2
from compressed_chunk_info_view where hypertable_name = 'mytab2' 
and compressed_chunk_name is not null limit 1 \gset

insert into mytab2 values ('2023-01-01 00:00:02+00'::timestamptz, 0, NULL, 0); -- goes into the uncompressed chunk

select show_chunks('mytab2') as chunk_to_compress_2 \gset

select ctid, * from :compressed_chunk_name_2;

select _timescaledb_internal.recompress_chunk_segmentwise(:'chunk_to_compress_2');

select ctid, * from :compressed_chunk_name_2;
