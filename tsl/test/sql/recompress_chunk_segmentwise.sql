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
   comp.table_name as compressed_chunk_name,
   c.id as chunk_id
FROM
   _timescaledb_catalog.hypertable h JOIN
  _timescaledb_catalog.chunk c ON h.id = c.hypertable_id
   LEFT JOIN _timescaledb_catalog.chunk comp
ON comp.id = c.compressed_chunk_id
;

CREATE OR REPLACE VIEW compression_rowcnt_view AS
select ccs.numrows_pre_compression, ccs.numrows_post_compression,
(v.chunk_schema || '.' || v.chunk_name) as chunk_name,
v.chunk_id as chunk_id
 from _timescaledb_catalog.compression_chunk_size ccs
join compressed_chunk_info_view v on ccs.chunk_id = v.chunk_id;

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

-- after compressing the chunk
select numrows_pre_compression, numrows_post_compression from _timescaledb_catalog.compression_chunk_size;

insert into mytab_oneseg values ('2023-01-01 19:56:20.048355+02'::timestamptz, 2, NULL, 2);
-- after inserting new row in compressed chunk
select numrows_pre_compression, numrows_post_compression from _timescaledb_catalog.compression_chunk_size;

select _timescaledb_internal.recompress_chunk_segmentwise(:'chunk_to_compress_1');

-- check the ctid of the rows in the recompressed chunk to verify that we've written new data
SELECT ctid, * FROM :compressed_chunk_name_1;
-- after recompressing chunk
select numrows_pre_compression, numrows_post_compression from _timescaledb_catalog.compression_chunk_size;

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

-- should have 2 compressed rows
-- select numrows_pre_compression, numrows_post_compression from _timescaledb_catalog.compression_chunk_size ccs
-- join compressed_chunk_info_view v on ccs.chunk_id = v.chunk_id where v.compressed_chunk_schema || '.' || v.compressed_chunk_name
--  = :'chunk_to_compress_2';
select * from compression_rowcnt_view where chunk_name = :'chunk_to_compress_2';

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

-- should still have 2 compressed rows
select * from compression_rowcnt_view where chunk_name = :'chunk_to_compress_2';

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
-- after compression
select * from compression_rowcnt_view where chunk_name = :'chunk_to_compress_2';

select _timescaledb_internal.recompress_chunk_segmentwise(:'chunk_to_compress_2');

select ctid, * from :compressed_chunk_name_2;
-- after recompression
select * from compression_rowcnt_view where chunk_name = :'chunk_to_compress_2';

-- failing test from compression_ddl
CREATE TABLE test_defaults(time timestamptz NOT NULL, device_id int);
SELECT create_hypertable('test_defaults','time');

ALTER TABLE test_defaults SET (timescaledb.compress,timescaledb.compress_segmentby='device_id');

-- create 2 chunks
INSERT INTO test_defaults SELECT '2000-01-01', 1;
INSERT INTO test_defaults SELECT '2001-01-01', 1;

SELECT compress_chunk(show_chunks) AS "compressed_chunk" FROM show_chunks('test_defaults') ORDER BY show_chunks::text LIMIT 1 \gset

select * from compression_rowcnt_view where chunk_name = :'compressed_chunk';

SELECT * FROM test_defaults ORDER BY 1;

ALTER TABLE test_defaults ADD COLUMN c1 int;
ALTER TABLE test_defaults ADD COLUMN c2 int NOT NULL DEFAULT 42;
SELECT * FROM test_defaults ORDER BY 1,2;

INSERT INTO test_defaults SELECT '2000-01-01', 2;
SELECT * FROM test_defaults ORDER BY 1,2;

call recompress_chunk(:'compressed_chunk');
SELECT * FROM test_defaults ORDER BY 1,2;
-- here we will have an additional compressed row after recompression because the new
-- data corresponds to a new segment
select * from compression_rowcnt_view where chunk_name = :'compressed_chunk';

-- test prepared statements
-- PREPRE A SELECT before recompress and perform it after recompress
CREATE TABLE mytab_prep (time timestamptz, a int, b int, c int);
SELECT create_hypertable('mytab_prep', 'time');
INSERT INTO mytab_prep VALUES ('2023-01-01'::timestamptz, 2, NULL, 2),
('2023-01-01'::timestamptz, 2, NULL, 2);

alter table mytab_prep set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');

PREPARE p1 AS
SELECT * FROM mytab_prep;

select show_chunks as chunk_to_compress_prep from show_chunks('mytab_prep') limit 1 \gset
SELECT compress_chunk(:'chunk_to_compress_prep'); -- the output of the prepared plan would change before and after compress
INSERT INTO mytab_prep VALUES ('2023-01-01'::timestamptz, 2, 3, 2);

-- plan should be invalidated to return results from the uncompressed chunk also
EXPLAIN (COSTS OFF) EXECUTE p1;
EXECUTE p1;

-- check plan again after recompression
CALL recompress_chunk(:'chunk_to_compress_prep');
EXPLAIN (COSTS OFF) EXECUTE p1;
EXECUTE p1;

-- verify segmentwise recompression when index exists, decompress + compress otherwise
-- we verify by checking the compressed chunk after recompression in both cases.
-- in the first case, it is the same before and after
-- in the second case, a new compressed chunk is created
CREATE TABLE mytab (time timestamptz, a int, b int, c int);
SELECT create_hypertable('mytab', 'time');
INSERT INTO mytab VALUES ('2023-01-01'::timestamptz, 2, NULL, 2),
('2023-01-01'::timestamptz, 2, NULL, 2);

select show_chunks as chunk_to_compress_mytab from show_chunks('mytab') limit 1 \gset
-- index exists, recompression should happen segment by segment so expect a  debug message
alter table mytab set (timescaledb.compress, timescaledb.compress_segmentby = 'a, c');
select compress_chunk(show_chunks('mytab'));
select compressed_chunk_name as compressed_chunk_name_before_recompression from compressed_chunk_info_view where hypertable_name = 'mytab' \gset
INSERT INTO mytab VALUES ('2023-01-01'::timestamptz, 2, 3, 2);
-- segmentwise recompression should not create a new compressed chunk, so verify compressed chunk is the same after recompression
call recompress_chunk(:'chunk_to_compress_mytab');
select compressed_chunk_name as compressed_chunk_name_after_recompression from compressed_chunk_info_view where hypertable_name = 'mytab' \gset
select :'compressed_chunk_name_before_recompression' as before_segmentwise_recompression, :'compressed_chunk_name_after_recompression' as after_segmentwise_recompression;

SELECT decompress_chunk(show_chunks('mytab'));
alter table mytab set (timescaledb.compress = false);
alter table mytab set (timescaledb.compress);
select compress_chunk(show_chunks('mytab'));
select compressed_chunk_name as compressed_chunk_name_before_recompression from compressed_chunk_info_view where hypertable_name = 'mytab' \gset
INSERT INTO mytab VALUES ('2023-01-01'::timestamptz, 2, 3, 2);
-- expect to see a different compressed chunk after recompressing now as the operation is decompress + compress
call recompress_chunk(:'chunk_to_compress_mytab');
select compressed_chunk_name as compressed_chunk_name_after_recompression from compressed_chunk_info_view where hypertable_name = 'mytab' \gset
select :'compressed_chunk_name_before_recompression' as before_recompression, :'compressed_chunk_name_after_recompression' as after_recompression;

-- check behavior with NULL values in segmentby columns
select '2022-01-01 09:00:00+00' as start_time \gset
create table nullseg_one (time timestamptz, a int, b int);

select create_hypertable('nullseg_one', 'time');

insert into nullseg_one values (:'start_time', 1, 1), (:'start_time', 1, 2), (:'start_time', 2,2), (:'start_time', 2,3);

alter table nullseg_one set (timescaledb.compress, timescaledb.compress_segmentby= 'a');
select compress_chunk(show_chunks('nullseg_one'));

insert into nullseg_one values (:'start_time', NULL, 4);

select show_chunks as chunk_to_compress from show_chunks('nullseg_one') limit 1 \gset
select compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name from compressed_chunk_info_view where hypertable_name = 'nullseg_one' \gset

call recompress_chunk(:'chunk_to_compress');

select * from :compressed_chunk_name;
-- insert again, check both reindex works and NULL values properly handled
insert into nullseg_one values (:'start_time', NULL, 4);
call recompress_chunk(:'chunk_to_compress');
select * from :compressed_chunk_name;

-- test multiple NULL segmentby columns
create table nullseg_many (time timestamptz, a int, b int, c int);

select create_hypertable('nullseg_many', 'time');

insert into nullseg_many values (:'start_time', 1, 1, 1), (:'start_time', 1, 2, 2), (:'start_time', 2,2, 2), (:'start_time', 2,3, 3), (:'start_time', 2, NULL, 3);

alter table nullseg_many set (timescaledb.compress, timescaledb.compress_segmentby= 'a, c');
select compress_chunk(show_chunks('nullseg_many'));
-- new segment (1, NULL)
insert into nullseg_many values (:'start_time', 1, 4, NULL);

select show_chunks as chunk_to_compress from show_chunks('nullseg_many') limit 1 \gset
select compressed_chunk_schema || '.' || compressed_chunk_name as compressed_chunk_name from compressed_chunk_info_view where hypertable_name = 'nullseg_many' \gset

call recompress_chunk(:'chunk_to_compress');

select * from :compressed_chunk_name;
-- insert again, check both reindex works and NULL values properly handled
-- should match existing segment (1, NULL)
insert into nullseg_many values (:'start_time', 1, NULL, NULL);
call recompress_chunk(:'chunk_to_compress');
select * from :compressed_chunk_name;
