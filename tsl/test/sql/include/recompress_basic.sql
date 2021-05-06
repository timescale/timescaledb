-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE OR REPLACE VIEW compressed_chunk_info_view AS
SELECT
   h.schema_name AS hypertable_schema,
   h.table_name AS hypertable_name,
   c.schema_name as chunk_schema,
   c.table_name as chunk_name,
   comp.schema_name as compressed_chunk_schema,
   comp.table_name as compressed_chunk_name
FROM
   _timescaledb_catalog.hypertable h,
  _timescaledb_catalog.chunk c,
   _timescaledb_catalog.chunk comp
WHERE h.id = c.hypertable_id and comp.id = c.compressed_chunk_id
and c.compressed_chunk_id > 0;

CREATE TABLE test2 (timec timestamptz , i integer ,
      b bigint, t text);
SELECT table_name from create_hypertable('test2', 'timec', chunk_time_interval=> INTERVAL '7 days');

INSERT INTO test2 SELECT q, 10, 11, 'hello' FROM generate_series( '2020-01-03 10:00:00-05', '2020-01-03 12:00:00-05' , '5 min'::interval) q;
ALTER TABLE test2 set (timescaledb.compress, 
timescaledb.compress_segmentby = 'b', 
timescaledb.compress_orderby = 'timec DESC');

SELECT compress_chunk(c)
FROM show_chunks('test2') c;

---insert into the middle of the range ---
INSERT INTO test2 values ( '2020-01-03 10:01:00-05', 20, 11, '2row'); 
INSERT INTO test2 values ( '2020-01-03 11:01:00-05', 20, 11, '3row'); 
INSERT INTO test2 values ( '2020-01-03 12:01:00-05', 20, 11, '4row'); 
--- insert a new segment  by ---
INSERT INTO test2 values ( '2020-01-03 11:01:00-05', 20, 12, '12row'); 

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as "COMP_CHUNK_NAME",
        chunk_schema || '.' || chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view 
WHERE hypertable_name = 'test2' \gset

SELECT count(*) from test2;

SELECT :'CHUNK_NAME', 'got here';
SELECT b, _timescaledb_internal.recompress_tuples(:'CHUNK_NAME'::regclass, c ) arr 
FROM :COMP_CHUNK_NAME c  group by b;

