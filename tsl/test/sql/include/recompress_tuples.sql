-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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

-- TEST1 test recompress_tuples has 1 output row per segment --
CREATE TABLE test3 (timec timestamptz NOT NULL, i integer ,
      segcol bigint, t text);
SELECT table_name from create_hypertable('test3', 'timec', chunk_time_interval=> INTERVAL '7 days');

-- insert into 2 segments and then compress
INSERT INTO test3 SELECT q, 10, 11, 'hello' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 12:00:00+00' , '5 min'::interval) q;
INSERT INTO test3 SELECT q, 10, 15, 'hello' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 12:00:00+00' , '5 min'::interval) q;

ALTER TABLE test3 set (timescaledb.compress, 
timescaledb.compress_segmentby = 'segcol', 
timescaledb.compress_orderby = 'timec DESC');

SELECT compress_chunk(c)
FROM show_chunks('test3') c;

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as "COMP_CHUNK_NAME",
        chunk_schema || '.' || chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view 
WHERE hypertable_name = 'test3' \gset

--insert into middle of a range of compressed chunk
INSERT INTO test3 values ( '2020-01-03 10:01:00+00', 20, 11, '2row');
INSERT INTO test3 values ( '2020-01-03 11:01:00+00', 20, 11, '3row');
INSERT INTO test3 values ( '2020-01-03 12:01:00+00', 20, 11, '4row');
--- insert a new segment  by ---
INSERT INTO test3 values ( '2020-01-03 11:01:00+00', 20, 12, '12row');

SELECT segcol, _timescaledb_internal.recompress_tuples(:'CHUNK_NAME'::regclass, c ) arr 
FROM :COMP_CHUNK_NAME c  WHERE segcol = 11 group by segcol;

\set TABLE_NAME test3 
\ir recompress_test.sql

-- TEST2 test recompress_tuples has > 1 output row per segment --
SELECT segcol, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1
FROM :COMP_CHUNK_NAME
ORDER BY 1, 3;

-- insert into 1 segments and then compress
INSERT INTO test3 SELECT q, 10, 12, 'seg12' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 11:00:00+00' , '1 s'::interval) q;

\set TABLE_NAME test3 
\ir recompress_test.sql

SELECT segcol, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1
FROM :COMP_CHUNK_NAME
ORDER BY 1, 3;
