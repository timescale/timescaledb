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

-- insert into 1 segments and then recompress
INSERT INTO test3 SELECT q, 10, 12, 'seg12' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 11:00:00+00' , '1 s'::interval) q;

SELECT segcol, min(_ts_meta_sequence_num)
FROM :COMP_CHUNK_NAME
WHERE _ts_meta_sequence_num > 0
GROUP BY segcol
ORDER BY 1;

\set TABLE_NAME test3 
\ir recompress_test.sql

SELECT segcol, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1
FROM :COMP_CHUNK_NAME
ORDER BY 1, 3, 4, 5;

--TEST3 test recompress_tuples has > 1 output row, now affects multiple segment bys.
-- and add a brand new segment-by
--existing segement=12, new range and middle of a range
INSERT INTO test3 SELECT q, 10, 12, 'seg1222' FROM generate_series( '2020-01-03 10:59:00+00', '2020-01-03 12:30:00+00' , '1 s'::interval) q;
--new segment = 14
INSERT INTO test3 SELECT q, 10, 14, 'seg1444' FROM generate_series( '2020-01-03 10:59:00+00', '2020-01-03 13:00:00+00' , '1 s'::interval) q;
INSERT INTO test3 SELECT q, 10, 11, 'seg1111' FROM generate_series( '2020-01-03 12:30:00+00', '2020-01-03 13:00:00+00' , '1 s'::interval) q;

SELECT segcol, min(_ts_meta_sequence_num)
FROM :COMP_CHUNK_NAME
WHERE _ts_meta_sequence_num > 0
GROUP BY segcol
ORDER BY 1;

\set TABLE_NAME test3
\ir recompress_test.sql

SELECT segcol, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1
FROM :COMP_CHUNK_NAME
ORDER BY 1, 3, 4, 5;

---- TEST4 table with multiple segment by---
CREATE TABLE test_multseg (timec timestamptz NOT NULL, segcol2 integer ,
      segcol bigint, t text);
SELECT table_name from create_hypertable('test_multseg', 'timec', chunk_time_interval=> INTERVAL '7 days');

-- insert into 2 segments and then compress
INSERT INTO test_multseg SELECT q, 100, 11, 'seg11_100' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 12:00:00+00' , '5 min'::interval) q;
INSERT INTO test_multseg SELECT q, 200, 11, 'seg11_200' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 12:00:00+00' , '5 min'::interval) q;
INSERT INTO test_multseg SELECT q, 900, 15, 'seg15_900' FROM generate_series( '2020-01-03 10:00:00+00', '2020-01-03 12:00:00+00' , '5 min'::interval) q;

ALTER TABLE test_multseg set (timescaledb.compress, 
timescaledb.compress_segmentby = 'segcol, segcol2', 
timescaledb.compress_orderby = 'timec DESC');

SELECT compress_chunk(c)
FROM show_chunks('test_multseg') c;

SELECT compressed_chunk_schema || '.' || compressed_chunk_name as "COMP_CHUNK_NAME",
        chunk_schema || '.' || chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view 
WHERE hypertable_name = 'test_multseg' \gset

--now insert into all the existing segments --
INSERT INTO test_multseg values ( '2020-01-03 10:01:00+00', 100, 11, '2row');
INSERT INTO test_multseg values ( '2020-01-03 11:01:00+00', 200, 11, '3row');
INSERT INTO test_multseg values ( '2020-01-03 12:01:00+00', 900, 15, '4row');
--- insert a new segment  by ---
INSERT INTO test_multseg values ( '2020-01-03 11:01:00+00', 20, 12, '12row');

SELECT segcol, segcol2, min(_ts_meta_sequence_num)
FROM :COMP_CHUNK_NAME
WHERE _ts_meta_sequence_num > 0
GROUP BY segcol, segcol2
ORDER BY 1;

\set TABLE_NAME test_multseg
\ir recompress_test2.sql

SELECT segcol, segcol2, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1
FROM :COMP_CHUNK_NAME
ORDER BY 1, 2, 3, 4, 5;
