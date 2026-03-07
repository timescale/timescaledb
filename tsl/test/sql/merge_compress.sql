-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test MERGE on compressed hypertables

CREATE TABLE target (
    time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    series_id BIGINT NOT NULL,
    partition_column TIMESTAMPTZ NOT NULL
);

SELECT table_name FROM create_hypertable(
                            'target'::regclass,
                            'partition_column'::name, chunk_time_interval=>interval '8 hours',
                            create_default_indexes=> false);

-- enable compression
ALTER TABLE target SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'partition_column, value'
);

SELECT '2022-10-10 14:33:44.1234+05:30' as start_date \gset
INSERT INTO target (series_id, value, partition_column)
  SELECT s,1,t from generate_series(:'start_date'::timestamptz, :'start_date'::timestamptz + interval '1 day', '5m') t cross join
    generate_series(1,3, 1) s;

-- compress chunks
SELECT count(compress_chunk(ch)) FROM show_chunks('target') ch;

-- create a plain PG table with same data for result comparison
CREATE TABLE target_pg AS SELECT * FROM target;

CREATE TABLE source (
        time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        series_id BIGINT NOT NULL
    );
SELECT table_name FROM create_hypertable(
                                'source'::regclass,
                                'time'::name, chunk_time_interval=>interval '6 hours',
                                create_default_indexes=> false);

-- Use a small source that won't cause "cannot affect row a second time"
-- by matching on partition_column (unique per series_id in target)
INSERT INTO source (time, series_id, value)
  SELECT t, s, 1 from generate_series('2022-10-10 14:33:44.1234+05:30'::timestamptz,
    '2022-10-10 14:33:44.1234+05:30'::timestamptz + interval '2 hours', '5m') t
    cross join generate_series(1, 2, 1) s;

-- total compressed chunks before any MERGE
SELECT count(*) AS "total compressed_chunks", is_compressed FROM timescaledb_information.chunks WHERE
    hypertable_name = 'target' GROUP BY is_compressed ORDER BY is_compressed;

-- Merge UPDATE on compressed hypertables should work
MERGE INTO target t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            UPDATE SET value = t.value + 1;

-- Apply same UPDATE on PG table for comparison
MERGE INTO target_pg t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            UPDATE SET value = t.value + 1;

SELECT CASE WHEN EXISTS (TABLE target EXCEPT TABLE target_pg)
              OR EXISTS (TABLE target_pg EXCEPT TABLE target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress for next test
SELECT count(compress_chunk(ch, true)) FROM show_chunks('target') ch;

-- Merge DELETE on compressed hypertables should work
MERGE INTO target t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            DELETE;

MERGE INTO target_pg t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            DELETE;

SELECT CASE WHEN EXISTS (TABLE target EXCEPT TABLE target_pg)
              OR EXISTS (TABLE target_pg EXCEPT TABLE target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress for next test
SELECT count(compress_chunk(ch, true)) FROM show_chunks('target') ch;

-- Merge UPDATE/INSERT on compressed hypertables should work
MERGE INTO target t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            UPDATE SET value = t.value + 1
            WHEN NOT MATCHED THEN
            INSERT VALUES ('2021-11-01 00:00:05'::timestamp with time zone, 5, 210, '2021-11-01 00:00:05'::timestamp with time zone);

MERGE INTO target_pg t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            UPDATE SET value = t.value + 1
            WHEN NOT MATCHED THEN
            INSERT VALUES ('2021-11-01 00:00:05'::timestamp with time zone, 5, 210, '2021-11-01 00:00:05'::timestamp with time zone);

SELECT CASE WHEN EXISTS (TABLE target EXCEPT TABLE target_pg)
              OR EXISTS (TABLE target_pg EXCEPT TABLE target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress for next test
SELECT count(compress_chunk(ch, true)) FROM show_chunks('target') ch;

-- Merge DELETE/INSERT on compressed hypertables should work
MERGE INTO target t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            DELETE
            WHEN NOT MATCHED THEN
            INSERT VALUES ('2021-11-01 00:00:05'::timestamp with time zone, 5, 210, '2021-11-01 00:00:05'::timestamp with time zone);

MERGE INTO target_pg t
            USING source s
            ON t.partition_column = s.time AND t.series_id = s.series_id
            WHEN MATCHED THEN
            DELETE
            WHEN NOT MATCHED THEN
            INSERT VALUES ('2021-11-01 00:00:05'::timestamp with time zone, 5, 210, '2021-11-01 00:00:05'::timestamp with time zone);

SELECT CASE WHEN EXISTS (TABLE target EXCEPT TABLE target_pg)
              OR EXISTS (TABLE target_pg EXCEPT TABLE target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- Merge INSERT on compressed hypertables should still work
SELECT count(compress_chunk(ch, true)) FROM show_chunks('target') ch;

SELECT count(*) AS "total compressed_chunks", is_compressed FROM timescaledb_information.chunks WHERE
    hypertable_name = 'target' GROUP BY is_compressed ORDER BY is_compressed;

MERGE INTO target t
            USING source s
            ON t.partition_column = s.time AND t.value = s.value
            WHEN NOT MATCHED THEN
            INSERT VALUES ('2021-11-01 00:00:05'::timestamp with time zone, 5, 210, '2021-11-01 00:00:05'::timestamp with time zone);

-- you should notice 1 uncompressed chunk
SELECT count(*) AS "total compressed_chunks", is_compressed FROM timescaledb_information.chunks WHERE
    hypertable_name = 'target' GROUP BY is_compressed ORDER BY is_compressed;

DROP TABLE target;
DROP TABLE target_pg;
DROP TABLE source;

-- Test MERGE DELETE on table with filler columns (different attribute numbering)
CREATE TABLE target2(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int);
SELECT create_hypertable('target2', 'time');
ALTER TABLE target2 SET (timescaledb.compress, timescaledb.compress_segmentby='device_id', timescaledb.compress_orderby='time');
INSERT INTO target2 SELECT 1,2,3, t, d, 0 FROM generate_series('2000-01-01'::timestamptz, '2000-01-02', '1h') t cross join generate_series(1,3) d;
SELECT compress_chunk(ch) FROM show_chunks('target2') ch;

CREATE TABLE source2(time timestamptz NOT NULL, device_id int);
INSERT INTO source2 SELECT t, d FROM generate_series('2000-01-01'::timestamptz, '2000-01-01 12:00:00', '1h') t cross join generate_series(1,2) d;

CREATE TABLE target2_pg AS SELECT * FROM target2;

SELECT count(*) FROM target2;

-- MERGE DELETE with filler columns
MERGE INTO target2 t USING source2 s ON t.time = s.time AND t.device_id = s.device_id WHEN MATCHED THEN DELETE;
MERGE INTO target2_pg t USING source2 s ON t.time = s.time AND t.device_id = s.device_id WHEN MATCHED THEN DELETE;

SELECT CASE WHEN EXISTS (TABLE target2 EXCEPT TABLE target2_pg)
              OR EXISTS (TABLE target2_pg EXCEPT TABLE target2)
            THEN 'different'
            ELSE 'same'
       END AS result;

DROP TABLE target2;
DROP TABLE target2_pg;
DROP TABLE source2;

-- ============================================================
-- Additional correctness tests for MERGE on compressed hypertables
-- ============================================================

CREATE TABLE merge_target (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    temp DOUBLE PRECISION,
    label TEXT
);

SELECT create_hypertable('merge_target', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE merge_target SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time'
);

INSERT INTO merge_target
  SELECT t, d, d * 10.0 + extract(hour from t), 'orig'
  FROM generate_series('2000-01-01'::timestamptz, '2000-01-03 23:00', '1h') t
  CROSS JOIN generate_series(1, 3) d;

SELECT count(compress_chunk(ch)) FROM show_chunks('merge_target') ch;

CREATE TABLE merge_target_pg AS SELECT * FROM merge_target;

CREATE TABLE merge_source (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    temp DOUBLE PRECISION,
    label TEXT
);

-- Source covers only part of the target's time range and some devices
INSERT INTO merge_source
  SELECT t, d, d * 100.0, 'new'
  FROM generate_series('2000-01-01'::timestamptz, '2000-01-02 12:00', '1h') t
  CROSS JOIN generate_series(2, 4) d;

-- -----------------------------------------------------------
-- Test 1: Conditional WHEN MATCHED AND ... UPDATE
-- Only update rows where temp > 20
-- -----------------------------------------------------------
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED AND t.temp > 20 THEN
    UPDATE SET temp = s.temp, label = 'cond_updated'
  WHEN MATCHED THEN
    DO NOTHING;

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED AND t.temp > 20 THEN
    UPDATE SET temp = s.temp, label = 'cond_updated'
  WHEN MATCHED THEN
    DO NOTHING;

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 2: DO NOTHING for matched, INSERT for not matched
-- -----------------------------------------------------------
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    DO NOTHING
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'inserted');

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    DO NOTHING
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'inserted');

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 3: Multiple sequential MERGEs without recompression
-- (tests that partially compressed chunks work correctly)
-- -----------------------------------------------------------
-- First MERGE: update some rows
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = t.temp + 1;

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = t.temp + 1;

-- Second MERGE without recompression: delete some rows
MERGE INTO merge_target t
  USING (SELECT * FROM merge_source WHERE device_id = 2) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    DELETE;

MERGE INTO merge_target_pg t
  USING (SELECT * FROM merge_source WHERE device_id = 2) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    DELETE;

-- Third MERGE without recompression: update again
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET label = 'triple_merge';

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET label = 'triple_merge';

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 4: MERGE with subquery as source
-- -----------------------------------------------------------
MERGE INTO merge_target t
  USING (SELECT time, device_id, temp * 2 as temp, label
         FROM merge_source
         WHERE device_id = 3 AND time < '2000-01-02'::timestamptz) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'sub_insert');

MERGE INTO merge_target_pg t
  USING (SELECT time, device_id, temp * 2 as temp, label
         FROM merge_source
         WHERE device_id = 3 AND time < '2000-01-02'::timestamptz) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'sub_insert');

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 5: MERGE UPDATE on orderby column (time)
-- Updating the column used in compress_orderby
-- -----------------------------------------------------------
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET time = t.time + interval '30 seconds';

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET time = t.time + interval '30 seconds';

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 6: MERGE with all three actions: UPDATE + DELETE + INSERT
-- -----------------------------------------------------------
MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED AND t.temp > 500 THEN
    DELETE
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp + t.temp
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'all_three');

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED AND t.temp > 500 THEN
    DELETE
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp + t.temp
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'all_three');

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 7: MERGE with source matching across all chunks
-- (ensures multi-chunk decompression works)
-- -----------------------------------------------------------
CREATE TABLE merge_source_large (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    temp DOUBLE PRECISION
);

INSERT INTO merge_source_large
  SELECT t, d, -1.0
  FROM generate_series('2000-01-01'::timestamptz, '2000-01-03 23:00', '3h') t
  CROSS JOIN generate_series(1, 3) d;

MERGE INTO merge_target t
  USING merge_source_large s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp;

MERGE INTO merge_target_pg t
  USING merge_source_large s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp;

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 8: MERGE with NULL values in non-key columns
-- -----------------------------------------------------------
CREATE TABLE merge_source_nulls (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    temp DOUBLE PRECISION,
    label TEXT
);

INSERT INTO merge_source_nulls VALUES
  ('2000-01-01 00:00'::timestamptz, 1, NULL, NULL),
  ('2000-01-01 01:00'::timestamptz, 2, NULL, 'has_label'),
  ('2000-01-01 02:00'::timestamptz, 3, 999.9, NULL);

MERGE INTO merge_target t
  USING merge_source_nulls s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp, label = s.label;

MERGE INTO merge_target_pg t
  USING merge_source_nulls s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp, label = s.label;

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 9: MERGE on partially compressed table
-- (some chunks compressed, some not)
-- -----------------------------------------------------------
-- decompress one chunk to create mixed state
SELECT decompress_chunk(ch) FROM show_chunks('merge_target') ch LIMIT 1;

MERGE INTO merge_target t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp * 3
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'partial');

MERGE INTO merge_target_pg t
  USING merge_source s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET temp = s.temp * 3
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'partial');

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- recompress
SELECT count(compress_chunk(ch, true)) FROM show_chunks('merge_target') ch;

-- -----------------------------------------------------------
-- Test 10: MERGE with empty source (no rows should be affected)
-- -----------------------------------------------------------
SELECT count(*) AS before_count FROM merge_target;

MERGE INTO merge_target t
  USING (SELECT * FROM merge_source WHERE false) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    DELETE
  WHEN NOT MATCHED THEN
    INSERT (time, device_id, temp, label)
    VALUES (s.time, s.device_id, s.temp, 'empty');

SELECT count(*) AS after_count FROM merge_target;

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- -----------------------------------------------------------
-- Test 11: MERGE with CTE as source
-- -----------------------------------------------------------
WITH ranked_source AS (
  SELECT time, device_id, temp,
         row_number() OVER (PARTITION BY device_id ORDER BY time) as rn
  FROM merge_source
)
MERGE INTO merge_target t
  USING (SELECT * FROM ranked_source WHERE rn <= 5) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET label = 'cte_updated';

WITH ranked_source AS (
  SELECT time, device_id, temp,
         row_number() OVER (PARTITION BY device_id ORDER BY time) as rn
  FROM merge_source
)
MERGE INTO merge_target_pg t
  USING (SELECT * FROM ranked_source WHERE rn <= 5) s
  ON t.time = s.time AND t.device_id = s.device_id
  WHEN MATCHED THEN
    UPDATE SET label = 'cte_updated';

SELECT CASE WHEN EXISTS (TABLE merge_target EXCEPT TABLE merge_target_pg)
              OR EXISTS (TABLE merge_target_pg EXCEPT TABLE merge_target)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- cleanup
DROP TABLE merge_target;
DROP TABLE merge_target_pg;
DROP TABLE merge_source;
DROP TABLE merge_source_large;
DROP TABLE merge_source_nulls;
