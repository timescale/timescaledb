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
