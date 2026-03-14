-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- create source table
CREATE TABLE source (
         filler_1 int,
         filler_2 int,
         filler_3 int,
         time timestamptz NOT NULL,
         device_id int
  );

INSERT INTO source (time, device_id, filler_2, filler_3, filler_1)
  SELECT time,
    device_id,
    device_id + 134,
    device_id + 209,
    device_id + 0.50127
FROM generate_series('2000-01-01 0:00:00+0'::timestamptz, '2000-01-05 23:55:00+0', '20m') gtime (time),
    generate_series(1, 5, 1) gdevice (device_id);

-- create corresponding PG tables to compare against hypertables
CREATE table metrics_pg as SELECT * FROM metrics;
CREATE table metrics_space_pg as SELECT * FROM metrics_space;
CREATE table metrics_compressed_pg as SELECT * FROM metrics_compressed;
CREATE table metrics_space_compressed_pg as SELECT * FROM  metrics_space_compressed;

-- MERGE UDPATE matched rows for normal PG tables
MERGE INTO metrics_pg t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_pg);

-- MERGE UDPATE matched rows for hypertable
MERGE INTO metrics t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_pg);

SELECT CASE WHEN EXISTS (TABLE metrics EXCEPT TABLE metrics_pg)
              OR EXISTS (TABLE metrics_pg EXCEPT TABLE metrics)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE DELETE matched rows for normal PG tables
MERGE INTO metrics_pg t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

-- MERGE DELETE matched rows for hypertable
MERGE INTO metrics t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

SELECT CASE WHEN EXISTS (TABLE metrics EXCEPT TABLE metrics_pg)
              OR EXISTS (TABLE metrics_pg EXCEPT TABLE metrics)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE INSERT/DELETE matched rows for normal PG tables
MERGE INTO metrics_pg t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN MATCHED THEN DELETE
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- MERGE INSERT/DELETE matched rows for hypertable
MERGE INTO metrics t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN MATCHED THEN DELETE
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- result should be 'same'
SELECT CASE WHEN EXISTS (TABLE metrics EXCEPT TABLE metrics_pg)
              OR EXISTS (TABLE metrics_pg EXCEPT TABLE metrics)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE INSERT/DELETE matched rows for normal PG tables
MERGE INTO metrics_pg t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN MATCHED THEN DELETE
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- MERGE INSERT/DELETE matched rows for hypertable
MERGE INTO metrics t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN MATCHED THEN DELETE
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- result should be 'same'
SELECT CASE WHEN EXISTS (TABLE metrics EXCEPT TABLE metrics_pg)
              OR EXISTS (TABLE metrics_pg EXCEPT TABLE metrics)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE UDPATE matched rows for normal PG tables
MERGE INTO metrics_space_pg t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_pg);

-- MERGE UDPATE matched rows for space partitioned hypertable
MERGE INTO metrics_space t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_pg);

-- result should be 'same'
SELECT CASE WHEN EXISTS (TABLE metrics_space EXCEPT TABLE metrics_space_pg)
              OR EXISTS (TABLE metrics_space_pg EXCEPT TABLE metrics_space)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE DELETE matched rows for normal PG tables
MERGE INTO metrics_space_pg t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

-- MERGE DELETE matched rows for space partitioned hypertable
MERGE INTO metrics_space t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

-- result should be 'same'
SELECT CASE WHEN EXISTS (TABLE metrics_space EXCEPT TABLE metrics_space_pg)
              OR EXISTS (TABLE metrics_space_pg EXCEPT TABLE metrics_space)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE INSERT matched rows for normal PG tables
MERGE INTO metrics_space_pg t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- MERGE INSERT matched rows for space partitioned hypertable
MERGE INTO metrics_space t
              USING source s
              ON t.time = s.time AND t.device_id = s.device_id
              WHEN NOT MATCHED THEN
              INSERT (time, device_id, v0, v1, v2, v3) VALUES
                     (s.time, s.device_id, 1,2,3,4);

-- result should be 'same'
SELECT CASE WHEN EXISTS (TABLE metrics_space EXCEPT TABLE metrics_space_pg)
              OR EXISTS (TABLE metrics_space_pg EXCEPT TABLE metrics_space)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE UPDATE matched rows for compressed hypertable
MERGE INTO metrics_compressed t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_compressed_pg);

MERGE INTO metrics_compressed_pg t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_compressed_pg);

SELECT CASE WHEN EXISTS (TABLE metrics_compressed EXCEPT TABLE metrics_compressed_pg)
              OR EXISTS (TABLE metrics_compressed_pg EXCEPT TABLE metrics_compressed)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE DELETE matched rows for compressed hypertable
MERGE INTO metrics_compressed t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

MERGE INTO metrics_compressed_pg t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

SELECT CASE WHEN EXISTS (TABLE metrics_compressed EXCEPT TABLE metrics_compressed_pg)
              OR EXISTS (TABLE metrics_compressed_pg EXCEPT TABLE metrics_compressed)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE UPDATE matched rows for space partitioned compressed hypertable
MERGE INTO metrics_space_compressed t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_space_compressed_pg);

MERGE INTO metrics_space_compressed_pg t
USING source s
ON t.time = s.time AND t.device_id = s.device_id
WHEN MATCHED THEN
UPDATE SET v1 = s.filler_1 * 1.23, v2 = (SELECT DISTINCT count(time) from metrics_space_compressed_pg);

SELECT CASE WHEN EXISTS (TABLE metrics_space_compressed EXCEPT TABLE metrics_space_compressed_pg)
              OR EXISTS (TABLE metrics_space_compressed_pg EXCEPT TABLE metrics_space_compressed)
            THEN 'different'
            ELSE 'same'
       END AS result;

-- MERGE DELETE matched rows for space partitioned compressed hypertable
MERGE INTO metrics_space_compressed t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

MERGE INTO metrics_space_compressed_pg t
       USING source s
       ON t.time = s.time AND t.device_id = s.device_id
       WHEN MATCHED THEN
       DELETE;

SELECT CASE WHEN EXISTS (TABLE metrics_space_compressed EXCEPT TABLE metrics_space_compressed_pg)
              OR EXISTS (TABLE metrics_space_compressed_pg EXCEPT TABLE metrics_space_compressed)
            THEN 'different'
            ELSE 'same'
       END AS result;
