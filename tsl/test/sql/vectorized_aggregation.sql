-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set EXPLAIN 'EXPLAIN (VERBOSE, COSTS OFF)'

CREATE TABLE testtable (
time timestamptz NOT NULL,
segment_by_value integer,
int_value integer,
float_value double precision);

SELECT FROM create_hypertable(relation=>'testtable', time_column_name=> 'time');

ALTER TABLE testtable SET (timescaledb.compress, timescaledb.compress_segmentby='segment_by_value');

INSERT INTO testtable
SELECT time AS time,
value AS segment_by_value,
value AS int_value,
value AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(-10, 100, 1) AS g2(value)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value), sum(int_value), sum(float_value) FROM testtable;

---
-- Tests with some chunks compressed
---
SELECT compress_chunk(ch) FROM show_chunks('testtable') ch LIMIT 3;

-- Vectorized aggregation possible
SELECT sum(segment_by_value) FROM testtable;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

-- Vectorization possible - filter on segment_by
:EXPLAIN
SELECT sum(segment_by_value) FROM testtable WHERE segment_by_value > 0;

-- Vectorization not possible due to a used filter
:EXPLAIN
SELECT sum(segment_by_value) FROM testtable WHERE segment_by_value > 0 AND int_value > 0;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable WHERE int_value > 0;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable WHERE float_value > 0;

-- Vectorization not possible due grouping
:EXPLAIN
SELECT sum(segment_by_value) FROM testtable GROUP BY float_value;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable GROUP BY int_value;

-- FIXME here the partitionwise aggregation plan is not chosen due to costs?
:EXPLAIN
SELECT sum(int_value) FROM testtable GROUP BY segment_by_value;

-- Vectorization possible with grouping by a segmentby column.
:EXPLAIN
SELECT sum(segment_by_value), segment_by_value FROM testtable GROUP BY segment_by_value ORDER BY 1, 2;

SELECT sum(segment_by_value), segment_by_value FROM testtable GROUP BY segment_by_value ORDER BY 1, 2;

:EXPLAIN
SELECT segment_by_value, sum(segment_by_value) FROM testtable GROUP BY segment_by_value ORDER BY 1, 2;

SELECT segment_by_value, sum(segment_by_value) FROM testtable GROUP BY segment_by_value ORDER BY 1, 2;

-- Vectorized aggregation possible
SELECT sum(int_value) FROM testtable;

:EXPLAIN
SELECT sum(int_value) FROM testtable;

-- Vectorized aggregation not possible
SELECT sum(float_value) FROM testtable;

:EXPLAIN
SELECT sum(float_value) FROM testtable;

---
-- Tests with all chunks compressed
---
SELECT compress_chunk(ch, if_not_compressed => true) FROM show_chunks('testtable') ch;

-- Vectorized aggregation possible
SELECT sum(segment_by_value) FROM testtable;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

-- Vectorized aggregation possible
SELECT sum(int_value) FROM testtable;

:EXPLAIN
SELECT sum(int_value) FROM testtable;

---
-- Tests with some chunks are partially compressed
---
INSERT INTO testtable (time, segment_by_value, int_value, float_value)
   VALUES ('1980-01-02 01:00:00-00', 0, 0, 0);

-- Vectorized aggregation possible
SELECT sum(segment_by_value) FROM testtable;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

-- Vectorized aggregation possible
SELECT sum(int_value) FROM testtable;

:EXPLAIN
SELECT sum(int_value) FROM testtable;


--Vectorized aggregation not possible for expression
SELECT sum(abs(int_value)) FROM testtable;

:EXPLAIN
SELECT sum(abs(int_value)) FROM testtable;


-- Vectorized aggregation NOT possible
SET timescaledb.enable_vectorized_aggregation = OFF;

:EXPLAIN
SELECT sum(int_value) FROM testtable;

RESET timescaledb.enable_vectorized_aggregation;

-- Vectorized aggregation without bulk decompression only possible for
-- segmentby columns.
SET timescaledb.enable_bulk_decompression = OFF;

:EXPLAIN
SELECT sum(int_value) FROM testtable;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

SELECT sum(segment_by_value) FROM testtable;

RESET timescaledb.enable_bulk_decompression;

-- Using the same sum function multiple times is supported by vectorization
:EXPLAIN
SELECT sum(int_value), sum(int_value) FROM testtable;

-- Using the same sum function multiple times is supported by vectorization
:EXPLAIN
SELECT sum(segment_by_value), sum(segment_by_value) FROM testtable;

-- Performing a sum on multiple columns is supported.
:EXPLAIN
SELECT sum(int_value), sum(segment_by_value) FROM testtable;

SELECT sum(int_value), sum(segment_by_value) FROM testtable;

-- Using the sum function together with another non-vector capable aggregate is not supported
:EXPLAIN
SELECT sum(int_value), max(int_value) FROM testtable;

-- Using the sum function together with another non-vector capable aggregate is not supported
:EXPLAIN
SELECT sum(segment_by_value), max(segment_by_value) FROM testtable;

---
-- Tests with only negative values
---
TRUNCATE testtable;

INSERT INTO testtable
SELECT time AS time,
value AS segment_by_value,
value AS int_value,
value AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(-10, 0, 1) AS g2(value)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value), sum(int_value), sum(float_value) FROM testtable;

SELECT compress_chunk(ch) FROM show_chunks('testtable') ch;

-- Aggregation with vectorization
SELECT sum(segment_by_value) FROM testtable;
SELECT sum(int_value) FROM testtable;

---
-- Tests with only positive values
---
TRUNCATE testtable;

INSERT INTO testtable
SELECT time AS time,
value AS segment_by_value,
value AS int_value,
value AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(0, 10, 1) AS g2(value)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value), sum(int_value), sum(float_value) FROM testtable;

SELECT compress_chunk(ch) FROM show_chunks('testtable') ch;

-- Aggregation with vectorization
SELECT sum(segment_by_value) FROM testtable;
SELECT sum(int_value) FROM testtable;

-- Vectorization possible - filter on segment_by
:EXPLAIN
SELECT sum(int_value) FROM testtable WHERE segment_by_value > 5;

SELECT sum(int_value) FROM testtable WHERE segment_by_value > 5;

SET timescaledb.enable_vectorized_aggregation = OFF;
SELECT sum(int_value) FROM testtable WHERE segment_by_value > 5;
RESET timescaledb.enable_vectorized_aggregation;

SELECT sum(int_value) FROM testtable WHERE segment_by_value > 10;

SET timescaledb.enable_vectorized_aggregation = OFF;
SELECT sum(int_value) FROM testtable WHERE segment_by_value > 10;
RESET timescaledb.enable_vectorized_aggregation;

---
-- Tests with parallel plans
---
SET parallel_leader_participation = off;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

SELECT sum(segment_by_value) FROM testtable;

RESET parallel_leader_participation;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

---
-- Tests with only zero values
---
TRUNCATE testtable;

INSERT INTO testtable
SELECT time AS time,
0 AS segment_by_value,
0 AS int_value,
0 AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value), sum(int_value), sum(float_value) FROM testtable;

SELECT compress_chunk(ch) FROM show_chunks('testtable') ch;

-- Aggregation with vectorization
SELECT sum(segment_by_value) FROM testtable;
SELECT sum(int_value) FROM testtable;

---
-- Tests with null values
---
TRUNCATE testtable;

INSERT INTO testtable
SELECT time AS time,
value AS segment_by_value,
value AS int_value,
value AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(0, 10, 1) AS g2(value)
ORDER BY time;

-- NULL values for compressed data
INSERT INTO testtable
SELECT time AS time,
value AS segment_by_value,
NULL AS int_value,
NULL AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(0, 5, 1) AS g2(value)
ORDER BY time;

-- NULL values for segment_by
INSERT INTO testtable
SELECT time AS time,
NULL AS segment_by_value,
value AS int_value,
value AS float_value
FROM
generate_series('1980-01-01 00:00:00-00', '1980-03-01 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(0, 2, 1) AS g2(value)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value), sum(int_value), sum(float_value) FROM testtable;

SELECT compress_chunk(ch) FROM show_chunks('testtable') ch;

-- Aggregation with vectorization
:EXPLAIN
SELECT sum(segment_by_value) FROM testtable;

:EXPLAIN
SELECT sum(int_value) FROM testtable;

SELECT sum(segment_by_value) FROM testtable;

SELECT sum(int_value) FROM testtable;

-- Aggregation filters are not supported at the moment
:EXPLAIN
SELECT sum(segment_by_value) FILTER (WHERE segment_by_value > 99999) FROM testtable;

SET timescaledb.enable_vectorized_aggregation = OFF;
SELECT sum(segment_by_value) FILTER (WHERE segment_by_value > 99999) FROM testtable;
RESET timescaledb.enable_vectorized_aggregation;

SELECT sum(segment_by_value) FILTER (WHERE segment_by_value > 99999) FROM testtable;

---
-- Tests with multiple segment by values
---
CREATE TABLE testtable2 (
time timestamptz NOT NULL,
segment_by_value1 integer NOT NULL,
segment_by_value2 integer NOT NULL,
int_value integer NOT NULL,
float_value double precision NOT NULL);

SELECT FROM create_hypertable(relation=>'testtable2', time_column_name=> 'time');

ALTER TABLE testtable2 SET (timescaledb.compress, timescaledb.compress_segmentby='segment_by_value1, segment_by_value2');

INSERT INTO testtable2
SELECT time AS time,
value1 AS segment_by_value1,
value2 AS segment_by_value2,
value1 AS int_value,
value1 AS float_value
FROM
generate_series('1980-01-03 00:00:00-00', '1980-03-04 00:00:00-00', INTERVAL '1 day') AS g1(time),
generate_series(-10, 25, 1) AS g2(value1),
generate_series(-30, 20, 1) AS g3(value2)
ORDER BY time;

-- Aggregation result without any vectorization
SELECT sum(segment_by_value1), sum(segment_by_value2) FROM testtable2;

SELECT compress_chunk(ch) FROM show_chunks('testtable2') ch;

ANALYZE testtable2;

:EXPLAIN
SELECT sum(segment_by_value1) FROM testtable2;

SELECT sum(segment_by_value1) FROM testtable2;

:EXPLAIN
SELECT sum(segment_by_value2) FROM testtable2;

SELECT sum(segment_by_value2) FROM testtable2;


-- Vectorization possible - filter on segment_by
:EXPLAIN
SELECT sum(segment_by_value1) FROM testtable2 WHERE segment_by_value1 > 0;

:EXPLAIN
SELECT sum(segment_by_value1) FROM testtable2 WHERE segment_by_value1 > 0 AND segment_by_value2 > 0;

:EXPLAIN
SELECT sum(segment_by_value1) FROM testtable2 WHERE segment_by_value1 > 0 AND segment_by_value2 > 0 AND 2>1;

-- Vectorization not possible filter on segment_by and compressed value

-- Disable parallel worker to get deterministic query plans on i386
SET max_parallel_workers_per_gather = 0;

:EXPLAIN
SELECT sum(segment_by_value1) FROM testtable2 WHERE segment_by_value1 > 1000 AND int_value > 1000;

RESET max_parallel_workers_per_gather;
