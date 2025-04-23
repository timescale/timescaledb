-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Increase the working memory limit slightly, otherwise the batch sorted merge
-- will be penalized for segmentby cardinalities larger than 100, where it is
-- still faster than sort.
SET work_mem to '16MB';

\set PREFIX 'EXPLAIN (analyze, verbose, costs off, timing off, summary off)'

CREATE TABLE test1 (
time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer,
    x4 integer,
    x5 integer);

SELECT FROM create_hypertable('test1', 'time');

ALTER TABLE test1 SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2, x5', timescaledb.compress_orderby = 'time DESC, x3 ASC, x4 ASC');

INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0);

SELECT compress_chunk(i) FROM show_chunks('test1') i;
ANALYZE test1;

CREATE TABLE test2 (
time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer,
    x4 integer,
    x5 integer);

SELECT FROM create_hypertable('test2', 'time');

ALTER TABLE test2 SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2, x5', timescaledb.compress_orderby = 'time ASC, x3 DESC, x4 DESC');

INSERT INTO test2 (time, x1, x2, x3, x4, x5) values('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0);
INSERT INTO test2 (time, x1, x2, x3, x4, x5) values('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0);
INSERT INTO test2 (time, x1, x2, x3, x4, x5) values('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0);
INSERT INTO test2 (time, x1, x2, x3, x4, x5) values('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0);

SELECT compress_chunk(i) FROM show_chunks('test2') i;
ANALYZE test2;

CREATE TABLE test_with_defined_null (
    time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer);

SELECT FROM create_hypertable('test_with_defined_null','time');

ALTER TABLE test_with_defined_null SET (timescaledb.compress,timescaledb.compress_segmentby='x1', timescaledb.compress_orderby='x2 ASC NULLS FIRST');

INSERT INTO test_with_defined_null (time, x1, x2) values('2000-01-01', '1', NULL);
INSERT INTO test_with_defined_null (time, x1, x2) values('2000-01-01','2', NULL);
INSERT INTO test_with_defined_null (time, x1, x2) values('2000-01-01','1',1);
INSERT INTO test_with_defined_null (time, x1, x2) values('2000-01-01','1',2);

SELECT compress_chunk(i) FROM show_chunks('test_with_defined_null') i;
ANALYZE test_with_defined_null;

-- test1 uses compress_segmentby='x1, x2, x5' and compress_orderby = 'time DESC, x3 ASC, x4 ASC'
-- test2 uses compress_segmentby='x1, x2, x5' and compress_orderby = 'time ASC, x3 DESC, x4 DESC'
-- test_with_defined_null uses compress_segmentby='x1' and compress_orderby = 'x2 ASC NULLS FIRST'

------
-- Tests based on ordering
------

-- Should be optimized (implicit NULLS first)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS LAST;

-- Should be optimized (implicit NULLS last)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 ASC NULLS LAST;

-- Should not be optimized (wrong order for x4)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 DESC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST, x3 DESC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST, x3 DESC NULLS FIRST, x4 DESC NULLS FIRST;

-- Should not be optimized (wrong order for x4 in backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST, x3 DESC NULLS LAST, x4 ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 DESC, x4 DESC;

-- Should not be optimized (wrong order for x3)
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 ASC NULLS LAST, x4 DESC;

-- Should not be optimized (wrong order for x3)
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 ASC NULLS FIRST, x4 DESC;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 NULLS LAST;

-- Should not be optimized (wrong order for x3 in backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS LAST, x3 DESC NULLS FIRST, x4 NULLS FIRST;

-- Should not be optimized (wrong order for x3 in backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS LAST, x3 DESC NULLS LAST, x4 NULLS FIRST;

-- Should be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS LAST;

-- Should not be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;

-- Should not be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;


------
-- Tests based on attributes
------

-- Should be optimized (some batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 0 ORDER BY time DESC;

-- Should be optimized (no batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x3;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4, x3, x4;

-- Should not be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x4, x3;

-- Should not be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time ASC, x3, x4;

-- Test that the enable_sort GUC doesn't disable the batch sorted merge plan.
SET enable_sort TO OFF;
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;
RESET enable_sort;

------
-- Tests based on results
------

-- Forward scan
SELECT * FROM test1 ORDER BY time DESC;

-- Backward scan
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST;

-- Forward scan
SELECT * FROM test2 ORDER BY time ASC;

-- Backward scan
SELECT * FROM test2 ORDER BY time DESC NULLS LAST;

-- With selection on compressed column (value larger as max value for all batches, so no batch has to be opened)
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- With selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT * FROM test1 WHERE x4 > 2 ORDER BY time DESC;

-- With selection on segment_by column
SELECT * FROM test1 WHERE time < '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;

-- With selection on segment_by and compressed column
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' AND x4 > 100 ORDER BY time DESC;

-- Without projection
SELECT * FROM test1 ORDER BY time DESC;

-- With projection on time
SELECT time FROM test1 ORDER BY time DESC;

-- With projection on x3
SELECT x3 FROM test1 ORDER BY time DESC;

-- With projection on x3 and time
SELECT x3,time FROM test1 ORDER BY time DESC;

-- With projection on time and x3
SELECT time,x3 FROM test1 ORDER BY time DESC;

-- Test with projection and constants
EXPLAIN (verbose) SELECT 1 as one, 2 as two, 3 as three, time, x2 FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, time, x2 FROM test1 ORDER BY time DESC;

-- Test with projection and constants
EXPLAIN (verbose) SELECT 1 as one, 2 as two, 3 as three, x2, time FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, x2, time FROM test1 ORDER BY time DESC;

-- With projection and selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT x4 FROM test1 WHERE x4 > 2 ORDER BY time DESC;

-- Aggregation with count
SELECT count(*) FROM test1;

-- Test with default values
ALTER TABLE test1 ADD COLUMN c1 int;
ALTER TABLE test1 ADD COLUMN c2 int NOT NULL DEFAULT 42;
SELECT * FROM test1 ORDER BY time DESC;

-- Recompress
SELECT decompress_chunk(i) FROM show_chunks('test1') i;
SELECT compress_chunk(i) FROM show_chunks('test1') i;
ANALYZE test1;

-- Test with a changed physical layout
-- build_physical_tlist() can not be used for the scan on the compressed chunk anymore
SELECT * FROM test1 ORDER BY time DESC;
ALTER TABLE test1 DROP COLUMN c2;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with a re-created column
ALTER TABLE test1 ADD COLUMN c2 int NOT NULL DEFAULT 43;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with the recreated column
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with projection and recreated column
:PREFIX
SELECT time, x2, x1, c2 FROM test1 ORDER BY time DESC;
SELECT time, x2, x1, c2 FROM test1 ORDER BY time DESC;

-- Test with projection and recreated column
:PREFIX
SELECT x2, x1, c2, time FROM test1 ORDER BY time DESC;
SELECT x2, x1, c2, time FROM test1 ORDER BY time DESC;

-- Test with projection, constants and recreated column
:PREFIX
SELECT 1 as one, 2 as two, 3 as three, x2, x1, c2, time FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, x2, x1, c2, time FROM test1 ORDER BY time DESC;

-- Test with null values
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS FIRST;
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS LAST;
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;

------
-- Tests based on compressed chunk state
------

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

BEGIN TRANSACTION;

INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 02:01:00-00', 10, 20, 30, 40, 50);

-- Should be optimized using a merge append path between the compressed and uncompressed part of the chunk
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- The inserted value should be visible
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

ROLLBACK;

------
-- Tests on a larger relation
------

CREATE TABLE sensor_data (
time timestamptz NOT NULL,
sensor_id integer NOT NULL,
cpu double precision NULL,
temperature double precision NULL);

SELECT FROM create_hypertable('sensor_data','time');

INSERT INTO sensor_data
SELECT
time + (INTERVAL '1 minute' * random()) AS time,
sensor_id,
random() AS cpu,
random() * 100 AS temperature
FROM
generate_series('1980-01-01 00:00:00-00', '1980-02-01 00:00:00-00', INTERVAL '10 minute') AS g1(time),
generate_series(1, 100, 1 ) AS g2(sensor_id)
ORDER BY
time;

ALTER TABLE sensor_data SET (timescaledb.compress, timescaledb.compress_segmentby='sensor_id', timescaledb.compress_orderby = 'time DESC');

SELECT add_compression_policy('sensor_data','1 minute'::INTERVAL);

SELECT compress_chunk(i) FROM show_chunks('sensor_data') i;

-- Ensure the optimization is used for queries on this table
:PREFIX
SELECT * FROM sensor_data ORDER BY time DESC LIMIT 1;

-- Verify that we produce the same order without and with the optimization
CREATE PROCEDURE order_test(query text) LANGUAGE plpgsql AS $$
        DECLARE
            count integer;
        BEGIN

        SET timescaledb.enable_decompression_sorted_merge = 0;
        EXECUTE format('CREATE TABLE temp_data1 AS %s;', query);
        ALTER TABLE temp_data1 ADD COLUMN new_id SERIAL PRIMARY KEY;

        SET timescaledb.enable_decompression_sorted_merge = 1;
        EXECUTE format('CREATE TABLE temp_data2 AS %s;', query);
        ALTER TABLE temp_data2 ADD COLUMN new_id SERIAL PRIMARY KEY;

        CREATE TEMP TABLE temp_data3 AS (
            SELECT * FROM temp_data1 UNION ALL SELECT * FROM temp_data2
        );

        count := (SELECT COUNT(*) FROM (SELECT COUNT(*) FROM temp_data3 GROUP BY time, new_id HAVING COUNT(*) != 2) AS s);

        IF count > 0 THEN
            RAISE EXCEPTION 'Detected different order with and without the optimization %', count;
        END IF;

        -- Drop old tables
        DROP TABLE temp_data1;
        DROP TABLE temp_data2;
        DROP TABLE temp_data3;

        END;
$$;

CALL order_test('SELECT * FROM sensor_data ORDER BY time DESC');
CALL order_test('SELECT * FROM sensor_data ORDER BY time DESC LIMIT 100');
CALL order_test('SELECT * FROM sensor_data ORDER BY time ASC NULLS FIRST');
CALL order_test('SELECT * FROM sensor_data ORDER BY time ASC NULLS FIRST LIMIT 100');

CALL order_test('SELECT * FROM test1 ORDER BY time DESC');
CALL order_test('SELECT * FROM test1 ORDER BY time ASC NULLS LAST');

------
-- Test window functions
------
CREATE TABLE insert_test(id INT);
INSERT INTO insert_test SELECT time_bucket_gapfill(1,time,1,5) FROM (VALUES (1),(2)) v(time) GROUP BY 1 ORDER BY 1;

SELECT * FROM insert_test AS ref_0
WHERE EXISTS (
    SELECT
        sum(ref_0.id) OVER (partition by ref_0.id ORDER BY ref_0.id,ref_0.id,sample_0.time)
    FROM
        sensor_data AS sample_0
    WHERE (1 > sample_0.temperature)
);

------
-- Test enabling and disabling the optimization based on costs
------

CREATE TABLE test_costs (
time timestamptz NOT NULL,
segment_by integer NOT NULL,
x1 integer NOT NULL);

SELECT FROM create_hypertable('test_costs', 'time');

ALTER TABLE test_costs SET (timescaledb.compress, timescaledb.compress_segmentby='segment_by', timescaledb.compress_orderby = 'time DESC, x1');

-- Create 100 segments
INSERT INTO test_costs
SELECT
'2000-01-01 02:01:00-00'::timestamptz AS time,
segment_by,
random() as x1
FROM
generate_series(1, 100, 1) AS g2(segment_by)
ORDER BY time;

SELECT add_compression_policy('test_costs','1 minute'::INTERVAL);

SELECT compress_chunk(i) FROM show_chunks('test_costs') i;
ANALYZE test_costs;

-- Number of segments
SELECT count(*) FROM (SELECT segment_by from test_costs group by segment_by) AS s;

-- Test query plan (should be optimized due to 100 different segments)
:PREFIX
SELECT time, segment_by, x1 FROM test_costs ORDER BY time DESC;

-- Decompress chunk
SELECT decompress_chunk(i) FROM show_chunks('test_costs') i;

-- Add 900 segments (1000 segments total)
INSERT INTO test_costs
SELECT
'2000-01-01 02:01:00-00'::timestamptz AS time,
segment_by,
random() as x1
FROM
generate_series(100, 1000, 1) AS g2(segment_by)
ORDER BY time;

-- Recompress chunk
SELECT compress_chunk(i) FROM show_chunks('test_costs') i;
ANALYZE test_costs;

-- Number of segments
SELECT count(*) FROM (SELECT segment_by from test_costs group by segment_by) AS s;

-- Test query plan (should not be optimized due to 1000 different segments)
:PREFIX
SELECT time, segment_by, x1 FROM test_costs ORDER BY time DESC;

-- Test query plan with predicate (query should be optimized due to ~100 segments)
:PREFIX
SELECT time, segment_by, x1 FROM test_costs WHERE segment_by > 900 and segment_by < 999 ORDER BY time DESC;

-- Target list creation - Issue 5738
CREATE TABLE bugtab(
 time timestamp without time zone,
 hin   character varying(128) NOT NULL,
 model character varying(128) NOT NULL,
 block character varying(128) NOT NULL,
 message_name character varying(128) NOT NULL,
 signal_name  character varying(128) NOT NULL,
 signal_numeric_value double precision,
 signal_string_value character varying(128)
);

SELECT create_hypertable('bugtab', 'time');

INSERT INTO bugtab values('2020-01-01 10:00', 'hin1111', 'model111', 'blok111', 'message_here', 'signal1', 12.34, '12.34');

ALTER TABLE bugtab SET (timescaledb.compress, timescaledb.compress_segmentby = 'hin, signal_name', timescaledb.compress_orderby = 'time');

SELECT chunk_schema || '.' || chunk_name AS "chunk_table_bugtab"
       FROM timescaledb_information.chunks
       WHERE hypertable_name = 'bugtab' ORDER BY range_start LIMIT 1 \gset

SELECT compress_chunk(i) FROM show_chunks('bugtab') i;

ANALYZE bugtab;

:PREFIX
SELECT "time","hin"::text,"model"::text,"block"::text,"message_name"::text,"signal_name"::text,"signal_numeric_value","signal_string_value"::text FROM :chunk_table_bugtab ORDER BY "time" DESC;

SELECT "time","hin"::text,"model"::text,"block"::text,"message_name"::text,"signal_name"::text,"signal_numeric_value","signal_string_value"::text FROM :chunk_table_bugtab ORDER BY "time" DESC;

SELECT "time","hin"::text,"model"::text,"block"::text,"message_name"::text,"signal_name"::text,"signal_numeric_value","signal_string_value"::text FROM bugtab ORDER BY "time" DESC;

-- Condition that filter the first tuple of a batch - Issue 5797
CREATE TABLE test (
    id      bigint,
    dttm    timestamp,
    otherId    int,
    valueFk int,
    otherFk int,
    measure double precision
);

SELECT create_hypertable('test', 'dttm');
ALTER TABLE test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'otherId,valueFk,otherFk'
);

INSERT INTO public.test (id, dttm, otherid, valuefk, otherfk, measure)
VALUES  (109288, '2023-05-25 23:12:13.000000', 130, 14499, 13, 0.13216569884001217),
        (109286, '2023-05-25 23:12:13.000000', 130, 14500, 13, 0.3740651978942786),
        (107617, '2023-05-25 14:24:12.000000', 130, 14850, 13, 0.5978259144311195),
        (103864, '2023-05-25 13:30:51.000000', 130, 16760, 13, 0.4733429856616205),
        (104977, '2023-05-25 12:11:47.000000', 133, 14843, 13, 0.24366893909655118),
        (108321, '2023-05-25 18:39:07.000000', 133, 15294, 13, 0.8768629101819378),
        (108320, '2023-05-25 16:09:17.000000', 133, 15294, 13, 0.6185638532799445),
        (104987, '2023-05-25 13:27:19.000000', 133, 15294, 13, 0.9830846939109854),
        (104737, '2023-05-25 19:59:54.000000', 135, 14238, 13, 0.2388520055224177),
        (106278, '2023-05-25 19:59:54.000000', 135, 14238, 13, 0.6305156586688518),
        (104741, '2023-05-25 19:59:54.000000', 135, 14238, 13, 0.4990673076480263),
        (106277, '2023-05-25 12:53:34.000000', 135, 14238, 13, 0.46086278330000496),
        (97409, '2023-05-25 12:38:48.000000', 137, 14533, 13, 0.8308173375978924),
        (105234, '2023-05-25 12:38:45.000000', 137, 14533, 13, 0.10860962941223917),
        (105233, '2023-05-25 12:06:35.000000', 137, 14533, 13, 0.09058791972962155),
        (97434, '2023-05-25 12:39:46.000000', 137, 14657, 13, 0.023315916140422388),
        (108167, '2023-05-25 15:41:30.000000', 137, 14964, 13, 0.21757999385617666),
        (107741, '2023-05-25 14:40:37.000000', 137, 14964, 13, 0.3449447147508202),
        (106312, '2023-05-25 14:40:16.000000', 137, 14964, 13, 0.11890456868959376),
        (106134, '2023-05-25 12:56:18.000000', 137, 14964, 13, 0.8004332371337775),
        (103696, '2023-05-25 12:54:31.000000', 137, 14964, 13, 0.30147495793613643),
        (106311, '2023-05-25 12:44:22.000000', 137, 14964, 13, 0.7412968055185551),
        (106133, '2023-05-25 12:44:22.000000', 137, 14964, 13, 0.12940337622720932),
        (105711, '2023-05-25 12:43:57.000000', 137, 14964, 13, 0.1044849979830822),
        (105710, '2023-05-25 12:34:04.000000', 137, 14964, 13, 0.9113563410974876),
        (108787, '2023-05-25 17:59:35.000000', 137, 15377, 13, 0.921829256160489),
        (107833, '2023-05-25 14:53:08.000000', 137, 16302, 13, 0.9663117845438407),
        (105435, '2023-05-25 12:30:59.000000', 137, 16568, 13, 0.13774896612028797),
        (105434, '2023-05-25 12:29:29.000000', 137, 16568, 13, 0.3891495411502035),
        (108357, '2023-05-25 16:18:39.000000', 137, 16665, 13, 0.44701901843246716),
        (98564, '2023-05-25 17:12:43.000000', 138, 14760, 13, 0.8463114782142114),
        (109032, '2023-05-25 19:00:00.000000', 138, 14992, 13, 0.025578609447126865),
        (108800, '2023-05-25 18:43:18.000000', 138, 14992, 13, 0.5397724043221928),
        (108799, '2023-05-25 18:00:00.000000', 138, 14992, 13, 0.0321658507434357),
        (107320, '2023-05-25 14:00:01.000000', 138, 14992, 13, 0.9042941365487067),
        (107296, '2023-05-25 14:00:00.000000', 138, 14992, 13, 0.7821178685669885),
        (104700, '2023-05-25 12:36:55.000000', 138, 14992, 13, 0.6854496458178119),
        (105177, '2023-05-25 12:00:01.000000', 138, 14992, 13, 0.23780719110724746),
        (109330, '2023-05-25 23:59:13.000000', 138, 15080, 13, 0.5409015970284159),
        (107400, '2023-05-25 16:45:13.000000', 138, 15080, 13, 0.6233594483468217),
        (107399, '2023-05-25 14:03:49.000000', 138, 15080, 13, 0.8192327792045404),
        (105004, '2023-05-25 13:37:49.000000', 138, 15080, 13, 0.2993620446103442),
        (102592, '2023-05-25 13:31:48.000000', 138, 15080, 13, 0.24649704579496046),
        (109028, '2023-05-25 19:00:00.000000', 138, 15123, 13, 0.5442767942906279),
        (108794, '2023-05-25 18:43:18.000000', 138, 15123, 13, 0.29095714680616425),
        (108793, '2023-05-25 18:00:00.000000', 138, 15123, 13, 0.681894893772391),
        (107314, '2023-05-25 14:00:01.000000', 138, 15123, 13, 0.9637603904838059),
        (107292, '2023-05-25 14:00:00.000000', 138, 15123, 13, 0.05956707862994293),
        (104696, '2023-05-25 12:36:55.000000', 138, 15123, 13, 0.27039489547807705),
        (105171, '2023-05-25 12:00:01.000000', 138, 15123, 13, 0.1269705046788907),
        (106625, '2023-05-25 13:22:19.000000', 138, 15326, 13, 0.7712280764026431),
        (106624, '2023-05-25 13:15:49.000000', 138, 15326, 13, 0.585381418741779),
        (105699, '2023-05-25 12:44:33.000000', 138, 15326, 13, 0.3710994669938259),
        (105698, '2023-05-25 12:33:41.000000', 138, 15326, 13, 0.8992328857980105),
        (108514, '2023-05-25 16:47:37.000000', 138, 15620, 13, 0.40346934167556725),
        (102691, '2023-05-25 13:33:57.000000', 138, 15620, 13, 0.8046719989908304),
        (103655, '2023-05-25 13:34:39.000000', 138, 15740, 13, 0.2541099322817928),
        (106987, '2023-05-25 13:37:36.000000', 138, 15766, 13, 0.8407818724583045),
        (102180, '2023-05-25 13:37:11.000000', 138, 15766, 13, 0.19149633299917213),
        (102717, '2023-05-25 13:38:17.000000', 138, 15868, 13, 0.03196157886032225),
        (102719, '2023-05-25 13:38:42.000000', 138, 15921, 13, 0.9986438564169191),
        (103659, '2023-05-25 13:35:11.000000', 138, 15926, 13, 0.8549591705597201),
        (108796, '2023-05-25 18:43:18.000000', 138, 15932, 13, 0.6213586835883191),
        (108795, '2023-05-25 18:00:00.000000', 138, 15932, 13, 0.6730718577847092),
        (107326, '2023-05-25 14:00:01.000000', 138, 15932, 13, 0.278131899094646),
        (107298, '2023-05-25 14:00:00.000000', 138, 15932, 13, 0.92423751071723),
        (104702, '2023-05-25 12:36:55.000000', 138, 15932, 13, 0.22221315122722984),
        (105175, '2023-05-25 12:00:01.000000', 138, 15932, 13, 0.28839114292751233),
        (102736, '2023-05-25 16:15:29.000000', 138, 16052, 13, 0.431037595792759),
        (99163, '2023-05-25 13:47:30.000000', 138, 16419, 13, 0.5291021511946319),
        (102738, '2023-05-25 13:45:05.000000', 138, 16420, 13, 0.6506497895856924),
        (99109, '2023-05-25 13:37:49.000000', 138, 16420, 13, 0.019501542758906254),
        (108798, '2023-05-25 18:43:18.000000', 138, 16590, 13, 0.8990882904615916),
        (108797, '2023-05-25 18:00:00.000000', 138, 16590, 13, 0.8888186371755147),
        (107328, '2023-05-25 14:00:01.000000', 138, 16590, 13, 0.019486942610562608),
        (107300, '2023-05-25 14:00:00.000000', 138, 16590, 13, 0.5614292991802508),
        (104698, '2023-05-25 12:36:55.000000', 138, 16590, 13, 0.01866956387405594),
        (105173, '2023-05-25 12:00:01.000000', 138, 16590, 13, 0.25661478763909074),
        (107224, '2023-05-25 13:51:57.000000', 138, 16633, 13, 0.0010321723593804677),
        (99064, '2023-05-25 13:37:49.000000', 138, 16633, 13, 0.8675616866165861),
        (109225, '2023-05-25 22:13:01.000000', 138, 16669, 13, 0.1076822852142385),
        (109224, '2023-05-25 21:11:56.000000', 138, 16669, 13, 0.24001365186054713);

SELECT compress_chunk(show_chunks('test', older_than => INTERVAL '1 week'), true);

SELECT t.dttm FROM test t WHERE t.dttm > '2023-05-25T14:23:12' ORDER BY t.dttm;
