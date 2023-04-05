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

CREATE TABLE sample_table (
       time TIMESTAMP WITH TIME ZONE NOT NULL,
       sensor_id INTEGER NOT NULL,
       cpu double precision null,
       temperature double precision null,
       name varchar(100) default 'this is a default string value'
);

SELECT * FROM create_hypertable('sample_table', 'time',
       chunk_time_interval => INTERVAL '2 months');

SELECT '2022-01-28 01:09:53.583252+05:30' as start_date \gset
INSERT INTO sample_table
    SELECT
       	time + (INTERVAL '1 minute' * random()) AS time,
       		sensor_id,
       		random() AS cpu,
       		random()* 100 AS temperature
       	FROM
       		generate_series(:'start_date'::timestamptz - INTERVAL '1 months',
                            :'start_date'::timestamptz - INTERVAL '1 week',
                            INTERVAL '1 hour') AS g1(time),
       		generate_series(1, 8, 1 ) AS g2(sensor_id)
       	ORDER BY
       		time;

SELECT '2023-03-17 17:51:11.322998+05:30' as start_date \gset
-- insert into new chunks
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 12, 21.98, 33.123, 'new row1');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 12, 17.66, 13.875, 'new row1');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 13, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 1, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 4, 21.98, 33.123, 'new row2');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 5, 0.988, 33.123, 'new row3');
INSERT INTO sample_table VALUES (:'start_date'::timestamptz, 6, 4.6554, 47, 'new row3');

-- enable compression
ALTER TABLE sample_table SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'sensor_id'
);

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- test rows visibility
BEGIN;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE name = 'updated row';
-- update 4 rows
UPDATE sample_table SET name = 'updated row' WHERE cpu = 21.98 AND temperature = 33.123;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE name = 'updated row';
ROLLBACK;

-- get count of affected rows
SELECT count(*) FROM sample_table WHERE cpu = 21.98 AND temperature = 33.123;
-- do update
UPDATE sample_table SET name = 'updated row' WHERE cpu = 21.98 AND temperature = 33.123;
-- get count of updated rows
SELECT count(*) FROM sample_table WHERE name = 'updated row';

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- recompress the paritial chunks
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
CALL recompress_chunk('_timescaledb_internal._hyper_1_2_chunk');

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- get count of affected rows
SELECT count(*) FROM sample_table WHERE name = 'updated row';
-- do delete
DELETE FROM sample_table WHERE name = 'updated row';
-- get count of updated rows
SELECT count(*) FROM sample_table WHERE name = 'updated row';

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- recompress the paritial chunks
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
CALL recompress_chunk('_timescaledb_internal._hyper_1_2_chunk');

-- test for IS NULL checks
-- should not UPDATE any rows
UPDATE sample_table SET temperature = 34.21 WHERE sensor_id IS NULL;

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- test for IS NOT NULL checks
-- should UPDATE all rows
UPDATE sample_table SET temperature = 34.21 WHERE sensor_id IS NOT NULL;

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- recompress the paritial chunks
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
CALL recompress_chunk('_timescaledb_internal._hyper_1_2_chunk');

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- report 0 rows
SELECT COUNT(*) FROM sample_table WHERE name = 'updated row based on < OR > comparison';
-- get total count of rows which satifies the condition
SELECT COUNT(*) as "total_affected_rows" FROM sample_table WHERE
  time > '2022-01-20 19:10:00.101514+05:30' and
  time < '2022-01-20 21:10:43.855297+05:30' \gset

-- perform UPDATE with < and > comparison on SEGMENTBY column
UPDATE sample_table SET name = 'updated row based on < OR > comparison' WHERE
  time > '2022-01-20 19:10:00.101514+05:30' and time < '2022-01-20 21:10:43.855297+05:30';

-- check chunk compression status after UPDATE
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- count should be same
SELECT COUNT(*) = (:total_affected_rows) FROM sample_table WHERE name = 'updated row based on < OR > comparison';

DROP TABLE sample_table;

-- test to ensure that only required rows from compressed chunks
-- are extracted if SEGMENTBY column is used in WHERE condition
CREATE TABLE sample_table(
    time INT NOT NULL,
    device_id INT,
    val INT);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 10);

ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');

INSERT INTO sample_table VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (10, 3, 2), (11, 4, 2), (11, 1, 2);

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- there should 2 rows matching the conditions coming from 2 chunks
SELECT * FROM sample_table WHERE  device_id = 3 ORDER BY time, device_id;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id = 3
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id = 3;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id = 3;

-- delete rows with device_id = 3
DELETE FROM sample_table WHERE device_id = 3;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id = 3
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id = 3;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id = 3;

-- there should be no rows
SELECT * FROM sample_table WHERE  device_id = 3 ORDER BY time, device_id;

-- there should 2 rows matching the conditions coming from 2 chunks
SELECT val FROM sample_table WHERE  1 = device_id ORDER BY time, device_id;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id = 1
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE 1 = device_id;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE 1 = device_id;

-- update rows with device_id = 1
UPDATE sample_table SET val = 200 WHERE 1 = device_id;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id = 1
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE 1 = device_id;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE 1 = device_id;

-- there should be 2 rows
SELECT val FROM sample_table WHERE  1 = device_id ORDER BY time, device_id;

DROP TABLE sample_table;

CREATE TABLE sample_table(
    time INT NOT NULL,
    device_id INT,
    val INT);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 10);

ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'time, val');

INSERT INTO sample_table VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (1, 3, 2), (11, 4, 2), (1, 1, 2);

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- there should 2 rows matching the conditions coming from 2 chunks
SELECT * FROM sample_table WHERE time = 1 AND val = 2 ORDER BY time, device_id;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where time = 1 AND val = 2
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE time = 1 AND val = 2;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE time = 1 AND val = 2;

-- delete rows with time = 1 AND val = 2
EXPLAIN (costs off, verbose) DELETE FROM sample_table WHERE time = 1 AND 2 = val;
-- should delete rows from 1 of the compressed chunks
DELETE FROM sample_table WHERE time = 1 AND 2 = val;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks wheretime = 1 AND val = 2
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE time = 1 AND val = 2;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE time = 1 AND val = 2;

-- there should be no rows
SELECT * FROM sample_table WHERE time = 1 AND val = 2 ORDER BY time, device_id;
DROP TABLE sample_table;

-- Test chunk compile time startup exclusion
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE TABLE sample_table(time timestamptz NOT NULL, temp float, colorid integer, attr jsonb);
SELECT create_hypertable('sample_table', 'time', chunk_time_interval => 2628000000000);

-- create three chunks
INSERT INTO sample_table VALUES ('2017-03-22T09:18:22', 23.5, 1, '{"a": 1, "b": 2}'),
                                ('2017-03-22T09:18:23', 21.5, 1, '{"a": 1, "b": 2}'),
                                ('2017-05-22T09:18:22', 36.2, 2, '{"c": 3, "b": 2}'),
                                ('2017-05-22T09:18:23', 15.2, 2, '{"c": 3}'),
                                ('2017-08-22T09:18:22', 34.1, 3, '{"c": 4}');

ALTER TABLE sample_table SET (timescaledb.compress,
                              timescaledb.compress_segmentby = 'time');

SELECT compress_chunk(show_chunks('sample_table'));
-- ensure all chunks are compressed
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- report 0 rows
SELECT * FROM sample_table WHERE time > now_s() + '-1 month' AND colorid = 4;
-- update 1 row
UPDATE sample_table SET colorid = 4 WHERE time > now_s() + '-1 month';
-- report 1 row
SELECT * FROM sample_table WHERE time > now_s() + '-1 month' AND colorid = 4;

-- ensure that 1 chunk is partially compressed
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

DROP TABLE sample_table;

-- test for NULL values in SEGMENTBY column
CREATE TABLE sample_table(
    time INT,
    device_id INT,
    val INT);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 10);

ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');

INSERT INTO sample_table VALUES (1, 1, 1), (2, NULL, 1), (3, NULL, 1), (10, NULL, 2), (11, NULL, 2), (11, 1, 2);

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id IS NULL
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id IS NULL;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id IS NULL;

-- get total count of SEGMENTBY column with NULL values
SELECT COUNT(*) FROM sample_table WHERE device_id IS NULL;

-- delete NULL values in SEGMENTBY column
DELETE FROM sample_table WHERE device_id IS NULL;

-- ensure that not all rows are moved to staging area
-- should have few compressed rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id IS NULL
-- should report 0 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id IS NULL;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id IS NULL;

-- check chunk compression status after DELETE
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

DROP TABLE sample_table;

-- test for IS NOT NULL values in SEGMENTBY column
CREATE TABLE sample_table(
    time INT,
    device_id INT,
    val INT);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 10);

ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');

INSERT INTO sample_table VALUES (1, NULL, 1), (2, NULL, 1), (3, NULL, 1), (10, 3, 2), (11, 2, 2), (11, 1, 2);

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

SELECT COUNT(*) FROM sample_table WHERE val = 1234;

-- UPDATE based on IS NOT NULL condition on SEGMENTBY column
UPDATE sample_table SET val = 1234 WHERE device_id IS NOT NULL;

-- get total count of SEGMENTBY column with NULL values
SELECT COUNT(*) FROM sample_table WHERE val = 1234;

-- check chunk compression status after DELETE
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

DROP TABLE sample_table;

-- test to for <= AND >= on SEGMENTBY column
CREATE TABLE sample_table(
    time INT,
    device_id INT,
    val INT);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 10);

ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id, val');

INSERT INTO sample_table VALUES (1, 1, 1), (2, NULL, 1), (3, 4, 1), (10, NULL, 2), (11, NULL, 2), (11, 1, 2), (13, 5, 3);
INSERT INTO sample_table VALUES (4, 3, NULL), (6, NULL, NULL), (12, NULL, NULL), (13, 4, NULL);
-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

-- test will multiple NULL/NOT NULL columns
BEGIN;
-- report 0 row
SELECT * FROM sample_table WHERE device_id IS NULL AND val = 987;
-- these 3 rows will be affected by below UPDATE
SELECT * FROM sample_table WHERE device_id IS NULL AND val IS NOT NULL ORDER BY 1;
-- update 3 rows
UPDATE sample_table SET val = 987 WHERE device_id IS NULL AND val IS NOT NULL;
-- report 3 row
SELECT * FROM sample_table WHERE device_id IS NULL AND val = 987;
ROLLBACK;

-- test will multiple columns
BEGIN;
-- report 2 rows
SELECT * FROM sample_table WHERE device_id IS NULL AND val = 2;
-- delete 2 rows
DELETE from sample_table WHERE device_id IS NULL AND val = 2;
-- report 0 rows
SELECT * FROM sample_table WHERE device_id IS NULL AND val = 2;
ROLLBACK;

BEGIN;
-- report 1 row
SELECT * FROM sample_table WHERE device_id = 3 AND val IS NULL;
-- delete 1 rows
DELETE from sample_table WHERE device_id = 3 AND val IS NULL;
-- report 0 rows
SELECT * FROM sample_table WHERE device_id = 3 AND val IS NULL;
ROLLBACK;

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id >= 4 AND val <= 1
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id >= 4 AND val <= 1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id >= 4 AND val <= 1;

-- get total count of SEGMENTBY column with device_id >= 4 AND val <= 1
SELECT COUNT(*) FROM sample_table WHERE device_id >= 4 AND val <= 1;

-- delete NULL values in SEGMENTBY column
DELETE FROM sample_table WHERE device_id >= 4 AND val <= 1;

-- ensure that not all rows are moved to staging area
-- should have few compressed rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id IS NULL
-- should report 0 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id >= 4 AND val <= 1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id >= 4 AND val <= 1;

-- check chunk compression status after DELETE
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- added tests for code coverage
UPDATE sample_table SET time = 21 WHERE (device_id) in ( 30, 51, 72, 53);
UPDATE sample_table SET time = 21 WHERE device_id + 365 = 8765;

DROP TABLE sample_table;

-- test with different physical layout
CREATE TABLE sample_table(
    time INT,
    device_id INT,
    val INT default 8,
    a INT default 10,
    b INT default 11,
    c INT default 12,
    d INT,
    e INT default 13);

SELECT * FROM create_hypertable('sample_table', 'time', chunk_time_interval => 5);
INSERT INTO sample_table (time, device_id, d) VALUES (1, 1, 1), (2, NULL, 1), (3, 4, 1), (10, NULL, 2), (11, NULL, 2), (11, 1, 2), (13, 5, 3);

ALTER TABLE sample_table DROP COLUMN c;
ALTER TABLE sample_table SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id, d');

-- compress all chunks
SELECT compress_chunk(show_chunks('sample_table'));

ALTER TABLE sample_table ADD COLUMN c int default 23;

-- check chunk compression status
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- get FIRST uncompressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "UNCOMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE '_hyper_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND uncompressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "UNCOMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE '_hyper_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get SECOND compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_2"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- ensure segment by column index position in compressed and uncompressed
-- chunk is different
SELECT attname, attnum
FROM pg_attribute
WHERE attrelid IN (:'COMPRESS_CHUNK_1'::regclass, :'UNCOMPRESS_CHUNK_1'::regclass) AND attname = 'd'
ORDER BY attnum;

SELECT attname, attnum
FROM pg_attribute
WHERE attrelid IN (:'COMPRESS_CHUNK_2'::regclass, :'UNCOMPRESS_CHUNK_2'::regclass) AND attname = 'd'
ORDER BY attnum;

-- get total rowcount from compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where d = 3
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE d = 3;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE d = 3;

-- get total count of SEGMENTBY column with d = 3
SELECT COUNT(*) FROM sample_table WHERE d = 3;

-- delete based on SEGMENTBY column
DELETE FROM sample_table WHERE d = 3;

-- ensure that not all rows are moved to staging area
-- should have few compressed rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where d = 3
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE d = 3;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE d = 3;

-- check chunk compression status after DELETE
SELECT chunk_status,
       chunk_name as "CHUNK_NAME"
FROM compressed_chunk_info_view
WHERE hypertable_name = 'sample_table' ORDER BY chunk_name;

-- get rowcount from compressed chunks where device_id IS NULL
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id IS NULL;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id IS NULL;

BEGIN;
-- report 0 row
SELECT * FROM sample_table WHERE a = 247;
-- delete 1 row
UPDATE sample_table SET a = 247 WHERE device_id IS NULL;
-- ensure rows are visible
SELECT * FROM sample_table WHERE a = 247;
ROLLBACK;

-- report 0 rows
SELECT COUNT(*) FROM sample_table WHERE a = 247;

-- UPDATE based on NULL values in SEGMENTBY column
UPDATE sample_table SET a = 247 WHERE device_id IS NULL;

-- report 3 rows
SELECT COUNT(*) FROM sample_table WHERE a = 247;

-- ensure that not all rows are moved to staging area
-- should have few compressed rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2;

-- get rowcount from compressed chunks where device_id IS NULL
-- should report 0 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE device_id IS NULL;
SELECT COUNT(*) FROM :COMPRESS_CHUNK_2 WHERE device_id IS NULL;

DROP TABLE sample_table;

-- test with different physical layout
CREATE TABLE sample_table(time timestamptz, c1 text, c2 text, c3 text);
SELECT create_hypertable('sample_table','time');
INSERT INTO sample_table SELECT '2000-01-01';
ALTER TABLE sample_table DROP column c3;
ALTER TABLE sample_table ADD column c4 text;
INSERT INTO sample_table SELECT '2000-01-01', '1', '2', '3';
ALTER TABLE sample_table SET (timescaledb.compress,timescaledb.compress_segmentby='c4');
SELECT compress_chunk(show_chunks('sample_table'));

BEGIN;
-- report 1 row
SELECT * FROM sample_table WHERE c4 IS NULL;
-- delete 1 row
DELETE FROM sample_table WHERE c4 IS NULL;
-- report 0 rows
SELECT * FROM sample_table WHERE c4 IS NULL;
ROLLBACK;

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id \gset

-- report 2 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
-- report 1 row
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 IS NULL;

-- report 1 row
SELECT * FROM sample_table WHERE c4 IS NULL;
-- delete 1 row
DELETE FROM sample_table WHERE c4 IS NULL;
-- report 0 row
SELECT * FROM sample_table WHERE c4 IS NULL;

-- report 1 row which ensure that only required row is moved and deleted
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1;
-- report 0 row
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 IS NULL;

DROP TABLE sample_table;
