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

\set start_date '2022-01-28 01:09:53.583252+05:30'

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

\set start_date '2023-03-17 17:51:11.322998+05:30'

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

-- recompress the partial chunks
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

-- test filtering with ORDER BY columns
CREATE TABLE sample_table(time timestamptz, c1 int, c2 int, c3 int, c4 int);
SELECT create_hypertable('sample_table','time',chunk_time_interval=>'1 day'::interval);
ALTER TABLE sample_table SET (timescaledb.compress,timescaledb.compress_segmentby='c4', timescaledb.compress_orderby='c1,c2,time');
INSERT INTO sample_table
SELECT t, c1, c2, c3, c4
FROM generate_series(:'start_date'::timestamptz - INTERVAL '9 hours',
            :'start_date'::timestamptz,
            INTERVAL '1 hour') t,
    generate_series(0,9,1) c1,
    generate_series(0,9,1) c2,
    generate_series(0,9,1) c3,
    generate_series(0,9,1) c4;
SELECT compress_chunk(show_chunks('sample_table'));

-- get FIRST chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE '_hyper_%'
ORDER BY ch1.id LIMIT 1 \gset

-- get FIRST compressed chunk
SELECT ch1.schema_name|| '.' || ch1.table_name AS "COMPRESS_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ch1.table_name LIKE 'compress_%'
ORDER BY ch1.id LIMIT 1 \gset

-- check that you uncompress and delete only for exact SEGMENTBY value
SET client_min_messages TO DEBUG1;
BEGIN;
-- report 10 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 = 5;
-- report 10k rows
SELECT COUNT(*) FROM sample_table WHERE c4 = 5;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c4 = 5 \gset
-- delete 10k rows
DELETE FROM sample_table WHERE c4 = 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 = 5;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunk
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 = 5;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only for less than SEGMENTBY value
BEGIN;
-- report 50 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 < 5;
-- report 50k rows
SELECT COUNT(*) FROM sample_table WHERE c4 < 5;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c4 < 5 \gset
-- delete 50k rows
DELETE FROM sample_table WHERE c4 < 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 < 5;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunk
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 < 5;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only for greater and equal than SEGMENTBY value
BEGIN;
-- report 50 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 >= 5;
-- report 50k rows
SELECT COUNT(*) FROM sample_table WHERE c4 >= 5;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c4 >= 5 \gset
-- delete 50k rows
DELETE FROM sample_table WHERE c4 >= 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 >= 5;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunk
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 where c4 >= 5;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only for exact ORDERBY value
-- this will uncompress segments which have min <= value and max >= value
BEGIN;
-- report 10k rows
SELECT COUNT(*) FROM sample_table WHERE c2 = 3;
-- report 100 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_min_2 <= 3 and _ts_meta_max_2 >= 3;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c2 = 3 \gset
-- delete 10k rows
DELETE FROM sample_table WHERE c2 = 3;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c2 = 3;
-- report 90k rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_min_2 <= 3 and _ts_meta_max_2 >= 3;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only for less then ORDERBY value
-- this will uncompress segments which have min < value
BEGIN;
-- report 20k rows
SELECT COUNT(*) FROM sample_table WHERE c1 < 2;
-- report 20 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_max_1 < 2;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c1 < 2 \gset
-- delete 20k rows
DELETE FROM sample_table WHERE c1 < 2;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c1 < 2;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunk
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_max_1 < 2;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only for greater or equal then ORDERBY value
-- this will uncompress segments which have max >= value
BEGIN;
-- report 30k rows
SELECT COUNT(*) FROM sample_table WHERE c1 >= 7;
-- report 30 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_min_1 >= 7;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c1 >= 7 \gset
-- delete 30k rows
DELETE FROM sample_table WHERE c1 >= 7;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c1 >= 7;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE _ts_meta_min_1 >= 7;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only tuples which satisfy SEGMENTBY
-- and ORDERBY qualifiers, segments only contain one distinct value for
-- these qualifiers, everything should be deleted that was decompressed
BEGIN;
-- report 1k rows
SELECT COUNT(*) FROM sample_table WHERE c4 = 5 and c1 = 5;
-- report 1 row in compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 = 5 AND _ts_meta_min_1 <= 5 and _ts_meta_max_1 >= 5;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c4 = 5 and c1 = 5 \gset
-- delete 1k rows
DELETE FROM sample_table WHERE c4 = 5 and c1 = 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 = 5 and c1 = 5;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 = 5 AND _ts_meta_min_1 <= 5 and _ts_meta_max_1 >= 5;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only tuples which satisfy SEGMENTBY
-- and ORDERBY qualifiers, segments contain more than one distinct value for
-- these qualifiers, not everything should be deleted that was decompressed
BEGIN;
-- report 4k rows
SELECT COUNT(*) FROM sample_table WHERE c4 > 5 and c2 = 5;
-- report 40 rows in compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 > 5 AND _ts_meta_min_2 <= 5 and _ts_meta_max_2 >= 5;
-- fetch total and number of affected rows
SELECT COUNT(*) AS "total_rows" FROM sample_table \gset
SELECT COUNT(*) AS "total_affected_rows" FROM sample_table WHERE c4 > 5 and c2 = 5 \gset
-- delete 4k rows
DELETE FROM sample_table WHERE c4 > 5 and c2 = 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 > 5 and c2 = 5;
-- report 36k rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 0 rows in compressed chunks
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 > 5 AND _ts_meta_min_2 <= 5 and _ts_meta_max_2 >= 5;
-- validate correct number of rows was deleted
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM sample_table;
ROLLBACK;

-- check that you uncompress and delete only tuples which satisfy SEGMENTBY
-- and ORDERBY qualifiers.
-- no: of rows satisfying SEGMENTBY qualifiers is 10
-- no: of rows satisfying ORDERBY qualifiers is 3
-- Once both qualifiers are applied ensure that only 7 rows are present in
-- compressed chunk
BEGIN;
-- report 0 rows in uncompressed chunk
SELECT COUNT(*) FROM ONLY :CHUNK_1;
-- report 10 compressed rows for given condition c4 = 4
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 = 4;
-- report 3 compressed rows for given condition c4 = 4 and c1 >= 7
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 = 4 AND _ts_meta_max_1 >= 7;
SELECT COUNT(*) AS "total_rows" FROM :COMPRESS_CHUNK_1 WHERE c4 = 4 \gset
SELECT COUNT(*) AS "total_affected_rows" FROM :COMPRESS_CHUNK_1 WHERE c4 = 4 AND _ts_meta_max_1 >= 7 \gset
UPDATE sample_table SET c3 = c3 + 0 WHERE c4 = 4 AND c1 >= 7;
-- report 7 rows
SELECT COUNT(*) FROM :COMPRESS_CHUNK_1 WHERE c4 = 4;
-- ensure correct number of rows are moved from compressed chunk
-- report true
SELECT COUNT(*) = :total_rows - :total_affected_rows FROM :COMPRESS_CHUNK_1 WHERE c4 = 4;
ROLLBACK;
RESET client_min_messages;

--github issue: 5640
CREATE TABLE tab1(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON tab1(time);
CREATE INDEX ON tab1(device_id,time);
SELECT create_hypertable('tab1','time',create_default_indexes:=false);

ALTER TABLE tab1 DROP COLUMN filler_1;
INSERT INTO tab1(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','57m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ALTER TABLE tab1 DROP COLUMN filler_2;
INSERT INTO tab1(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','58m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ALTER TABLE tab1 DROP COLUMN filler_3;
INSERT INTO tab1(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','59m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ANALYZE tab1;

-- compress chunks
ALTER TABLE tab1 SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('tab1'));

-- ensure there is an index scan generated for below DELETE query
BEGIN;
SELECT count(*) FROM tab1 WHERE device_id = 1;
INSERT INTO tab1(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 1000, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*) FROM tab1 WHERE device_id = 1;
ANALYZE tab1;
EXPLAIN (costs off) DELETE FROM public.tab1 WHERE public.tab1.device_id = 1;
DELETE FROM tab1 WHERE tab1.device_id = 1;
SELECT count(*) FROM tab1 WHERE device_id = 1;
ROLLBACK;

-- github issue 5658
-- verify that bitmap heap scans work on all the correct data and
-- none of it left over after the dml command
BEGIN;
SELECT count(*) FROM tab1 WHERE device_id = 1;
INSERT INTO tab1(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 1000, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*) FROM tab1 WHERE device_id = 1;
ANALYZE tab1;
SET enable_seqscan = off;
SET enable_indexscan = off;
EXPLAIN (costs off) DELETE FROM tab1 WHERE tab1.device_id = 1;
DELETE FROM tab1 WHERE tab1.device_id = 1;
SELECT count(*) FROM tab1 WHERE device_id = 1;
ROLLBACK;

-- create hypertable with space partitioning and compression
CREATE TABLE tab2(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON tab2(time);
CREATE INDEX ON tab2(device_id,time);
SELECT create_hypertable('tab2','time','device_id',3,create_default_indexes:=false);

ALTER TABLE tab2 DROP COLUMN filler_1;
INSERT INTO tab2(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','35m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ALTER TABLE tab2 DROP COLUMN filler_2;
INSERT INTO tab2(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','45m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ALTER TABLE tab2 DROP COLUMN filler_3;
INSERT INTO tab2(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','55m') gtime(time), generate_series(1,1,1) gdevice(device_id);
ANALYZE tab2;

-- compress chunks
ALTER TABLE tab2 SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('tab2'));

-- below test will cause chunks of tab2 to get decompressed
-- without fix for issue #5460
SET timescaledb.enable_optimizations = OFF;
BEGIN;
DELETE FROM tab1 t1 USING tab2 t2 WHERE t1.device_id = t2.device_id AND t2.time > '2000-01-01';
ROLLBACK;

--cleanup
RESET timescaledb.enable_optimizations;
DROP table tab1;
DROP table tab2;

-- test joins with UPDATE/DELETE on compression chunks
CREATE TABLE join_test1(time timestamptz NOT NULL,device text, value float);
CREATE TABLE join_test2(time timestamptz NOT NULL,device text, value float);
CREATE VIEW chunk_status AS SELECT ht.table_name AS hypertable, ch.table_name AS chunk,ch.status from _timescaledb_catalog.chunk ch INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id AND ht.table_name IN ('join_test1','join_test2') ORDER BY ht.id, ch.id;

SELECT table_name FROM create_hypertable('join_test1', 'time');
SELECT table_name FROM create_hypertable('join_test2', 'time');

ALTER TABLE join_test1 SET (timescaledb.compress, timescaledb.compress_segmentby='device');
ALTER TABLE join_test2 SET (timescaledb.compress, timescaledb.compress_segmentby='device');

INSERT INTO join_test1 VALUES ('2000-01-01','d1',0.1), ('2000-02-01','d1',0.1), ('2000-03-01','d1',0.1);
INSERT INTO join_test2 VALUES ('2000-02-01','d1',0.1), ('2000-02-01','d2',0.1), ('2000-02-01','d3',0.1);

SELECT compress_chunk(show_chunks('join_test1'));
SELECT compress_chunk(show_chunks('join_test2'));

SELECT * FROM chunk_status;

BEGIN;
DELETE FROM join_test1 USING join_test2;
-- only join_test1 chunks should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
DELETE FROM join_test2 USING join_test1;
-- only join_test2 chunks should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
DELETE FROM join_test1 t1 USING join_test1 t2 WHERE t1.time = '2000-01-01';
-- only first chunk of join_test1 should have status change
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
DELETE FROM join_test1 t1 USING join_test1 t2 WHERE t2.time = '2000-01-01';
-- all chunks of join_test1 should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
UPDATE join_test1 t1 SET value = t1.value + 1 FROM join_test2 t2;
-- only join_test1 chunks should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
UPDATE join_test2 t1 SET value = t1.value + 1 FROM join_test1 t2;
-- only join_test2 chunks should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
UPDATE join_test1 t1 SET value = t1.value + 1 FROM join_test1 t2 WHERE t1.time = '2000-01-01';
-- only first chunk of join_test1 should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

BEGIN;
UPDATE join_test1 t1 SET value = t1.value + 1 FROM join_test1 t2 WHERE t2.time = '2000-01-01';
-- all chunks of join_test1 should have status 9
SELECT * FROM chunk_status;
ROLLBACK;

DROP TABLE join_test1;
DROP TABLE join_test2;

-- test if index scan qualifiers are properly used
CREATE TABLE index_scan_test(time timestamptz NOT NULL, device_id int, value float);
SELECT create_hypertable('index_scan_test','time',create_default_indexes:=false);
INSERT INTO index_scan_test(time,device_id,value) SELECT time, device_id, device_id + 0.5 FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-01 23:55:00+0','1m') gtime(time), generate_series(1,5,1) gdevice(device_id);

-- compress chunks
ALTER TABLE index_scan_test SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('index_scan_test'));
ANALYZE index_scan_test;

SELECT ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ht.table_name = 'index_scan_test'
AND ch1.hypertable_id = ht.id
AND ch1.table_name LIKE '_hyper%'
ORDER BY ch1.id LIMIT 1 \gset

SELECT ch2.schema_name|| '.' || ch2.table_name AS "COMP_CHUNK_1"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk ch2, _timescaledb_catalog.hypertable ht
WHERE ht.table_name = 'index_scan_test'
AND ch1.hypertable_id = ht.id
AND ch1.compressed_chunk_id  = ch2.id
ORDER BY ch2.id LIMIT 1 \gset

INSERT INTO index_scan_test(time,device_id,value) SELECT time, device_id, device_id + 0.5 FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','1m') gtime(time), generate_series(1,5,1) gdevice(device_id);

-- test index on single column
BEGIN;
SELECT count(*) as "UNCOMP_LEFTOVER" FROM ONLY :CHUNK_1 WHERE device_id != 2 \gset
CREATE INDEX ON index_scan_test(device_id);
EXPLAIN (costs off, verbose) DELETE FROM index_scan_test WHERE device_id = 2;
DELETE FROM index_scan_test WHERE device_id = 2;
-- everything should be deleted
SELECT count(*) FROM index_scan_test where device_id = 2;

-- there shouldn't be anything in the uncompressed chunk where device_id = 2
SELECT count(*) = :UNCOMP_LEFTOVER FROM ONLY :CHUNK_1;
-- there shouldn't be anything in the compressed chunk from device_id = 2
SELECT count(*) FROM :COMP_CHUNK_1 where device_id = 2;
ROLLBACK;

-- test multi column index
BEGIN;
SELECT count(*) as "UNCOMP_LEFTOVER" FROM ONLY :CHUNK_1 WHERE device_id != 2 OR time <= '2000-01-02'::timestamptz \gset
CREATE INDEX ON index_scan_test(device_id, time);
EXPLAIN (costs off, verbose) DELETE FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;
DELETE FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;
-- everything should be deleted
SELECT count(*) FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;

-- there shouldn't be anything in the uncompressed chunk that matches predicates
SELECT count(*) = :UNCOMP_LEFTOVER FROM ONLY :CHUNK_1;
-- there shouldn't be anything in the compressed chunk that matches predicates
SELECT count(*) FROM :COMP_CHUNK_1 WHERE device_id = 2 AND _ts_meta_max_1 >= '2000-01-02'::timestamptz;
ROLLBACK;

-- test index with filter condition
BEGIN;
SELECT count(*) as "UNCOMP_LEFTOVER" FROM ONLY :CHUNK_1 WHERE device_id != 2 OR time <= '2000-01-02'::timestamptz \gset
CREATE INDEX ON index_scan_test(device_id);
EXPLAIN (costs off, verbose) DELETE FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;
DELETE FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;
-- everything should be deleted
SELECT count(*) FROM index_scan_test WHERE device_id = 2 AND time > '2000-01-02'::timestamptz;

-- there shouldn't be anything in the uncompressed chunk that matches predicates
SELECT count(*) = :UNCOMP_LEFTOVER FROM ONLY :CHUNK_1;
-- there shouldn't be anything in the compressed chunk that matches predicates
SELECT count(*) FROM :COMP_CHUNK_1 WHERE device_id = 2 AND _ts_meta_max_1 >= '2000-01-02'::timestamptz;
ROLLBACK;

-- test for disabling DML decompression
SHOW timescaledb.enable_dml_decompression;
SET timescaledb.enable_dml_decompression = false;

\set ON_ERROR_STOP 0
-- should ERROR both UPDATE/DELETE statements because the DML decompression is disabled
UPDATE sample_table SET c3 = NULL WHERE c4 = 5;
DELETE FROM sample_table WHERE c4 = 5;
\set ON_ERROR_STOP 1

-- make sure reseting the GUC we will be able to UPDATE/DELETE compressed chunks
RESET timescaledb.enable_dml_decompression;
SHOW timescaledb.enable_dml_decompression;

BEGIN;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 = 5 AND c3 IS NULL;
UPDATE sample_table SET c3 = NULL WHERE c4 = 5;
-- report 10k rows
SELECT count(*) FROM sample_table WHERE c4 = 5 AND c3 IS NULL;
ROLLBACK;

BEGIN;
-- report 10k rows
SELECT count(*) FROM sample_table WHERE c4 = 5;
DELETE FROM sample_table WHERE c4 = 5;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE c4 = 5;
ROLLBACK;

-- create new uncompressed chunk
INSERT INTO sample_table
SELECT t, 1, 1, 1, 1
FROM generate_series('2023-05-04 00:00:00-00'::timestamptz,
            '2023-05-04 00:00:00-00'::timestamptz + INTERVAL '2 hours',
            INTERVAL '1 hour') t;

-- check chunk compression status
SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'sample_table'
ORDER BY chunk_name;

-- test for uncompressed and compressed chunks
SHOW timescaledb.enable_dml_decompression;
SET timescaledb.enable_dml_decompression = false;

BEGIN;
-- report 3 rows
SELECT count(*) FROM sample_table WHERE time >= '2023-05-04 00:00:00-00'::timestamptz;
-- delete from uncompressed chunk should work
DELETE FROM sample_table WHERE time >= '2023-05-04 00:00:00-00'::timestamptz;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE time >= '2023-05-04 00:00:00-00'::timestamptz;
ROLLBACK;

BEGIN;
-- report 0 rows
SELECT count(*) FROM sample_table WHERE time >= '2023-05-04 00:00:00-00'::timestamptz AND c3 IS NULL;
UPDATE sample_table SET c3 = NULL WHERE time >= '2023-05-04 00:00:00-00'::timestamptz;
-- report 3 rows
SELECT count(*) FROM sample_table WHERE time >= '2023-05-04 00:00:00-00'::timestamptz AND c3 IS NULL;
ROLLBACK;

\set ON_ERROR_STOP 0
-- should ERROR both UPDATE/DELETE statements because the DML decompression is disabled
-- and both statements we're touching compressed and uncompressed chunks
UPDATE sample_table SET c3 = NULL WHERE time >= '2023-03-17 00:00:00-00'::timestamptz AND c3 IS NULL;
DELETE FROM sample_table WHERE time >= '2023-03-17 00:00:00-00'::timestamptz;
\set ON_ERROR_STOP 1

--github issue: 5586
--testcase with multiple indexes
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
\c test :ROLE_SUPERUSER
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
CREATE TABLE tab1(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('tab1','time',create_default_indexes:=false);
INSERT INTO tab1(filler_1, filler_2, filler_3,time,device_id,v0,v1,v2,v3) SELECT device_id, device_id+1,  device_id + 2, time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE tab1 SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id, filler_1, filler_2, filler_3');
-- create multiple indexes on compressed hypertable
DROP INDEX _timescaledb_internal._compressed_hypertable_2_device_id_filler_1_filler_2_filler_idx;
CREATE INDEX ON _timescaledb_internal._compressed_hypertable_2 (_ts_meta_min_1);
CREATE INDEX ON _timescaledb_internal._compressed_hypertable_2 (_ts_meta_min_1, _ts_meta_sequence_num);
CREATE INDEX ON _timescaledb_internal._compressed_hypertable_2 (_ts_meta_min_1, _ts_meta_max_1, filler_1);

CREATE INDEX filler_1 ON _timescaledb_internal._compressed_hypertable_2 (filler_1);
CREATE INDEX filler_2 ON _timescaledb_internal._compressed_hypertable_2 (filler_2);
CREATE INDEX filler_3 ON _timescaledb_internal._compressed_hypertable_2 (filler_3);
-- below indexes should be selected
CREATE INDEX filler_1_filler_2 ON _timescaledb_internal._compressed_hypertable_2 (filler_1, filler_2);
CREATE INDEX filler_2_filler_3 ON _timescaledb_internal._compressed_hypertable_2 (filler_2, filler_3);

SELECT compress_chunk(show_chunks('tab1'));
SET client_min_messages TO DEBUG2;
BEGIN;
SELECT COUNT(*) FROM tab1 WHERE filler_3 = 5 AND filler_2 = 4;
UPDATE tab1 SET v0 = v1 + v2 WHERE filler_3 = 5 AND filler_2 = 4;
ROLLBACK;

BEGIN;
SELECT COUNT(*) FROM tab1 WHERE filler_1 < 5 AND filler_2 = 4;
UPDATE tab1 SET v0 = v1 + v2 WHERE filler_1 < 5 AND filler_2 = 4;
ROLLBACK;

-- idealy filler_1 index should be selected,
-- instead first matching index is selected
BEGIN;
SELECT COUNT(*) FROM tab1 WHERE filler_1 < 5;
UPDATE tab1 SET v0 = v1 + v2 WHERE filler_1 < 5;
ROLLBACK;

RESET client_min_messages;
DROP TABLE tab1;
