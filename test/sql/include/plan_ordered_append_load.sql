-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create a now() function for repeatable testing that always returns
-- the same timestamp. It needs to be marked STABLE
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN '2000-01-08T0:00:00+0'::timestamptz;
END;
$BODY$;

CREATE TABLE devices(device_id INT PRIMARY KEY, name TEXT);
INSERT INTO devices VALUES
(1,'Device 1'),
(2,'Device 2'),
(3,'Device 3');

-- create a second table where we create chunks in reverse order
CREATE TABLE ordered_append_reverse(time timestamptz NOT NULL, device_id INT, value float);
SELECT create_hypertable('ordered_append_reverse','time');

INSERT INTO ordered_append_reverse SELECT generate_series('2000-01-18'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 0.5;

-- table where dimension column is last column
CREATE TABLE IF NOT EXISTS dimension_last(
    id INT8 NOT NULL,
    device_id INT NOT NULL,
    name TEXT NOT NULL,
    time timestamptz NOT NULL
);

SELECT create_hypertable('dimension_last', 'time', chunk_time_interval => interval '1day', if_not_exists => True);

-- table with only dimension column
CREATE TABLE IF NOT EXISTS dimension_only(
    time timestamptz NOT NULL
);

SELECT create_hypertable('dimension_only', 'time', chunk_time_interval => interval '1day', if_not_exists => True);

INSERT INTO dimension_last SELECT 1,1,'Device 1',generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-04 23:59:00+0'::timestamptz,'1m'::interval);

INSERT INTO dimension_only VALUES
('2000-01-01'),
('2000-01-03'),
('2000-01-05'),
('2000-01-07');

ANALYZE devices;
ANALYZE ordered_append_reverse;
ANALYZE dimension_last;
ANALYZE dimension_only;

-- create hypertable with indexes not on all chunks
CREATE TABLE ht_missing_indexes(time timestamptz NOT NULL, device_id int, value float);

SELECT create_hypertable('ht_missing_indexes','time');

INSERT INTO ht_missing_indexes SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 1, 0.5;
INSERT INTO ht_missing_indexes SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 2, 1.5;
INSERT INTO ht_missing_indexes SELECT generate_series('2000-01-01'::timestamptz,'2000-01-18'::timestamptz,'1m'::interval), 3, 2.5;

-- drop index from 2nd chunk of ht_missing_indexes
SELECT format('%I.%I',i.schemaname,i.indexname) AS "INDEX_NAME"
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable ht ON c.hypertable_id = ht.id
INNER JOIN pg_indexes i ON i.schemaname = c.schema_name AND i.tablename=c.table_name
WHERE ht.table_name = 'ht_missing_indexes'
ORDER BY c.id LIMIT 1 OFFSET 1 \gset

DROP INDEX :INDEX_NAME;

ANALYZE ht_missing_indexes;

-- create hypertable with with dropped columns
CREATE TABLE ht_dropped_columns(c1 int, c2 int, c3 int, c4 int, c5 int, time timestamptz NOT NULL, device_id int, value float);

SELECT create_hypertable('ht_dropped_columns','time');

ALTER TABLE ht_dropped_columns DROP COLUMN c1;
INSERT INTO ht_dropped_columns(time,device_id,value) SELECT generate_series('2000-01-01'::timestamptz,'2000-01-02'::timestamptz,'1m'::interval), 1, 0.5;
ALTER TABLE ht_dropped_columns DROP COLUMN c2;
INSERT INTO ht_dropped_columns(time,device_id,value) SELECT generate_series('2000-01-08'::timestamptz,'2000-01-09'::timestamptz,'1m'::interval), 1, 0.5;
ALTER TABLE ht_dropped_columns DROP COLUMN c3;
INSERT INTO ht_dropped_columns(time,device_id,value) SELECT generate_series('2000-01-15'::timestamptz,'2000-01-16'::timestamptz,'1m'::interval), 1, 0.5;
ALTER TABLE ht_dropped_columns DROP COLUMN c4;
INSERT INTO ht_dropped_columns(time,device_id,value) SELECT generate_series('2000-01-22'::timestamptz,'2000-01-23'::timestamptz,'1m'::interval), 1, 0.5;
ALTER TABLE ht_dropped_columns DROP COLUMN c5;
INSERT INTO ht_dropped_columns(time,device_id,value) SELECT generate_series('2000-01-29'::timestamptz,'2000-01-30'::timestamptz,'1m'::interval), 1, 0.5;

ANALYZE ht_dropped_columns;

CREATE TABLE space2(time timestamptz NOT NULL, device_id int NOT NULL, tag_id int NOT NULL, value float);
SELECT create_hypertable('space2','time','device_id',number_partitions:=3);
SELECT add_dimension('space2','tag_id',number_partitions:=3);

INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 1, 1.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 1, 2.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 3, 1, 3.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 2, 1.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 2, 2.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 3, 2, 3.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 3, 1.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 3, 2.5;
INSERT INTO space2 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 3, 3, 3.5;

ANALYZE space2;

CREATE TABLE space3(time timestamptz NOT NULL, x int NOT NULL, y int NOT NULL, z int NOT NULL, value float);
SELECT create_hypertable('space3','time','x',number_partitions:=2);
SELECT add_dimension('space3','y',number_partitions:=2);
SELECT add_dimension('space3','z',number_partitions:=2);

INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 1, 1, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 1, 2, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 2, 1, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 1, 2, 2, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 1, 1, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 1, 2, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 2, 1, 1.5;
INSERT INTO space3 SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 2, 2, 2, 1.5;

ANALYZE space3;

CREATE TABLE sortopt_test(time timestamptz NOT NULL, device TEXT);
SELECT create_hypertable('sortopt_test','time',create_default_indexes:=false);

-- since alpine does not support locales we cant test collations in our ci
-- CREATE COLLATION IF NOT EXISTS en_US(LOCALE='en_US.utf8');
-- CREATE INDEX time_device_utf8 ON sortopt_test(time, device COLLATE "en_US");

CREATE INDEX time_device_nullsfirst ON sortopt_test(time, device NULLS FIRST);
CREATE INDEX time_device_nullslast ON sortopt_test(time, device DESC NULLS LAST);

INSERT INTO sortopt_test SELECT generate_series('2000-01-10'::timestamptz,'2000-01-01'::timestamptz,'-1m'::interval), 'Device 1';

ANALYZE sortopt_test;

