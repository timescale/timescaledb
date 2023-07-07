-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

--old chunks
COPY "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
1257894000000000000,dev3,1.5,2
\.
\copy "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
1257894000000000000,dev3,1.5,2
\.

--new chunks
COPY "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
2257894000000000000,dev3,1.5,2
\.
\copy "two_Partitions"("timeCustom", device_id, series_0, series_1) FROM STDIN DELIMITER ',';
2257894000000000000,dev3,1.5,2
\.

COPY (SELECT * FROM "two_Partitions" ORDER BY "timeCustom", device_id, series_0, series_1) TO STDOUT;


---test hypertable with FK
CREATE TABLE "meta" ("id" serial PRIMARY KEY);
CREATE TABLE "hyper" (
    "meta_id" integer NOT NULL REFERENCES meta(id),
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
SELECT create_hypertable('hyper', 'time', chunk_time_interval => 100);

INSERT INTO "meta" ("id") values (1);
\copy hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
1,1,1
\.

COPY hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
2,1,1
\.

\set ON_ERROR_STOP 0
\copy hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
1,2,1
\.
COPY hyper (time, meta_id, value) FROM STDIN DELIMITER ',';
2,2,1
\.
\set ON_ERROR_STOP 1

COPY (SELECT * FROM hyper ORDER BY time, meta_id) TO STDOUT;

--test that copy works with a low setting for max_open_chunks_per_insert
set timescaledb.max_open_chunks_per_insert = 1;
CREATE TABLE "hyper2" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
SELECT create_hypertable('hyper2', 'time', chunk_time_interval => 10);
\copy hyper2 from data/copy_data.csv with csv header ;

-- test copy with blocking trigger
CREATE FUNCTION gt_10() RETURNS trigger AS
$func$
BEGIN
    IF NEW."time" < 11
        THEN RETURN NULL;
    END IF;
    RETURN NEW;
END
$func$ LANGUAGE plpgsql;

CREATE TABLE "trigger_test" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
SELECT create_hypertable('trigger_test', 'time', chunk_time_interval => 10);
CREATE TRIGGER check_time BEFORE INSERT ON trigger_test
FOR EACH ROW EXECUTE FUNCTION gt_10();

\copy trigger_test from data/copy_data.csv with csv header ;
SELECT * FROM trigger_test ORDER BY time;

-- Test that if we copy from stdin to a hypertable and violate a null
-- constraint, it does not crash and generate an appropriate error
-- message.
CREATE TABLE test(a INT NOT NULL, b TIMESTAMPTZ);
SELECT create_hypertable('test', 'b');
\set ON_ERROR_STOP 0
COPY TEST (a,b) FROM STDIN (delimiter ',', null 'N');
N,'2020-01-01'
\.
\set ON_ERROR_STOP 1

----------------------------------------------------------------
-- Testing COPY TO.
----------------------------------------------------------------

\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages TO NOTICE;

-- COPY TO using a hypertable will not copy any tuples, but should
-- show a notice.
COPY hyper TO STDOUT DELIMITER ',';

-- COPY TO using a query should display all the tuples and not show a
-- notice.
COPY (SELECT * FROM hyper) TO STDOUT DELIMITER ',';

----------------------------------------------------------------
-- Testing multi-buffer optimization.
----------------------------------------------------------------

CREATE TABLE "hyper_copy" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('hyper_copy', 'time', chunk_time_interval => 2);

-- First copy call with default client_min_messages, to get rid of the
-- building index "_hyper_XXX_chunk_hyper_copy_time_idx" on table "_hyper_XXX_chunk" serially
-- messages
\copy hyper_copy FROM data/copy_data.csv WITH csv header;

SET client_min_messages TO DEBUG1;
\copy hyper_copy FROM data/copy_data.csv WITH csv header;

SELECT count(*) FROM hyper_copy;

-- Limit number of open chunks

SET timescaledb.max_open_chunks_per_insert = 1;

\copy hyper_copy FROM data/copy_data.csv WITH csv header;

SELECT count(*) FROM hyper_copy;

-- Before trigger disable the multi-buffer optimization

CREATE OR REPLACE FUNCTION empty_test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$;

-- Before trigger (CIM_SINGLE should be used)
CREATE TRIGGER hyper_copy_trigger_insert_before
    BEFORE INSERT ON hyper_copy
    FOR EACH ROW EXECUTE FUNCTION empty_test_trigger();

\copy hyper_copy FROM data/copy_data.csv WITH csv header;

SELECT count(*) FROM hyper_copy;

-- Suppress 'DEBUG:  EventTriggerInvoke XXXX' messages
RESET client_min_messages;
DROP TRIGGER hyper_copy_trigger_insert_before ON hyper_copy;
SET client_min_messages TO DEBUG1;

-- After trigger (CIM_MULTI_CONDITIONAL should be used)
CREATE TRIGGER hyper_copy_trigger_insert_after
    AFTER INSERT ON hyper_copy
    FOR EACH ROW EXECUTE FUNCTION empty_test_trigger();

\copy hyper_copy FROM data/copy_data.csv WITH csv header;

SELECT count(*) FROM hyper_copy;

-- Insert data into the chunks in random order
COPY hyper_copy FROM STDIN DELIMITER ',' NULL AS 'null';
5,1
7,1
1,1
0,5
15,3
0,7
17,2
20,1
5,6
19,1
18,2
17,1
16,1
15,1
14,1
13,1
12,1
11,1
10,1
11,1
12,2
13,2
14,2
15,2
16,2
17,2
18,2
19,2
20,2
\.

SELECT count(*) FROM hyper_copy;

RESET client_min_messages;
RESET timescaledb.max_open_chunks_per_insert;

----------------------------------------------------------------
-- Testing multi-buffer optimization
-- (no index on destination hypertable).
----------------------------------------------------------------

CREATE TABLE "hyper_copy_noindex" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('hyper_copy_noindex', 'time', chunk_time_interval => 10, create_default_indexes => false);

-- No trigger
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;

SET client_min_messages TO DEBUG1;
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;
RESET client_min_messages;

SELECT count(*) FROM hyper_copy_noindex;

-- Before trigger (CIM_SINGLE should be used)
CREATE TRIGGER hyper_copy_trigger_insert_before
    BEFORE INSERT ON hyper_copy_noindex
    FOR EACH ROW EXECUTE FUNCTION empty_test_trigger();
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;

SET client_min_messages TO DEBUG1;
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;
RESET client_min_messages;

SELECT count(*) FROM hyper_copy_noindex;

-- After trigger (CIM_MULTI_CONDITIONAL should be used)
DROP TRIGGER hyper_copy_trigger_insert_before ON hyper_copy_noindex;
CREATE TRIGGER hyper_copy_trigger_insert_after
    AFTER INSERT ON hyper_copy_noindex
    FOR EACH ROW EXECUTE FUNCTION empty_test_trigger();
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;

SET client_min_messages TO DEBUG1;
\copy hyper_copy_noindex FROM data/copy_data.csv WITH csv header;
RESET client_min_messages;

SELECT count(*) FROM hyper_copy_noindex;

----------------------------------------------------------------
-- Testing multi-buffer optimization
-- (more chunks than MAX_PARTITION_BUFFERS).
----------------------------------------------------------------

CREATE TABLE "hyper_copy_large" (
    "time" timestamp NOT NULL,
    "value" double precision NOT NULL
);

-- Genate data that will create more than 32 (MAX_PARTITION_BUFFERS)
-- chunks on the 10 second chunk_time_interval partitioned hypertable.
INSERT INTO hyper_copy_large
SELECT time,
random() AS value
FROM
generate_series('2022-01-01', '2022-01-31', INTERVAL '1 hour') AS g1(time)
ORDER BY time;

SELECT COUNT(*) FROM hyper_copy_large;

-- Migrate data to chunks by using copy
SELECT create_hypertable('hyper_copy_large', 'time',
   chunk_time_interval => INTERVAL '1 hour', migrate_data => 'true');

SELECT COUNT(*) FROM hyper_copy_large;

----------------------------------------------------------------
-- Testing multi-buffer optimization
-- (triggers on chunks).
----------------------------------------------------------------

CREATE TABLE "table_with_chunk_trigger" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);

-- This trigger counts the already inserted tuples in
-- the table table_with_chunk_trigger.
CREATE OR REPLACE FUNCTION count_test_chunk_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT count(*) FROM table_with_chunk_trigger INTO cnt;
    RAISE WARNING 'Trigger counted % tuples in table table_with_chunk_trigger', cnt;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$;

-- Create hypertable and chunks
SELECT create_hypertable('table_with_chunk_trigger', 'time', chunk_time_interval => 1);

-- Insert data to create all missing chunks
\copy table_with_chunk_trigger from data/copy_data.csv with csv header;
SELECT count(*) FROM table_with_chunk_trigger;

-- Chunk 1: 1-2, Chunk 2: 2-3, Chunk 3: 3-4, Chunk 4: 4-5
SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks
    WHERE hypertable_name = 'table_with_chunk_trigger' AND range_end_integer=5 \gset

-- Create before trigger on the 4th chunk
CREATE TRIGGER table_with_chunk_trigger_before_trigger
    BEFORE INSERT ON :chunk_schema.:chunk_name
    FOR EACH ROW EXECUTE FUNCTION count_test_chunk_trigger();

-- Insert data
-- 25 tuples are already imported. The trigger is executed before tuples
-- are copied into the 4th chunk. So, the trigger should report 25+3 = 28
-- This test requires that the multi-insert buffers of the other chunks
-- are flushed before the trigger is executed.
SET client_min_messages TO DEBUG1;
\copy table_with_chunk_trigger FROM data/copy_data.csv WITH csv header;
RESET client_min_messages;

SELECT count(*) FROM table_with_chunk_trigger;
DROP TRIGGER table_with_chunk_trigger_before_trigger ON :chunk_schema.:chunk_name;

-- Create after trigger
CREATE TRIGGER table_with_chunk_trigger_after_trigger
    AFTER INSERT ON :chunk_schema.:chunk_name
    FOR EACH ROW EXECUTE FUNCTION count_test_chunk_trigger();

-- Insert data
-- 50 tuples are already imported. The trigger is executed after all
-- tuples are imported. So, the trigger should report 50+25 = 75
SET client_min_messages TO DEBUG1;
\copy table_with_chunk_trigger FROM data/copy_data.csv WITH csv header;
RESET client_min_messages;

SELECT count(*) FROM table_with_chunk_trigger;

-- Hypertable with after row trigger and no index
DROP TABLE table_with_chunk_trigger;
CREATE TABLE "table_with_chunk_trigger" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);

-- Create hypertable and chunks
SELECT create_hypertable('table_with_chunk_trigger', 'time', chunk_time_interval => 1, create_default_indexes => false);

-- Insert data to create all missing chunks
\copy table_with_chunk_trigger from data/copy_data.csv with csv header;
SELECT count(*) FROM table_with_chunk_trigger;

-- Chunk 1: 1-2, Chunk 2: 2-3, Chunk 3: 3-4, Chunk 4: 4-5
SELECT chunk_schema, chunk_name FROM timescaledb_information.chunks
    WHERE hypertable_name = 'table_with_chunk_trigger' AND range_end_integer=5 \gset

-- Create after trigger
CREATE TRIGGER table_with_chunk_trigger_after_trigger
    AFTER INSERT ON :chunk_schema.:chunk_name
    FOR EACH ROW EXECUTE FUNCTION count_test_chunk_trigger();

\copy table_with_chunk_trigger from data/copy_data.csv with csv header;
SELECT count(*) FROM table_with_chunk_trigger;

----------------------------------------------------------------
-- Testing multi-buffer optimization
-- (Hypertable without before insert trigger)
----------------------------------------------------------------
CREATE TABLE "table_without_bf_trigger" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('table_without_bf_trigger', 'time', chunk_time_interval => 1);

-- Drop the default insert block trigger
DROP TRIGGER ts_insert_blocker ON table_without_bf_trigger;

\copy table_without_bf_trigger from data/copy_data.csv with csv header;

SET client_min_messages TO DEBUG1;
\copy table_without_bf_trigger from data/copy_data.csv with csv header;
RESET client_min_messages;

SELECT count(*) FROM table_without_bf_trigger;

-- After trigger (CIM_MULTI_CONDITIONAL should be used)
CREATE TRIGGER table_with_chunk_trigger_after_trigger
    AFTER INSERT ON table_without_bf_trigger
    FOR EACH ROW EXECUTE FUNCTION empty_test_trigger();

SET client_min_messages TO DEBUG1;
\copy table_without_bf_trigger from data/copy_data.csv with csv header;
RESET client_min_messages;

SELECT count(*) FROM table_without_bf_trigger;

----------------------------------------------------------------
-- Testing multi-buffer optimization
-- (Chunks with different layouts)
----------------------------------------------------------------
-- Time is not the first attribute of the hypertable

CREATE TABLE "table_with_layout_change" (
    "value1" real NOT NULL DEFAULT 1,
    "value2" smallint DEFAULT NULL,
    "value3" bigint DEFAULT NULL,
    "time" bigint NOT NULL,
    "value4" double precision NOT NULL DEFAULT 4,
    "value5" double precision NOT NULL DEFAULT 5
);

SELECT create_hypertable('table_with_layout_change', 'time', chunk_time_interval => 1);

-- Chunk 1 (time = 1)
COPY table_with_layout_change FROM STDIN DELIMITER ',' NULL AS 'null';
100,200,300,1,400,500
\.

SELECT * FROM table_with_layout_change;

-- Drop the first attribute
ALTER TABLE table_with_layout_change DROP COLUMN value1;
SELECT * FROM table_with_layout_change;

-- COPY into existing chunk (time = 1)
COPY table_with_layout_change FROM STDIN DELIMITER ',' NULL AS 'null';
201,301,1,401,501
\.

-- Create new chunk (time = 2)
COPY table_with_layout_change FROM STDIN DELIMITER ',' NULL AS 'null';
202,302,2,402,502
\.

SELECT * FROM table_with_layout_change ORDER BY time, value2, value3, value4, value5;

-- Create new chunk (time = 2), insert in different order
COPY table_with_layout_change (time, value5, value4, value3, value2) FROM STDIN DELIMITER ',' NULL AS 'null';
2,503,403,303,203
\.

COPY table_with_layout_change (value5, value4, value3, value2, time) FROM STDIN DELIMITER ',' NULL AS 'null';
504,404,304,204,2
\.

COPY table_with_layout_change (value5, value4, value3, time, value2) FROM STDIN DELIMITER ',' NULL AS 'null';
505,405,305,2,205
\.

SELECT * FROM table_with_layout_change ORDER BY time, value2, value3, value4, value5;

-- Drop the last attribute and add a new one
ALTER TABLE table_with_layout_change DROP COLUMN value5;
ALTER TABLE table_with_layout_change ADD COLUMN value6 double precision NOT NULL default 600;

SELECT * FROM table_with_layout_change ORDER BY time, value2, value3, value4, value6;

-- COPY in first chunk (time = 1)
COPY table_with_layout_change (time, value2, value3, value4, value6) FROM STDIN DELIMITER ',' NULL AS 'null';
1,206,306,406,606
\.

-- COPY in second chunk (time = 2)
COPY table_with_layout_change (time, value2, value3, value4, value6) FROM STDIN DELIMITER ',' NULL AS 'null';
2,207,307,407,607
\.

-- COPY in new chunk (time = 3)
COPY table_with_layout_change (time, value2, value3, value4, value6) FROM STDIN DELIMITER ',' NULL AS 'null';
3,208,308,408,608
\.

-- COPY in all chunks, different attribute order
COPY table_with_layout_change (value3, value4, time, value6, value2) FROM STDIN DELIMITER ',' NULL AS 'null';
309,409,3,609,209
310,410,2,610,210
311,411,1,611,211
\.

SELECT * FROM table_with_layout_change ORDER BY time, value2, value3, value4, value6;

-- Drop first column
ALTER TABLE table_with_layout_change DROP COLUMN value2;
SELECT * FROM table_with_layout_change ORDER BY time, value3, value4, value6;

-- COPY in all exiting chunks and create a new one (time 4)
COPY table_with_layout_change (value3, value4, time, value6) FROM STDIN DELIMITER ',' NULL AS 'null';
312,412,3,612
313,413,2,613
314,414,4,614
315,415,1,615
\.

SELECT * FROM table_with_layout_change ORDER BY time, value3, value4, value6;

-- Drop the last two columns
ALTER TABLE table_with_layout_change DROP COLUMN value4;
ALTER TABLE table_with_layout_change DROP COLUMN value6;

-- COPY in all exiting chunks and create a new one (time 5)
COPY table_with_layout_change (value3, time) FROM STDIN DELIMITER ',' NULL AS 'null';
316,2
317,1
318,3
319,5
320,4
\.

SELECT * FROM table_with_layout_change ORDER BY time, value3;

-- Drop the last of the initial attributes and add a new one
ALTER TABLE table_with_layout_change DROP COLUMN value3;
ALTER TABLE table_with_layout_change ADD COLUMN value7 double precision NOT NULL default 700;

-- COPY in all exiting chunks and create a new one (time 6)
COPY table_with_layout_change (value7, time) FROM STDIN DELIMITER ',' NULL AS 'null';
721,2
722,1
723,3
724,5
725,6
726,4
\.

SELECT * FROM table_with_layout_change ORDER BY time, value7;

