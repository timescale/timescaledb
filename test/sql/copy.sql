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
FOR EACH ROW EXECUTE PROCEDURE gt_10();

\copy trigger_test from data/copy_data.csv with csv header ;
SELECT * FROM trigger_test ORDER BY time;

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
