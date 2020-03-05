-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains tests for functionality introduced in PG12

------- TEST 1: Restrictive copy from file
CREATE TABLE "copy_golden" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
\COPY copy_golden (time, value) FROM data/copy_data.csv WITH CSV HEADER
SELECT * FROM copy_golden ORDER BY TIME;

CREATE TABLE "copy_control" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
\COPY copy_control (time, value) FROM data/copy_data.csv WITH CSV HEADER WHERE time > 10;
SELECT * FROM copy_control ORDER BY TIME;

CREATE TABLE "copy_test" (
    "time" bigint NOT NULL,
    "value" double precision NOT NULL
);
SELECT create_hypertable('copy_test', 'time', chunk_time_interval => 10);
\COPY copy_test (time, value) FROM data/copy_data.csv WITH CSV HEADER WHERE time > 10;
SELECT * FROM copy_test ORDER BY TIME;

-- Verify attempting to use subqueries fails the same as non-hypertables
\set ON_ERROR_STOP 0
\COPY copy_control (time, value) FROM data/copy_data.csv WITH CSV HEADER WHERE time IN (SELECT time FROM copy_golden);
\COPY copy_test (time, value) FROM data/copy_data.csv WITH CSV HEADER WHERE time IN (SELECT time FROM copy_golden);
\set ON_ERROR_STOP 1

DROP TABLE copy_golden;
DROP TABLE copy_control;
DROP TABLE copy_test;
