-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE cluster_test(time INTEGER, temp float, location int, value TEXT);

SELECT create_hypertable('cluster_test', 'time', chunk_time_interval => 5);

CREATE INDEX on cluster_test (location);

CREATE OR REPLACE FUNCTION ensure_scans_work(table_name TEXT, should_output REGCLASS=NULL, seqscan BOOLEAN=FALSE, indexscan BOOLEAN=FALSE, bitmapscan BOOLEAN=FALSE) RETURNS TABLE (a TEXT, b TEXT)
LANGUAGE SQL STABLE AS
$BODY$
    SELECT format($INNER$
            SET LOCAL enable_seqscan=%1$s;
            SET LOCAL enable_indexscan=%2$s;
            SET LOCAL enable_bitmapscan=%3$s;
            EXPLAIN (costs off) SELECT * FROM %4$I WHERE time=7
        $INNER$, seqscan, indexscan, bitmapscan, table_name) as a,
        format($INNER$
            WITH T1 as (SELECT * FROM %1$I WHERE time=7),
                 T2 as (SELECT * FROM %2$I WHERE time=7)
            SELECT T1.time, T2.time, T1.temp, T2.temp, T1.location, T2.location,
                   substring(T1.value for 30), substring(T2.value for 30),
                   length(T1.value), length(T2.value)
            FROM T1 FULL OUTER JOIN T2 ON T1 = T2
            WHERE T1 IS NULL OR T2 IS NULL
        $INNER$, table_name, should_output) as b;
$BODY$;

CREATE OR REPLACE FUNCTION ensure_scans_work_no_val(table_name TEXT, should_output REGCLASS=NULL, seqscan BOOLEAN=FALSE, indexscan BOOLEAN=FALSE, bitmapscan BOOLEAN=FALSE) RETURNS TABLE (a TEXT, b TEXT)
LANGUAGE SQL STABLE AS
$BODY$
    SELECT format($INNER$
            SET LOCAL enable_seqscan=%1$s;
            SET LOCAL enable_indexscan=%2$s;
            SET LOCAL enable_bitmapscan=%3$s;
            EXPLAIN (costs off) SELECT * FROM %4$I WHERE time=6
        $INNER$, seqscan, indexscan, bitmapscan, table_name) as a,
        format($INNER$
            WITH T1 as (SELECT * FROM %1$I WHERE time=7),
                 T2 as (SELECT * FROM %2$I WHERE time=7)
            SELECT T1.time, T2.time, T1.temp, T2.temp, T1.location, T2.location
            FROM T1 FULL OUTER JOIN T2 ON T1 = T2
            WHERE T1 IS NULL OR T2 IS NULL
        $INNER$, table_name, should_output) as b;
$BODY$;

-- Show default indexes
SELECT * FROM test.show_indexes('cluster_test');

-- Show clustered indexes
SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true ORDER BY 1;

-- Create two chunks
INSERT INTO cluster_test VALUES
    ( 0, 18.9, 3, 'a'),
    ( 8, 13.3, 7, 'b'),
    ( 3, 19.4, 3, 'c'),
    ( 6, 27.3, 9, 'd'),
    ( 5, 25.4, 1, 'e'),
    ( 4, 18.3, 2, 'f'),
    ( 2, 12.4, 8, 'g'),
    ( 7, 20.3, 4, repeat('xyzzy', 100000)), -- toasted value
    ( 9, 11.4, 1, 'h'),
    ( 1, 23.4, 5, 'i');

CREATE TABLE expected AS SELECT * FROM cluster_test;

-- original results to be compared against
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY ctid;
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY ctid;
