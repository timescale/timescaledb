-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

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

BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- Show chunk indexes
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_2_chunk');

\set ON_ERROR_STOP 0
-- cannot reorder a if with no index of the first call
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk', verbose => TRUE);
\set ON_ERROR_STOP 1

BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder a chunk directly with an chunk index
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', TRUE);
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder a chunk without an index
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk', verbose => TRUE);
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder a chunk directly with a hypertable index
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk', 'cluster_test_time_idx', TRUE);
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

\set ON_ERROR_STOP 0
-- other chunks in the hypertable are still not clustered
SELECT reorder_chunk('_timescaledb_internal._hyper_1_1_chunk', verbose => TRUE);

-- cannot cluster using another chunk's index
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_1_chunk_cluster_test_time_idx', TRUE);

-- cannot reorder a hypertable
SELECT reorder_chunk('cluster_test', 'cluster_test_time_idx', TRUE);

-- cannot reorder NULL
SELECT reorder_chunk(NULL, verbose => TRUE);

-- cannot reorder within a transaction
BEGIN;
SELECT reorder_chunk('_timescaledb_internal._hyper_1_2_chunk',  verbose => TRUE);
END;

\set ON_ERROR_STOP 1

BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder puts things in the correct order
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY ctid;
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY ctid;

-- can perform first reorder using a hypertable index (also test non-toast)
SELECT reorder_chunk('_timescaledb_internal._hyper_1_1_chunk', 'cluster_test_time_idx', verbose => TRUE);
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- and this sets the clustered index correctly
SELECT reorder_chunk('_timescaledb_internal._hyper_1_1_chunk', verbose => TRUE);
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder puts things in the correct order
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY ctid;
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY ctid;

-- can still use other index
BEGIN;
SET LOCAL enable_seqscan=false;
SET LOCAL enable_indexscan=true;
SET LOCAL enable_bitmapscan=false;

EXPLAIN (costs off) SELECT * FROM cluster_test WHERE location < 6;

SELECT time, temp, location, substring(value for 30), length(value)
    FROM cluster_test
    where location < 6
    ORDER BY time;
COMMIT;

-- drop toast column
ALTER TABLE cluster_test DROP COLUMN value;
ALTER TABLE expected DROP COLUMN value;

BEGIN;
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

SELECT reorder_chunk('_timescaledb_internal._hyper_1_1_chunk', verbose => TRUE);
BEGIN;
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- reorder puts things in the correct order
SELECT ctid, time, temp, location
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY ctid;
SELECT ctid, time, temp, location
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY ctid;

-- force an indexscan
SET enable_seqscan=false;
SET enable_indexscan=true;
SET enable_bitmapscan=false;
SELECT reorder_chunk('_timescaledb_internal._hyper_1_1_chunk', verbose => TRUE);

BEGIN;
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work_no_val('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;
SET enable_seqscan=Default;
SET enable_indexscan=Default;
SET enable_bitmapscan=Default;

-- reorder puts things in the correct order
SELECT ctid, time, temp, location
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY time;
SELECT ctid, time, temp, location
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY time;


CREATE TABLE ct2(time INTEGER, val BIGINT);
SELECT create_hypertable('ct2', 'time', chunk_time_interval => 5);

INSERT INTO ct2 VALUES
    (10,  0),
    ( 2, 12),
    ( 0, 10),
    (14,  4),
    (11,  1),
    (12,  2),
    ( 3, 13),
    ( 1, 11),
    ( 4, 14),
    (13, 13);
select show_chunks('ct2');

-- if we use someone elses index error
\set ON_ERROR_STOP 0
SELECT reorder_chunk('_timescaledb_internal._hyper_2_3_chunk', 'cluster_test_time_idx', verbose => TRUE);
SELECT reorder_chunk('_timescaledb_internal._hyper_2_3_chunk', '_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose => TRUE);
\set ON_ERROR_STOP 1

-- if the hypertable has a CLUSTERed index we can use it
CLUSTER ct2 USING ct2_time_idx;
SELECT reorder_chunk('_timescaledb_internal._hyper_2_3_chunk', verbose => TRUE);

-- deleted chunks are removed correctly
DELETE FROM ct2 where time < 2 OR val < 2;

SELECT reorder_chunk('_timescaledb_internal._hyper_2_3_chunk', verbose => TRUE);
SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_3_chunk ORDER BY time;
SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_4_chunk ORDER BY time;

-- There's a special case when a tuple is deleted, but that deletion isn't committed
-- But we disallow reorder within transactions for now, if we enable it, we should
-- enable the test
-- BEGIN;
-- DELETE FROM ct2 where time = 2 OR val = 2;
-- SELECT reorder_chunk('_timescaledb_internal._hyper_2_4_chunk', verbose => TRUE);
-- COMMIT;

-- SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_3_chunk ORDER BY time;
-- SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_4_chunk ORDER BY time;


-- There's a special case when a tuple is inserted earlier in the same txn
-- NOTE: this reorder vaccums the one deleted in the previous test
-- But we disallow reorder within transactions for now, if we enable it, we should
-- enable the test
-- BEGIN;
-- INSERT INTO ct2 VALUES (12,  2), ( 2, 12);
-- SELECT reorder_chunk('_timescaledb_internal._hyper_2_4_chunk', verbose => TRUE);
-- COMMIT;

-- SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_3_chunk ORDER BY time;
-- SELECT ctid, time, val FROM _timescaledb_internal._hyper_2_4_chunk ORDER BY time;

-- we do not allow reordering tables with OIDs
CREATE table coids (time INTEGER) WITH (OIDS);
SELECT create_hypertable('coids', 'time', chunk_time_interval => 5);
INSERT INTO coids VALUES (1);
\set ON_ERROR_STOP 0
SELECT reorder_chunk('_timescaledb_internal._hyper_3_5_chunk', 'coids_time_idx', verbose => TRUE);
\set ON_ERROR_STOP 1

SELECT indexrelid::regclass, indisclustered
FROM pg_index
WHERE indisclustered = true ORDER BY 1;
