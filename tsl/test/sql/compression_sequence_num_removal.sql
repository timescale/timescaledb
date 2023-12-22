-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test query planning with hypertable which contains
-- compressed chunks that depend on sequence number optimization
-- which is removed in latest schema revision
\set EXPLAIN 'EXPLAIN (VERBOSE, COSTS OFF)'

CREATE TABLE hyper(
    time INT NOT NULL,
    device_id INT,
    val INT);
SELECT * FROM create_hypertable('hyper', 'time', chunk_time_interval => 10);

-- test case with segmentby
ALTER TABLE hyper SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');
INSERT INTO hyper VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (10, 3, 2), (11, 4, 2), (11, 5, 2), (21, 2, 3), (22, 3, 3), (23, 4, 3), (30, 1, 4), (31, 3, 4), (31, 5, 4);

SELECT compress_chunk(show_chunks('hyper'));
:EXPLAIN SELECT * FROM hyper
ORDER BY device_id, time;

-- modify two chunks by adding sequence number to the segments
-- and rebuild the index based on that column
SELECT comp_ch.table_name AS "CHUNK_NAME", comp_ch.schema_name|| '.' || comp_ch.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk comp_ch, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset
SELECT schemaname || '.' || indexname AS "CHUNK_INDEX" FROM pg_indexes where tablename = :'CHUNK_NAME'
LIMIT 1 \gset

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SET timescaledb.restoring TO ON;
-- add sequence number column and fill in the correct sequences
ALTER TABLE :CHUNK_FULL_NAME ADD COLUMN _ts_meta_sequence_num int;
INSERT INTO :CHUNK_FULL_NAME (device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_sequence_num)
SELECT device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 , 10*row_number() over (partition by device_id) as _ts_meta_sequence_num
FROM :CHUNK_FULL_NAME
ORDER BY device_id, _ts_meta_min_1, _ts_meta_max_1;
DELETE FROM :CHUNK_FULL_NAME WHERE _ts_meta_sequence_num IS NULL;
-- drop exising index
DROP INDEX :CHUNK_INDEX;
-- create index based on sequence numbers
CREATE INDEX ON :CHUNK_FULL_NAME (device_id, _ts_meta_sequence_num);
SET timescaledb.restoring TO OFF;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT comp_ch.table_name AS "CHUNK_NAME", comp_ch.schema_name|| '.' || comp_ch.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk comp_ch, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id OFFSET 2 LIMIT 1 \gset
SELECT schemaname || '.' || indexname AS "CHUNK_INDEX" FROM pg_indexes where tablename = :'CHUNK_NAME'
LIMIT 1 \gset

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SET timescaledb.restoring TO ON;
-- add sequence number column and fill in the correct sequences
ALTER TABLE :CHUNK_FULL_NAME ADD COLUMN _ts_meta_sequence_num int;
INSERT INTO :CHUNK_FULL_NAME (device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_sequence_num)
SELECT device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 , 10*row_number() over (partition by device_id) as _ts_meta_sequence_num
FROM :CHUNK_FULL_NAME
ORDER BY device_id, _ts_meta_min_1, _ts_meta_max_1;
DELETE FROM :CHUNK_FULL_NAME WHERE _ts_meta_sequence_num IS NULL;
-- drop exising index
DROP INDEX :CHUNK_INDEX;
-- create index based on sequence numbers
CREATE INDEX ON :CHUNK_FULL_NAME (device_id, _ts_meta_sequence_num);
SET timescaledb.restoring TO OFF;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- from this point, two chunks should use sequence numbers and the rest will work without them
:EXPLAIN SELECT * FROM hyper
ORDER BY device_id, time;

-- test recompression which should do a full recompress of the chunk
-- while dropping sequence numbers
INSERT INTO hyper VALUES (20, 1, 1);
SELECT compress_chunk(show_chunks('hyper'));
-- removal of sequence numbers from the chunk should be
-- reflected in this plan
:EXPLAIN SELECT * FROM hyper
ORDER BY device_id, time;

-- test case without segmentby
TRUNCATE hyper;
ALTER TABLE hyper SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = '');
INSERT INTO hyper VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (10, 3, 2), (11, 4, 2), (11, 5, 2), (21, 2, 3), (22, 3, 3), (23, 4, 3), (30, 1, 4), (31, 3, 4), (31, 5, 4);

SELECT compress_chunk(show_chunks('hyper'));

:EXPLAIN SELECT * FROM hyper
ORDER BY time;

-- modify two chunks by adding sequence number to the segments
SELECT comp_ch.table_name AS "CHUNK_NAME", comp_ch.schema_name|| '.' || comp_ch.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk comp_ch, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SET timescaledb.restoring TO ON;
-- add sequence number column and fill in the correct sequences
ALTER TABLE :CHUNK_FULL_NAME ADD COLUMN _ts_meta_sequence_num int;
INSERT INTO :CHUNK_FULL_NAME (device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_sequence_num)
SELECT device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 , 10*row_number() over () as _ts_meta_sequence_num
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_max_1;
DELETE FROM :CHUNK_FULL_NAME WHERE _ts_meta_sequence_num IS NULL;
SET timescaledb.restoring TO OFF;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT comp_ch.table_name AS "CHUNK_NAME", comp_ch.schema_name|| '.' || comp_ch.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk comp_ch, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id OFFSET 3 LIMIT 1 \gset

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SET timescaledb.restoring TO ON;
-- add sequence number column and fill in the correct sequences
ALTER TABLE :CHUNK_FULL_NAME ADD COLUMN _ts_meta_sequence_num int;
INSERT INTO :CHUNK_FULL_NAME (device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_sequence_num)
SELECT device_id, time, val, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 , 10*row_number() over () as _ts_meta_sequence_num
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_max_1;
DELETE FROM :CHUNK_FULL_NAME WHERE _ts_meta_sequence_num IS NULL;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- from this point, two chunks should use sequence numbers and the rest will work without them
:EXPLAIN SELECT * FROM hyper
ORDER BY time;


-- test rolling up chunks during compression
-- this will not work with chunks which have sequence numbers
-- since the last chunk has sequence numbers, new chunks will be only rolled up together

ALTER TABLE hyper SET (timescaledb.compress_chunk_time_interval = '30');
INSERT INTO hyper VALUES (41, 1, 1), (42, 2, 1), (43, 3, 1), (50, 3, 2), (51, 4, 2), (51, 5, 2);

-- compression should leave the previous 4 chunks alone
-- and roll up the two new chunks into a single chunk
SELECT compress_chunk(show_chunks('hyper'));

