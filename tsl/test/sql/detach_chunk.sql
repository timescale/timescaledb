-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT CREATE ON DATABASE :"TEST_DBNAME" TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create test tables and hypertables
CREATE TABLE devices(id int PRIMARY KEY);
INSERT INTO devices VALUES (1), (2), (3);

CREATE TABLE detach_test(id int, time timestamptz not null, device int, temp float);
CREATE INDEX detach_test_device_idx ON detach_test (device);
ALTER TABLE detach_test
    ADD CONSTRAINT detach_test_temp_check CHECK (temp > 0),
    ADD CONSTRAINT detach_test_device_fkey FOREIGN KEY (device) REFERENCES devices(id),
    ADD CONSTRAINT detach_test_id_time_unique UNIQUE (id, time);
SELECT * FROM create_hypertable('detach_test', 'time', 'id', 2);

CREATE TABLE detach_test_ref (
    id int PRIMARY KEY,
    ref_id int,
    ref_time timestamptz,
    FOREIGN KEY (ref_id, ref_time) REFERENCES detach_test(id, time)
);

INSERT INTO detach_test VALUES
    (1, '2025-06-01 05:00:00-8', 1, 23.4),
    (2, '2025-06-15 05:00:00-8', 2, 24.5),
    (3, '2025-06-30 05:00:00-8', 3, 25.6);

-- Get chunk information for testing
SELECT
    chunk_id AS "CHUNK_ID",
    hypertable_id AS "HYPERTABLE_ID",
    schema_name AS "CHUNK_SCHEMA",
    table_name AS "CHUNK_TABLE",
    schema_name || '.' || table_name AS "CHUNK_NAME"
FROM _timescaledb_functions.show_chunk((SELECT show_chunks('detach_test') LIMIT 1)); \gset

-- Detach by non-owner is not allowed
\set ON_ERROR_STOP 0
set role :ROLE_1;
CALL detach_chunk(:'CHUNK_NAME');
set role :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 1

SELECT count(*) AS "PREV_CHUNK_ROWS"
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = :'HYPERTABLE_ID'; \gset

-- Test successful detach
CALL detach_chunk(:'CHUNK_NAME');

-- Verify chunk is detached:
SELECT count(*) = :PREV_CHUNK_ROWS-1 FROM _timescaledb_catalog.chunk WHERE hypertable_id = :'HYPERTABLE_ID';
SELECT count(*) = 0 FROM pg_inherits WHERE inhrelid = :'CHUNK_NAME'::regclass;

-- Data should still be in the detached table
SELECT count(*) > 0 FROM :CHUNK_NAME;

-- Verify catalog cleanup
SELECT count(*) = 0 FROM _timescaledb_catalog.chunk_constraint WHERE chunk_id = :'CHUNK_ID';

SELECT count(*) = 0 FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.chunk_constraint cc ON cc.dimension_slice_id = ds.id
WHERE cc.chunk_id = :'CHUNK_ID';

SELECT count(*) = 0 FROM pg_constraint
WHERE contype = 'f' AND confrelid = :'CHUNK_NAME'::regclass::oid;

DROP TABLE detach_test_ref;

-- Verify new rows do not go to the detached chunk
SELECT count(*) AS "DETACHED_CHUNK_ROWS" FROM :CHUNK_NAME; \gset
INSERT INTO detach_test VALUES (4, '2025-06-01 09:00:00-8', 1, 50.0);
SELECT count(*) = :'DETACHED_CHUNK_ROWS' FROM :CHUNK_NAME;

-- Verify detach can rollback
SELECT count(*) AS "PREV_CHUNK_ROWS"
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = :'HYPERTABLE_ID'; \gset

BEGIN;
SELECT schema_name || '.' || table_name AS "ROLLBACK_CHUNK"
FROM _timescaledb_functions.show_chunk((SELECT * FROM show_chunks('detach_test') LIMIT 1)); \gset
CALL detach_chunk(:'ROLLBACK_CHUNK');
ROLLBACK;

SELECT count(*) = :PREV_CHUNK_ROWS
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = :'HYPERTABLE_ID';

-- Error cases
\set ON_ERROR_STOP 0
CALL detach_chunk(98765);
CALL detach_chunk(0);
CALL detach_chunk('detach_test');
CALL detach_chunk(NULL);
-- Detach a regular table
CREATE TABLE regular_table(time timestamptz, temp float);
CALL detach_chunk('regular_table');
-- Try to detach already detached chunk
CALL detach_chunk(:'CHUNK_NAME');
\set ON_ERROR_STOP 1

-- Compressed chunks
ALTER TABLE detach_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device'
);

SELECT schema_name || '.' || table_name AS "COMPRESSED_CHUNK"
FROM _timescaledb_functions.show_chunk((SELECT * FROM show_chunks('detach_test') LIMIT 1)); \gset
SELECT compress_chunk(:'COMPRESSED_CHUNK');

-- Try to detach compressed chunk (should fail)
\set ON_ERROR_STOP 0
CALL detach_chunk(:'COMPRESSED_CHUNK');
\set ON_ERROR_STOP 1

DROP TABLE :CHUNK_NAME;
DROP TABLE detach_test;
DROP TABLE devices CASCADE;
