-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT CREATE ON DATABASE :"TEST_DBNAME" TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create test tables and hypertables (reusing from detach_chunk test)
CREATE TABLE devices(id int PRIMARY KEY);
INSERT INTO devices VALUES (1), (2), (3);

CREATE TABLE attach_test(id int, time timestamptz not null, device int, temp float);
CREATE INDEX attach_test_device_idx ON attach_test (device);
ALTER TABLE attach_test
    ADD CONSTRAINT attach_test_temp_check CHECK (temp > 0),
    ADD CONSTRAINT attach_test_device_fkey FOREIGN KEY (device) REFERENCES devices(id),
    ADD CONSTRAINT attach_test_id_time_unique UNIQUE (id, time);
SELECT * FROM create_hypertable('attach_test', 'time', 'id', 2);

CREATE TABLE attach_test_ref (
    id int PRIMARY KEY,
    ref_id int,
    ref_time timestamptz,
    FOREIGN KEY (ref_id, ref_time) REFERENCES attach_test(id, time)
);

INSERT INTO attach_test VALUES
    (1, '2025-06-01 05:00:00+3', 1, 23.4),
    (2, '2025-06-15 05:00:00+3', 2, 24.5),
    (3, '2025-06-30 05:00:00+3', 3, 25.6);

-- Get chunk information for testing
SELECT
    chunk_id AS "CHUNK_ID",
    hypertable_id AS "HYPERTABLE_ID",
    schema_name AS "CHUNK_SCHEMA",
    table_name AS "CHUNK_TABLE",
    schema_name || '.' || table_name AS "CHUNK_NAME",
    slices AS "CHUNK_SLICES"
FROM _timescaledb_functions.show_chunk((SELECT show_chunks('attach_test') LIMIT 1)); \gset

-- Successful attachment
CREATE TABLE regular_table_to_attach(id int, time timestamptz not null, device int, temp float);
CREATE INDEX regular_table_device_idx ON regular_table_to_attach (device);
ALTER TABLE regular_table_to_attach
    ADD CONSTRAINT attach_test_temp_check CHECK (temp > 0),
    ADD CONSTRAINT pre_attach_temp_check CHECK (temp < 100),
    ADD CONSTRAINT pre_attach_id_time_unique UNIQUE (id, time);

INSERT INTO regular_table_to_attach VALUES (10, '2025-07-05 13:00:00+3', 2, 27.8);

-- Attach it as a chunk
CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Verify data is accessible through the hypertable
SELECT count(*) > 0 FROM attach_test WHERE time >= '2025-07-5'::timestamptz;

-- Check the chunk and related metadata are set up correctly
SELECT id AS "ATTACHED_CHUNK" FROM _timescaledb_catalog.chunk
WHERE hypertable_id = :'HYPERTABLE_ID' AND table_name = 'regular_table_to_attach'; \gset

-- Verify that each constraint/index on a auto-created chunk is also present on the attached chunk
SELECT hypertable_constraint_name FROM _timescaledb_catalog.chunk_constraint WHERE chunk_id = :'CHUNK_ID'
EXCEPT SELECT hypertable_constraint_name FROM _timescaledb_catalog.chunk_constraint WHERE chunk_id = :'ATTACHED_CHUNK';
SELECT * FROM test.show_indexesp('regular_table_to_attach');
SELECT * FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.chunk_constraint cc ON ds.id = cc.dimension_slice_id
WHERE cc.chunk_id = :'ATTACHED_CHUNK';

-- Verify that the chunk is a child of the hypertable
SELECT count(*) > 0 FROM pg_inherits
WHERE inhrelid = 'regular_table_to_attach'::regclass::oid AND inhparent = 'attach_test'::regclass::oid;

-- Verify foreign key references work
SELECT count(*) > 0 FROM pg_constraint
WHERE contype = 'f' AND confrelid = 'regular_table_to_attach'::regclass::oid;

SELECT count(*) > 0 FROM pg_constraint
WHERE contype = 'f' AND conrelid = 'regular_table_to_attach'::regclass::oid;

-- Verify data is routed to the correct chunk
SELECT count(*) FROM attach_test WHERE time > '2025-07-01'::timestamptz;
INSERT INTO attach_test VALUES (5, '2025-07-4 05:00:00+3', 2, 19.5);
SELECT count(*) FROM regular_table_to_attach;

-- Detach and re-attach a chunk
CALL detach_chunk(:'CHUNK_NAME');
CALL attach_chunk('attach_test', :'CHUNK_NAME', :'CHUNK_SLICES');
-- Store the new chunk id
SELECT chunk_id AS "CHUNK_ID" FROM _timescaledb_functions.show_chunk(:'CHUNK_NAME'); \gset

-- Verify it's re-attached
SELECT count(*) > 0 FROM pg_inherits WHERE inhrelid = :'CHUNK_NAME'::regclass::oid;

-- Verify constraints and indexes are restored
SELECT * FROM _timescaledb_catalog.chunk_constraint WHERE chunk_id = :'CHUNK_ID';
SELECT * FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.chunk_constraint cc ON ds.id = cc.dimension_slice_id
WHERE cc.chunk_id = :'CHUNK_ID';

-- Attach a chunk to another hypertable with a different dimension
CALL detach_chunk('regular_table_to_attach');

CREATE TABLE hypertable_with_different_dimension(id int, time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('hypertable_with_different_dimension', 'time');

CALL attach_chunk('hypertable_with_different_dimension', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"]}');
CALL detach_chunk('regular_table_to_attach');

CREATE TABLE not_a_hypertable(id int, time timestamptz not null, device int, temp float);

-- Error cases
\set ON_ERROR_STOP 0
CALL attach_chunk('attach_test', 'nonexistent_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
CALL attach_chunk('not_a_hypertable', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
CALL attach_chunk('nonexistent_hypertable', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
CALL attach_chunk(98765, 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
CALL attach_chunk(0, 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- invalid json format
CALL attach_chunk('attach_test', 'regular_table_to_attach', '"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]');
-- incorrect dimension information
CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025123-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "incorrect_key": [-9223372036854775808, 1073741823]}');

CALL attach_chunk('attach_test', 'regular_table_to_attach', NULL);
CALL attach_chunk('attach_test', 'regular_table_to_attach', :'CHUNK_SLICES');
CALL attach_chunk('attach_test', :'CHUNK_NAME', :'CHUNK_SLICES');

-- Attach a chunk of another hypertable
CALL attach_chunk('hypertable_with_different_dimension', :'CHUNK_NAME', :'CHUNK_SLICES');

-- Try to attach a table that's already a child of another table
CREATE TABLE parent_table(id int);
CREATE TABLE child_table(time timestamptz not null, device int, temp float) INHERITS (parent_table);
CALL attach_chunk('attach_test', 'child_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Try to attach a table with incompatible types
CREATE TABLE incompatible_table(id int, time timestamptz not null, device text, temp float);
CALL attach_chunk('attach_test', 'incompatible_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Try to attach a table with missing columns
CREATE TABLE missing_col_table(id int, time timestamptz not null);
CALL attach_chunk('attach_test', 'missing_col_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Try to attach a table with extra columns
CREATE TABLE extra_col_table(id int, time timestamptz not null, device int, temp float, extra_col int);
CALL attach_chunk('attach_test', 'extra_col_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Attach by non-owner is not allowed
set role :ROLE_1;
CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
set role :ROLE_DEFAULT_PERM_USER;

CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-06-01 05:00:00+3", "2025-06-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');

-- Attach hypertable as a chunk
CALL attach_chunk('attach_test', 'hypertable_with_different_dimension', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
\set ON_ERROR_STOP 1

-- Test rollback behavior
SELECT count(*) AS "PRE_ROLLBACK_CHUNKS" FROM _timescaledb_catalog.chunk WHERE hypertable_id = :'HYPERTABLE_ID'; \gset

BEGIN;
CREATE TABLE rollback_test_table(id int, time timestamptz not null, device int, temp float);
ALTER TABLE rollback_test_table ADD CONSTRAINT attach_test_temp_check CHECK (temp > 0);
CALL attach_chunk('attach_test', 'rollback_test_table', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
ROLLBACK;

SELECT count(*) = :'PRE_ROLLBACK_CHUNKS' FROM _timescaledb_catalog.chunk WHERE hypertable_id = :'HYPERTABLE_ID';

CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00", "2025-07-07 05:00:00"], "id": [-9223372036854775808, 1073741823]}');
CALL detach_chunk('regular_table_to_attach');

CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01", "2025-07-07"], "id": [-9223372036854775808, 1073741823]}');
CALL detach_chunk('regular_table_to_attach');

CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["Tue July 1 05:00:00 2025 GMT+3", "Mon July 7 05:00:00 2025 GMT+3"], "id": [-9223372036854775808, 1073741823]}');
CALL detach_chunk('regular_table_to_attach');

CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": [1751356800000000, 1751875200000000], "id": [-9223372036854775808, 1073741823]}');
CALL detach_chunk('regular_table_to_attach');

-- Attach a table with rows violating chunk constraints
INSERT INTO regular_table_to_attach VALUES (4, '2024-07-05 13:00:00+3', 2, 27.8);
\set ON_ERROR_STOP 0
CALL attach_chunk('attach_test', 'regular_table_to_attach', '{"time": ["2025-07-01 05:00:00+3", "2025-07-07 05:00:00+3"], "id": [-9223372036854775808, 1073741823]}');
\set ON_ERROR_STOP 1

-- Clean up
DROP TABLE regular_table_to_attach;
DROP TABLE attach_test_ref;
DROP TABLE attach_test;
DROP TABLE devices CASCADE;
