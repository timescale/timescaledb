-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT CREATE ON DATABASE :"TEST_DBNAME" TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE SCHEMA "ChunkSchema";
-- Use range types as well for columns
CREATE TABLE chunkapi(time timestamptz not null, device int, temp float, rng int8range);

SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4, int8range(4, 10));

SELECT (_timescaledb_functions.show_chunk(show_chunks)).*
FROM show_chunks('chunkapi')
ORDER BY chunk_id;

-- Creating a chunk with the constraints of an existing chunk should
-- return the existing chunk
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419200000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}');

\set VERBOSITY default
\set ON_ERROR_STOP 0
-- Modified time constraint should fail with collision
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Missing dimension
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000]}');
-- Extra dimension
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "device": [-9223372036854775808, 1073741823], "time2": [1514419600000000, 1515024000000000]}');
-- Bad dimension name
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "dev": [-9223372036854775808, 1073741823]}');
-- Same dimension twice
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000], "time": [1514419600000000, 1515024000000000]}');
-- Bad bounds value
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": ["-1514419200000000", 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Bad bounds value
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": ["badtimestamp", 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Bad slices format
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Bad slices json
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time: [1515024000000000] "device": [-9223372036854775808, 1073741823]}');
-- Bad bound type
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [true, 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
\set ON_ERROR_STOP 1

-- Test that granting insert on tables allow create_chunk to be
-- called. This will also create a chunk that does not collide and has
-- a custom schema and name.
SET ROLE :ROLE_SUPERUSER;
GRANT INSERT ON chunkapi TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', 'ChunkSchema', 'My_chunk_Table_name');

SET ROLE :ROLE_SUPERUSER;
DROP TABLE chunkapi;

-- Test creating a chunk from an existing chunk table which was not
-- created via create_chunk_table and having a different name.
CREATE TABLE devices (id int PRIMARY KEY);
INSERT INTO devices VALUES (1);
CREATE TABLE chunkapi (time timestamptz NOT NULL PRIMARY KEY, device int REFERENCES devices(id), temp float  CHECK(temp > 0));
SELECT * FROM create_hypertable('chunkapi', 'time');
INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4);
SELECT chunk_schema AS "CHUNK_SCHEMA", chunk_name AS "CHUNK_NAME"
FROM timescaledb_information.chunks c
ORDER BY chunk_name DESC
LIMIT 1 \gset

SELECT slices AS "SLICES"
FROM _timescaledb_functions.show_chunk(:'CHUNK_SCHEMA'||'.'||:'CHUNK_NAME') \gset

TRUNCATE chunkapi;

CREATE TABLE newchunk (time timestamptz NOT NULL, device int, temp float);
SELECT * FROM test.show_constraints('newchunk');

INSERT INTO newchunk VALUES ('2018-01-01 05:00:00-8', 1, 23.4);

\set ON_ERROR_STOP 0
-- Creating the chunk without required CHECK constraints on a table
-- should fail. Currently, PostgreSQL only enforces presence of CHECK
-- constraints, but not foreign key, unique, or primary key
-- constraints. We should probably add checks to enforce the latter
-- too or auto-create all constraints.
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME', 'newchunk');
\set ON_ERROR_STOP 1
-- Add the missing CHECK constraint. Note that the name must be the
-- same as on the parent table.
ALTER TABLE newchunk ADD CONSTRAINT chunkapi_temp_check CHECK (temp > 0);
CREATE TABLE newchunk2 as select * from newchunk;
ALTER TABLE newchunk2 ADD CONSTRAINT chunkapi_temp_check CHECK (temp > 0);
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME', 'newchunk');
-- adding an existing table to an exiting range must fail
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME', 'newchunk2');
\set ON_ERROR_STOP 1

-- Show the chunk and that names are what we'd expect
SELECT
	:'CHUNK_SCHEMA' AS expected_schema,
	:'CHUNK_NAME' AS expected_table_name,
	(_timescaledb_functions.show_chunk(ch)).*
FROM show_chunks('chunkapi') ch;

-- The chunk should inherit the hypertable
SELECT relname
FROM pg_catalog.pg_inherits, pg_class
WHERE inhrelid = (:'CHUNK_SCHEMA'||'.'||:'CHUNK_NAME')::regclass AND inhparent = oid;

-- Test that it is possible to query the data via the hypertable
SELECT * FROM chunkapi ORDER BY 1,2,3;

-- Show that the chunk has all the necessary constraints. These
-- include inheritable constraints and dimensional constraints, which
-- are specific to the chunk.  Currently, foreign key, unique, and
-- primary key constraints are not inherited or auto-created.
SELECT * FROM test.show_constraints(format('%I.%I', :'CHUNK_SCHEMA', :'CHUNK_NAME')::regclass);

TRUNCATE chunkapi;

-- Create a table with extra columns
CREATE TABLE extra_col_chunk (time timestamptz NOT NULL, device int, temp float, extra int, CONSTRAINT chunkapi_temp_check CHECK (temp > 0));

-- Adding a new chunk with extra column should fail
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1514419200000000, 1515024000000000]}', NULL, NULL, 'extra_col_chunk');
\set ON_ERROR_STOP 1

-- It should succeed after dropping the extra column
ALTER TABLE extra_col_chunk DROP COLUMN extra;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1514419200000000, 1515024000000000]}', NULL, NULL, 'extra_col_chunk');

-- Test creating a chunk with columns in different order
CREATE TABLE reordered_chunk (device int, temp float, time timestamptz NOT NULL, CONSTRAINT chunkapi_temp_check CHECK (temp > 0));

SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1515024000000000, 1515628800000000]}', NULL, NULL, 'reordered_chunk');

-- Test creating a chunk with initially missing a column
CREATE TABLE missing_col_chunk (time timestamptz NOT NULL, device int);
INSERT INTO missing_col_chunk (time, device) VALUES ('2018-01-11 05:00:00-8', 1);
ALTER TABLE missing_col_chunk ADD COLUMN temp float CONSTRAINT chunkapi_temp_check CHECK (temp > 0);
INSERT INTO missing_col_chunk (time, device, temp) VALUES ('2018-01-12 05:00:00-8', 1, 19.5);

-- This should succeed since all required columns are now present
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1515628800000000, 1516233600000000]}', NULL, NULL, 'missing_col_chunk');

-- Test creating a chunk with mismatched column types
CREATE TABLE wrong_type_chunk (time timestamptz NOT NULL, device text, temp float CONSTRAINT chunkapi_temp_check CHECK (temp > 0));

-- This should fail due to type mismatch
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1516233600000000, 1516838400000000]}', NULL, NULL, 'wrong_type_chunk');
\set ON_ERROR_STOP 1

-- Test creating a chunk with binary coercible types
ALTER TABLE chunkapi ADD COLUMN name varchar(10);
CREATE TABLE coercible_type_chunk (time timestamptz NOT NULL, device int, temp float CONSTRAINT chunkapi_temp_check CHECK (temp > 0), name text);
INSERT INTO coercible_type_chunk (time, device, temp, name) VALUES ('2018-01-20 06:00:00-8', 1, 25.1, 'device1');

\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1516233600000000, 1516838400000000]}', NULL, NULL, 'coercible_type_chunk');
\set ON_ERROR_STOP 1

ALTER TABLE chunkapi DROP COLUMN name;

-- Test data routing to the successfully created chunks. Each row should go to a different chunks
INSERT INTO chunkapi VALUES
    ('2018-01-01 05:00:00-8', 1, 23.4),
    ('2018-01-08 05:00:00-8', 1, 24.5),
    ('2018-01-15 05:00:00-8', 1, 25.6);

-- Verify data was routed correctly
SELECT
    'SELECT * FROM ' || chunk_schema || '.' || chunk_name
FROM timescaledb_information.chunks
WHERE hypertable_name = 'chunkapi'; \gexec

TRUNCATE chunkapi;

-- Test generated columns
-- This should fail since generated column is not allowed in chunks if parent does not have the column as generated.
CREATE TABLE generated_col_chunk (time timestamptz NOT NULL, device int, temp float GENERATED ALWAYS AS (device::float) STORED, CONSTRAINT chunkapi_temp_check CHECK (temp > 0));
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1516838400000000, 1517443200000000]}', NULL, NULL, 'generated_col_chunk');
\set ON_ERROR_STOP 1

-- When parent has a generated column while the chunk does not have the same column as generated, it should fail too.
CREATE TABLE non_generated_chunk (time timestamptz NOT NULL, device int, temp float, CONSTRAINT chunkapi_temp_check CHECK (temp > 0));
\set ON_ERROR_STOP 0
ALTER TABLE chunkapi DROP COLUMN temp;
ALTER TABLE chunkapi ADD COLUMN temp float GENERATED ALWAYS AS (device::float) STORED;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1517443200000000, 1518048000000000]}', NULL, NULL, 'non_generated_chunk');
\set ON_ERROR_STOP 1

-- Generation expression cannot be different from the parent
\set ON_ERROR_STOP 0
ALTER TABLE generated_col_chunk DROP COLUMN temp;
ALTER TABLE generated_col_chunk ADD COLUMN temp float GENERATED ALWAYS AS ((device::float)+1) STORED;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1518048000000000, 1518652800000000]}', NULL, NULL, 'generated_col_chunk');
\set ON_ERROR_STOP 1

-- This should succeed since the generated column is now the same as in the parent
ALTER TABLE generated_col_chunk DROP COLUMN temp;
ALTER TABLE generated_col_chunk ADD COLUMN temp float GENERATED ALWAYS AS ((device::float)) STORED;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1518652800000000, 1519257600000000]}', NULL, NULL, 'generated_col_chunk');

INSERT INTO chunkapi VALUES ('2018-02-20 05:00:00-8', 1);
SELECT
    'SELECT * FROM ' || chunk_schema || '.' || chunk_name
FROM timescaledb_information.chunks
WHERE hypertable_name = 'chunkapi'; \gexec

DROP TABLE chunkapi;

-- Test the fix for Bug #8577
-- Mismatch in attnum between chunk and hypertable should not fail
CREATE TABLE chunkapi(col_to_drop int, time timestamptz not null, device int);
SELECT create_hypertable('chunkapi', 'time', 'device', 2);
ALTER TABLE chunkapi DROP COLUMN col_to_drop;

CREATE TABLE new_chunk(time timestamptz not null, device int);
INSERT INTO new_chunk VALUES ('2018-01-01 05:00:00-8', 1);
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1514419200000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}', NULL, NULL, 'new_chunk');

CREATE TABLE reordered_chunk(device int, time timestamptz not null);
INSERT INTO reordered_chunk VALUES (1, '2018-01-08 05:00:00-8');
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1515024000000000, 1515628800000000], "device": [-9223372036854775808, 1073741823]}', NULL, NULL, 'reordered_chunk');

CREATE TABLE new_col_chunk(time timestamptz not null, temp float, device int);
INSERT INTO new_col_chunk VALUES ('2018-01-15 05:00:00-8', 23.4, 1);
ALTER TABLE chunkapi ADD COLUMN temp float;
SELECT * FROM _timescaledb_functions.create_chunk('chunkapi', '{"time": [1515628800000000, 1516233600000000], "device": [-9223372036854775808, 1073741823]}', NULL, NULL, 'new_col_chunk');

SELECT show_chunks('chunkapi') AS chunk LIMIT 1 \gset
BEGIN;
SELECT txid_current_if_assigned() IS NULL;
SELECT COUNT(*) FROM :chunk LIMIT 1;
SELECT txid_current_if_assigned() IS NULL;
COMMIT;
