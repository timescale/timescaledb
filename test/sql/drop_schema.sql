-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA chunk_schema1;
CREATE SCHEMA chunk_schema2;
CREATE SCHEMA hypertable_schema;
CREATE SCHEMA extra_schema;
GRANT ALL ON SCHEMA hypertable_schema TO :ROLE_DEFAULT_PERM_USER;
GRANT ALL ON SCHEMA chunk_schema1 TO :ROLE_DEFAULT_PERM_USER;
GRANT ALL ON SCHEMA chunk_schema2 TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE hypertable_schema.test1 (time timestamptz, temp float, location int);
CREATE TABLE hypertable_schema.test2 (time timestamptz, temp float, location int);

--create two identical tables with their own chunk schemas
SELECT create_hypertable('hypertable_schema.test1', 'time', 'location', 2, associated_schema_name => 'chunk_schema1');
SELECT create_hypertable('hypertable_schema.test2', 'time', 'location', 2, associated_schema_name => 'chunk_schema2');
INSERT INTO hypertable_schema.test1 VALUES ('2001-01-01 01:01:01', 23.3, 1);
INSERT INTO hypertable_schema.test2 VALUES ('2001-01-01 01:01:01', 23.3, 1);

SELECT * FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk;

RESET ROLE;
--drop the associated schema. We drop the extra schema to show we can
--handle multi-schema drops
DROP SCHEMA chunk_schema1, extra_schema CASCADE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--show that the metadata for the table using the dropped schema is
--changed. The other table is not affected.
SELECT * FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk;

--new chunk should be created in the internal associated schema
INSERT INTO hypertable_schema.test1 VALUES ('2001-01-01 01:01:01', 23.3, 1);
SELECT * FROM _timescaledb_catalog.chunk;

RESET ROLE;
--dropping the internal schema should not work
\set ON_ERROR_STOP 0
DROP SCHEMA _timescaledb_internal CASCADE;
\set ON_ERROR_STOP 1
--dropping the hypertable schema should delete everything
DROP SCHEMA hypertable_schema CASCADE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--everything should be cleaned up
SELECT * FROM _timescaledb_catalog.hypertable GROUP BY id;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT * FROM _timescaledb_catalog.dimension_slice;
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM _timescaledb_catalog.chunk_constraint;
