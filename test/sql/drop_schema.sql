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

SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT id, hypertable_id, schema_name, table_name, status, osm_chunk FROM _timescaledb_catalog.chunk;

RESET ROLE;
--drop the associated schema. We drop the extra schema to show we can
--handle multi-schema drops
DROP SCHEMA chunk_schema1, extra_schema CASCADE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--show that the metadata for the table using the dropped schema is
--changed. The other table is not affected.
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT id, hypertable_id, schema_name, table_name, status, osm_chunk FROM _timescaledb_catalog.chunk;

--new chunk should be created in the internal associated schema
INSERT INTO hypertable_schema.test1 VALUES ('2001-01-01 01:01:01', 23.3, 1);
SELECT id, hypertable_id, schema_name, table_name, status, osm_chunk FROM _timescaledb_catalog.chunk;

RESET ROLE;
--dropping the internal schema should not work
\set ON_ERROR_STOP 0
DROP SCHEMA _timescaledb_internal CASCADE;
\set ON_ERROR_STOP 1
--dropping the hypertable schema should delete everything
DROP SCHEMA hypertable_schema CASCADE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--everything should be cleaned up
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable GROUP BY id;
SELECT id, hypertable_id, schema_name, table_name, status, osm_chunk FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT * FROM _timescaledb_catalog.dimension_slice;
SELECT chunk_id, id AS dimension_slice_id, format('constraint_%s', id)::name AS constraint_name FROM _timescaledb_catalog.dimension_slice ORDER BY chunk_id, dimension_slice_id;
