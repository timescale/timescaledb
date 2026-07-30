-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM test.show_columnsp('_timescaledb_internal.%_hyper%');

-- Test that renaming hypertable works
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');
ALTER TABLE "two_Partitions" RENAME TO "newname";
SELECT * FROM "newname";
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA "newschema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

ALTER TABLE "newname" SET SCHEMA "newschema";
SELECT * FROM "newschema"."newname";
SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable;

DROP TABLE "newschema"."newname";

SELECT id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, compression_state, status FROM _timescaledb_catalog.hypertable;
SELECT schema, name FROM test.relation WHERE schema IN ('public', '_timescaledb_catalog', '_timescaledb_internal');

-- Test that renaming ordinary table works

CREATE TABLE renametable (foo int);
ALTER TABLE "renametable" RENAME TO "newname_none_ht";
SELECT * FROM "newname_none_ht";
