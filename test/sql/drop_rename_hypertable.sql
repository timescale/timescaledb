\o /dev/null
\ir include/insert_two_partitions.sql
\o

\d+ "_timescaledb_internal".*

-- Test that renaming hypertable works 

\d _timescaledb_internal._hyper_1_1_chunk
ALTER TABLE "two_Partitions" RENAME TO "newname";
SELECT * FROM "newname";
SELECT * FROM _timescaledb_catalog.hypertable;

CREATE SCHEMA "newschema";

ALTER TABLE "newname" SET SCHEMA "newschema";
SELECT * FROM "newschema"."newname";
SELECT * FROM _timescaledb_catalog.hypertable;

SELECT * FROM _timescaledb_catalog.hypertable_index WHERE main_schema_name <> 'newschema';
SELECT * FROM _timescaledb_catalog.chunk_index WHERE main_schema_name <> 'newschema';

SELECT * FROM _timescaledb_catalog.hypertable;
DROP TABLE "newschema"."newname" CASCADE;

SELECT * FROM _timescaledb_catalog.hypertable;
\dt  "public".*
\dt  "_timescaledb_catalog".*
\dt+ "_timescaledb_internal".*

-- Test that renaming ordinary table works

CREATE TABLE renametable (foo int);
ALTER TABLE "renametable" RENAME TO "newname_none_ht";
SELECT * FROM "newname_none_ht";

