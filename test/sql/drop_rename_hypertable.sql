\o /dev/null
\ir include/insert_two_partitions.sql
\o

\d+ "_timescaledb_internal".*

-- Test that renaming hypertable is blocked
\set VERBOSITY default
\set ON_ERROR_STOP 0
ALTER TABLE "two_Partitions" RENAME TO "newname";
\set ON_ERROR_STOP 1

-- Test that renaming ordinary table works
CREATE TABLE renametable (foo int);
ALTER TABLE "renametable" RENAME TO "newname";
SELECT * FROM "newname";

SELECT * FROM _timescaledb_catalog.hypertable;
DROP TABLE "two_Partitions" CASCADE;

SELECT * FROM _timescaledb_catalog.hypertable;
\dt  "public".*
\dt  "_timescaledb_catalog".*
\dt+ "_timescaledb_internal".*

