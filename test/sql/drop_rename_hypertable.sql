-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM test.show_columnsp('_timescaledb_internal.%_hyper%');

-- Test that renaming hypertable works
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');
ALTER TABLE "two_Partitions" RENAME TO "newname";
SELECT * FROM "newname";
SELECT * FROM _timescaledb_catalog.hypertable;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA "newschema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

ALTER TABLE "newname" SET SCHEMA "newschema";
SELECT * FROM "newschema"."newname";
SELECT * FROM _timescaledb_catalog.hypertable;

DROP TABLE "newschema"."newname";

SELECT * FROM _timescaledb_catalog.hypertable;
\dt  "public".*
\dt  "_timescaledb_catalog".*
\dt "_timescaledb_internal".*

-- Test that renaming ordinary table works

CREATE TABLE renametable (foo int);
ALTER TABLE "renametable" RENAME TO "newname_none_ht";
SELECT * FROM "newname_none_ht";
