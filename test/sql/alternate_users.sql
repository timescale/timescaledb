-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir include/insert_single.sql

\c :TEST_DBNAME :ROLE_SUPERUSER

-- make sure tablespace1 exists
-- since there is no CREATE TABLESPACE IF EXISTS we drop with if exists and recreate
SET client_min_messages TO error;
DROP TABLESPACE IF EXISTS tablespace1;
RESET client_min_messages;
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;

--needed for ddl ops:
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER_2;

--needed for ROLE_DEFAULT_PERM_USER_2 to write to the 'one_Partition' schema which
--is owned by ROLE_DEFAULT_PERM_USER
GRANT CREATE ON SCHEMA "one_Partition" TO :ROLE_DEFAULT_PERM_USER_2;

--test creating and using schema as non-superuser
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT * FROM test.relation WHERE schema='public' ORDER BY schema, name;

\set ON_ERROR_STOP 0
SELECT * FROM "one_Partition";
SELECT set_chunk_time_interval('"one_Partition"', 1::bigint);
select add_dimension('"one_Partition"', 'device_id', 2);
select attach_tablespace('tablespace1', '"one_Partition"');
\set ON_ERROR_STOP 1

CREATE TABLE "1dim"(time timestamp, temp float);
SELECT create_hypertable('"1dim"', 'time');
INSERT INTO "1dim" VALUES('2017-01-20T09:00:01', 22.5);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:21', 21.2);
INSERT INTO "1dim" VALUES('2017-01-20T09:00:47', 25.1);
SELECT * FROM "1dim";


\ir include/ddl_ops_1.sql
\ir include/ddl_ops_2.sql

--test proper denials for all security definer functions:
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLE plain_table_su (time timestamp, temp float);
CREATE TABLE hypertable_su (time timestamp, temp float);
SELECT create_hypertable('hypertable_su', 'time');
CREATE INDEX "ind_1" ON hypertable_su (time);
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
--all of the following should produce errors
\set ON_ERROR_STOP 0
SELECT create_hypertable('plain_table_su', 'time');
CREATE INDEX ON plain_table_su (time, temp);
CREATE INDEX ON hypertable_su (time, temp);
DROP INDEX "ind_1";
ALTER INDEX "ind_1" RENAME TO "ind_2";
\set ON_ERROR_STOP 1

--test that I can't do anything to a non-owned hypertable.
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
SELECT * FROM hypertable_su;
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--grant read permissions
\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT SELECT ON hypertable_su TO :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT * FROM hypertable_su;
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--grant read, insert permissions
\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT SELECT, INSERT ON hypertable_su TO :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
SELECT * FROM hypertable_su;
\set ON_ERROR_STOP 0
CREATE INDEX ON hypertable_su (time, temp);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;
\set ON_ERROR_STOP 1

--change owner
\c :TEST_DBNAME :ROLE_SUPERUSER
ALTER TABLE hypertable_su OWNER TO :ROLE_DEFAULT_PERM_USER_2;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
INSERT INTO hypertable_su VALUES('2017-01-20T09:00:01', 22.5);
SELECT * FROM hypertable_su;
CREATE INDEX ON hypertable_su (time, temp);
ALTER TABLE hypertable_su ADD COLUMN val2 integer;

--a chunk catalog entry for a table that is not a child of the hypertable
--must not give the hypertable owner access to that table
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
CREATE TABLE private_table (time TIMESTAMPTZ NOT NULL, value INT);
INSERT INTO private_table VALUES ('2024-01-01 12:00+00', 1), ('2024-01-01 13:00+00', 2);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE catalog_relid_test (time TIMESTAMPTZ NOT NULL, value INT);
SELECT create_hypertable('catalog_relid_test', 'time', chunk_time_interval => INTERVAL '1 day');
INSERT INTO catalog_relid_test VALUES ('2024-01-05 12:00+00', 5);

\c :TEST_DBNAME :ROLE_SUPERUSER
WITH c AS (
	INSERT INTO _timescaledb_catalog.chunk (relid, hypertable_id, creation_time)
	SELECT 'private_table'::regclass, h.id, now()
	FROM _timescaledb_catalog.hypertable h WHERE h.table_name = 'catalog_relid_test'
	RETURNING id, hypertable_id)
INSERT INTO _timescaledb_catalog.dimension_slice (chunk_id, dimension_id, range_start, range_end)
SELECT c.id, d.id,
	_timescaledb_functions.to_unix_microseconds('2024-01-01T00:00:00+00'),
	_timescaledb_functions.to_unix_microseconds('2024-01-02T00:00:00+00')
FROM c JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = c.hypertable_id;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT * FROM catalog_relid_test ORDER BY time;
UPDATE catalog_relid_test SET value = 10 WHERE value = 1;
DELETE FROM catalog_relid_test WHERE value = 2;
INSERT INTO catalog_relid_test VALUES ('2024-01-01 14:00+00', 3);
\set ON_ERROR_STOP 1
--queries that do not touch the entry still work
SELECT value FROM catalog_relid_test WHERE time >= '2024-01-05T00:00:00+00';

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT value FROM private_table ORDER BY value;

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_catalog.chunk WHERE relid = 'private_table'::regclass;
