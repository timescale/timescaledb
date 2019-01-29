-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_3;
SET client_min_messages TO NOTICE;
CREATE DATABASE server_1;
CREATE DATABASE server_2;
CREATE DATABASE server_3;

-- Add servers using the TimescaleDB server management API
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass');
SELECT * FROM add_server('server_2', database => 'server_2', password => 'pass');
SELECT * FROM add_server('server_3', database => 'server_3', port => inet_server_port(), password => 'pass');

-- Create distributed hypertables. Add a trigger and primary key
-- constraint to test how those work
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);

-- An underreplicated table that will has a replication_factor > num_servers
CREATE TABLE underreplicated(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('underreplicated', 'time', replication_factor => 4);

-- Create tables on remote servers
\c server_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('disttable', 'time');
CREATE TABLE underreplicated(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('underreplicated', 'time');
\c server_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('disttable', 'time');
CREATE TABLE underreplicated(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('underreplicated', 'time');
\c server_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('disttable', 'time');
CREATE TABLE underreplicated(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('underreplicated', 'time');
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE OR REPLACE FUNCTION test_trigger()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT count(*) INTO cnt FROM hyper;
    RAISE WARNING 'FIRING trigger when: % level: % op: % cnt: % trigger_name %',
        tg_when, tg_level, tg_op, cnt, tg_name;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$;

CREATE TRIGGER _0_test_trigger_insert
    BEFORE INSERT ON disttable
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

-- The constraints, indexes, and triggers on the hypertable
SELECT * FROM test.show_constraints('disttable');
SELECT * FROM test.show_indexes('disttable');
SELECT * FROM test.show_triggers('disttable');

-- Drop a column. This will make the attribute numbers of the
-- hypertable's root relation differ from newly created chunks. It is
-- a way to test that we properly handle attributed conversion between
-- the root table and chunks
ALTER TABLE disttable DROP COLUMN color;
-- Currently no distributed DDL support, so need to manually drop
-- column on datanodes
\c server_1
ALTER TABLE disttable DROP COLUMN color;
\c server_2
ALTER TABLE disttable DROP COLUMN color;
\c server_3
ALTER TABLE disttable DROP COLUMN color;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- EXPLAIN some inserts to see what plans and explain output for
-- remote inserts look like
EXPLAIN
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1);

EXPLAIN VERBOSE
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1);

-- Create some chunks through insertion
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1),
       ('2017-01-01 08:01', 1, 1.2),
       ('2018-01-02 08:01', 2, 1.3),
       ('2019-01-01 09:11', 3, 2.1),
       ('2017-01-01 06:05', 1, 1.4);

-- Show chunks created
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('disttable');

-- Show that there are assigned server_chunk_id:s in chunk server mappings
SELECT * FROM _timescaledb_catalog.chunk_server;

-- Show that chunks are created on remote servers
\c server_1
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('disttable');
SELECT * FROM disttable;
\c server_2
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('disttable');
SELECT * FROM disttable;
\c server_3
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('disttable');
SELECT * FROM disttable;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- The constraints, indexes, and triggers on foreign chunks. Only
-- check constraints should recurse to foreign chunks (although they
-- aren't enforced on a foreign table)
SELECT st."Child" as chunk_relid, test.show_constraints((st)."Child")
FROM test.show_subtables('disttable') st;
SELECT st."Child" as chunk_relid, test.show_indexes((st)."Child")
FROM test.show_subtables('disttable') st;
SELECT st."Child" as chunk_relid, test.show_triggers((st)."Child")
FROM test.show_subtables('disttable') st;

-- Check that the chunks are assigned servers
SELECT * FROM _timescaledb_catalog.chunk_server;

-- Adding a new trigger should not recurse to foreign chunks
CREATE TRIGGER _1_test_trigger_insert
    AFTER INSERT ON disttable
    FOR EACH ROW EXECUTE PROCEDURE test_trigger();

SELECT st."Child" as chunk_relid, test.show_triggers((st)."Child")
FROM test.show_subtables('disttable') st;

-- Check that we can create indexes on distributed hypertables and
-- that they don't recurse to foreign chunks
CREATE INDEX ON disttable (time, device);

SELECT * FROM test.show_indexes('disttable');
SELECT st."Child" as chunk_relid, test.show_indexes((st)."Child")
FROM test.show_subtables('disttable') st;

-- No index mappings should exist either
SELECT * FROM _timescaledb_catalog.chunk_index;

-- Check that creating columns work
ALTER TABLE disttable ADD COLUMN "Color" int;

-- Currently no distributed DDL support, so need to manually add
-- column on datanodes
\c server_1
ALTER TABLE disttable ADD COLUMN "Color" int;
\c server_2
ALTER TABLE disttable ADD COLUMN "Color" int;
\c server_3
ALTER TABLE disttable ADD COLUMN "Color" int;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT * FROM test.show_columns('disttable');
SELECT st."Child" as chunk_relid, test.show_columns((st)."Child")
FROM test.show_subtables('disttable') st;

-- Adding a new unique constraint should not recurse to foreign
-- chunks, but a check constraint should
ALTER TABLE disttable ADD CONSTRAINT disttable_color_unique UNIQUE (time, "Color");
ALTER TABLE disttable ADD CONSTRAINT disttable_temp_non_negative CHECK (temp > 0.0);

SELECT st."Child" as chunk_relid, test.show_constraints((st)."Child")
FROM test.show_subtables('disttable') st;

SELECT cc.*
FROM (SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
      FROM show_chunks('disttable')) c,
      _timescaledb_catalog.chunk_constraint cc
WHERE c.chunk_id = cc.chunk_id;


-- Test INSERTS with RETURNING. Since we previously dropped a column
-- on the hypertable, this also tests that we handle conversion of the
-- attribute numbers in the RETURNING clause, since they now differ
-- between the hypertable root relation and the chunk currently
-- RETURNING from.
INSERT INTO disttable (time, device, "Color", temp)
VALUES ('2017-09-02 06:09', 4, 1, 9.8)
RETURNING time, "Color", temp;

-- On conflict
INSERT INTO disttable (time, device, "Color", temp)
VALUES ('2017-09-02 06:09', 6, 2, 10.5)
ON CONFLICT DO NOTHING;

-- Show new row and that conflicting row is not inserted
\c server_1
SELECT * FROM disttable;
\c server_2
SELECT * FROM disttable;
\c server_3
SELECT * FROM disttable;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

\set ON_ERROR_STOP 0
-- ON CONFLICT only works with DO NOTHING
INSERT INTO disttable (time, device, "Color", temp)
VALUES ('2017-09-09 08:13', 7, 3, 27.5)
ON CONFLICT (time) DO UPDATE SET temp = 3.2;
\set ON_ERROR_STOP 1

-- Test updates
UPDATE disttable SET device = 4 WHERE device = 3;

WITH devices AS (
     SELECT DISTINCT device FROM disttable ORDER BY device
)
UPDATE disttable SET device = 2 WHERE device = (SELECT device FROM devices LIMIT 1);

\set ON_ERROR_STOP 0
-- Updates referencing non-existing column
UPDATE disttable SET device = 4 WHERE no_such_column = 2;
UPDATE disttable SET no_such_column = 4 WHERE device = 2;
-- Update to system column
UPDATE disttable SET tableoid = 4 WHERE device = 2;
\set ON_ERROR_STOP 1

-- Test deletes
DELETE FROM disttable WHERE device = 3;

-- Test underreplicated chunk warning
INSERT INTO underreplicated VALUES ('2017-01-01 06:01', 1, 1.1),
                                   ('2017-01-02 07:01', 2, 3.5);

SELECT * FROM _timescaledb_catalog.chunk_server;
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('underreplicated');

-- Show chunk server mappings
SELECT * FROM _timescaledb_catalog.chunk_server;

-- Show that chunks are created on remote servers and that all
-- servers/chunks have the same data due to replication
\c server_1
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('underreplicated');
SELECT * FROM underreplicated;
\c server_2
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('underreplicated');
SELECT * FROM underreplicated;
\c server_3
SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('underreplicated');
SELECT * FROM underreplicated;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
