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

\c server_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c server_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c server_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Add servers using the TimescaleDB server management API
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_2', database => 'server_2', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_3', database => 'server_3', port => inet_server_port(), password => 'pass', if_not_exists => true);

-- Create distributed hypertables. Add a trigger and primary key
-- constraint to test how those work
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);

-- An underreplicated table that will has a replication_factor > num_servers
CREATE TABLE underreplicated(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('underreplicated', 'time', replication_factor => 4);

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
EXPLAIN (COSTS FALSE)
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1);

EXPLAIN (VERBOSE, COSTS FALSE)
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1);

-- Create some chunks through insertion
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1),
       ('2017-01-01 08:01', 1, 1.2),
       ('2018-01-02 08:01', 2, 1.3),
       ('2019-01-01 09:11', 3, 2.1);

-- Test prepared statement
PREPARE dist_insert (timestamptz, int, float) AS
INSERT INTO disttable VALUES ($1, $2, $3);

EXECUTE dist_insert ('2017-01-01 06:05', 1, 1.4);

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

-- Show what some queries would look like on the frontend
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM disttable;

SELECT * FROM disttable;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT time_bucket('3 hours', time) AS time, device, avg(temp) AS avg_temp
FROM disttable GROUP BY 1, 2
ORDER BY 1;

-- Execute some queries on the frontend and return the results
SELECT * FROM disttable;

SELECT time_bucket('3 hours', time) AS time, device, avg(temp) AS avg_temp
FROM disttable
GROUP BY 1, 2
ORDER BY 1;

SELECT time_bucket('3 hours', time) AS time, device, avg(temp) AS avg_temp
FROM disttable GROUP BY 1, 2
HAVING avg(temp) > 1.2
ORDER BY 1;

SELECT time_bucket('3 hours', time) AS time, device, avg(temp) AS avg_temp
FROM disttable
WHERE temp > 2
GROUP BY 1, 2
HAVING avg(temp) > 1.2
ORDER BY 1;

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

-- Show contents after re-adding column
SELECT * FROM disttable;

-- Test INSERTS with RETURNING. Since we previously dropped a column
-- on the hypertable, this also tests that we handle conversion of the
-- attribute numbers in the RETURNING clause, since they now differ
-- between the hypertable root relation and the chunk currently
-- RETURNING from.
INSERT INTO disttable (time, device, "Color", temp)
VALUES ('2017-09-02 06:09', 4, 1, 9.8)
RETURNING time, "Color", temp;

INSERT INTO disttable (time, device, "Color", temp)
VALUES ('2017-09-03 06:18', 9, 3, 8.7)
RETURNING 1;

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
SELECT * FROM disttable;

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

-- Test deletes (no rows deleted)
DELETE FROM disttable WHERE device = 3
RETURNING *;

-- Test deletes (rows deleted)
DELETE FROM disttable WHERE device = 4
RETURNING *;

-- Query to show that rows are deleted
SELECT * FROM disttable;

-- Ensure rows are deleted on the servers
\c server_1
SELECT * FROM disttable;
\c server_2
SELECT * FROM disttable;
\c server_3
SELECT * FROM disttable;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

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

UPDATE underreplicated SET temp = 2.0 WHERE device = 2
RETURNING time, temp, device;

SELECT * FROM underreplicated;

-- Show that all replica chunks are updated
\c server_1
SELECT * FROM underreplicated;
\c server_2
SELECT * FROM underreplicated;
\c server_3
SELECT * FROM underreplicated;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

DELETE FROM underreplicated WHERE device = 2
RETURNING *;

-- Ensure deletes across all servers
\c server_1
SELECT * FROM underreplicated;
\c server_2
SELECT * FROM underreplicated;
\c server_3
SELECT * FROM underreplicated;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Test hypertable creation fails on distributed error
\c server_3
CREATE TABLE remotetable(time timestamptz PRIMARY KEY, id int, cost float);
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

\set ON_ERROR_STOP 0
CREATE TABLE remotetable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), color int, temp float);
SELECT * FROM create_hypertable('remotetable', 'time', replication_factor => 1);
\set ON_ERROR_STOP 1

SELECT * FROM timescaledb_information.hypertable;

-- Test distributed hypertable creation with many parameters
\c server_1
CREATE SCHEMA "T3sTSch";
CREATE SCHEMA "Table\\Schema";
GRANT ALL ON SCHEMA "T3sTSch" TO :ROLE_DEFAULT_CLUSTER_USER;
GRANT ALL ON SCHEMA "Table\\Schema" TO :ROLE_DEFAULT_CLUSTER_USER;
\c server_2
CREATE SCHEMA "T3sTSch";
CREATE SCHEMA "Table\\Schema";
GRANT ALL ON SCHEMA "T3sTSch" TO :ROLE_DEFAULT_CLUSTER_USER;
GRANT ALL ON SCHEMA "Table\\Schema" TO :ROLE_DEFAULT_CLUSTER_USER;
\c server_3
CREATE SCHEMA "T3sTSch";
CREATE SCHEMA "Table\\Schema";
GRANT ALL ON SCHEMA "T3sTSch" TO :ROLE_DEFAULT_CLUSTER_USER;
GRANT ALL ON SCHEMA "Table\\Schema" TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA "T3sTSch";
CREATE SCHEMA "Table\\Schema";
GRANT ALL ON SCHEMA "T3sTSch" TO :ROLE_DEFAULT_CLUSTER_USER;
GRANT ALL ON SCHEMA "Table\\Schema" TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE TABLE "Table\\Schema"."Param_Table"("time Col %#^#@$#" timestamptz, __region text, reading float);
SELECT * FROM create_hypertable('"Table\\Schema"."Param_Table"', 'time Col %#^#@$#', partitioning_column => '__region', number_partitions  => 4,
associated_schema_name => 'T3sTSch', associated_table_prefix => 'test*pre_', chunk_time_interval => interval '1 week',
create_default_indexes => FALSE, if_not_exists => TRUE, migrate_data => TRUE, replication_factor => 2,
servers => '{ "server_2", "server_3" }');

-- Test attach_server
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM attach_server('"Table\\Schema"."Param_Table"', 'server_1');
SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Verify hypertables on all servers
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT * FROM test.show_triggers('"Table\\Schema"."Param_Table"');

\c server_1
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT t.tgname, t.tgtype, t.tgfoid::regproc, substring(pg_get_triggerdef(t.oid, true) from 15)
FROM pg_trigger t, pg_class c WHERE c.relname = 'Param_Table' AND t.tgrelid = c.oid;

\c server_2
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT t.tgname, t.tgtype, t.tgfoid::regproc, substring(pg_get_triggerdef(t.oid, true) from 15)
FROM pg_trigger t, pg_class c WHERE c.relname = 'Param_Table' AND t.tgrelid = c.oid;

\c server_3
SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT t.tgname, t.tgtype, t.tgfoid::regproc, substring(pg_get_triggerdef(t.oid, true) from 15)
FROM pg_trigger t, pg_class c WHERE c.relname = 'Param_Table' AND t.tgrelid = c.oid;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Test multi-dimensional hypertable (note that add_dimension is not currently propagated to backends)
CREATE TABLE dimented_table (time timestamptz, column1 int, column2 timestamptz, column3 int);
SELECT * FROM create_hypertable('dimented_table', 'time', partitioning_column => 'column1', number_partitions  => 4, replication_factor => 1, servers => '{ "server_1" }');
SELECT * FROM add_dimension('dimented_table', 'column2', chunk_time_interval => interval '1 week');
SELECT * FROM add_dimension('dimented_table', 'column3', 4, partitioning_func => '_timescaledb_internal.get_partition_for_key');

SELECT * FROM _timescaledb_catalog.dimension;
SELECT * FROM attach_server('dimented_table', 'server_2');

SELECT * FROM _timescaledb_catalog.dimension;
\c server_1
-- Note that this didn't get the add_dimension
SELECT * FROM _timescaledb_catalog.dimension;
\c server_2
SELECT * FROM _timescaledb_catalog.dimension;

--test per-server queries
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
-- Create some chunks through insertion
CREATE TABLE disttable_replicated(time timestamptz PRIMARY KEY, device int CHECK (device > 0), temp float, "Color" int);
SELECT * FROM create_hypertable('disttable_replicated', 'time', replication_factor => 2);
INSERT INTO disttable_replicated VALUES
       ('2017-01-01 06:01', 1, 1.1, 1),
       ('2017-01-01 08:01', 1, 1.2, 2),
       ('2018-01-02 08:01', 2, 1.3, 3),
       ('2019-01-01 09:11', 3, 2.1, 4),
       ('2020-01-01 06:01', 5, 1.1, 10),
       ('2020-01-01 08:01', 6, 1.2, 11),
       ('2021-01-02 08:01', 7, 1.3, 12),
       ('2022-01-01 09:11', 8, 2.1, 13);

SELECT * FROM disttable_replicated;

EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE, TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated;

--guc disables the optimization
SET timescaledb.enable_per_server_queries = FALSE;
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE, TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated;
SET timescaledb.enable_per_server_queries = TRUE;

--test WHERE clause
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE, TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated WHERE temp > 2.0;

--test OR
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE, TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated WHERE temp > 2.0 or "Color" = 11;

--test some chunks excluded
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE,  TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated WHERE time < '2018-01-01 09:11';

--test all chunks excluded
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE,  TIMING FALSE, SUMMARY FALSE)
SELECT * FROM disttable_replicated WHERE time < '2002-01-01 09:11';

--test cte
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE,  TIMING FALSE, SUMMARY FALSE)
WITH cte AS (
    SELECT * FROM disttable_replicated
)
SELECT * FROM cte;

--queries that involve updates/inserts are not optimized
EXPLAIN (VERBOSE, ANALYZE, COSTS FALSE,  TIMING FALSE, SUMMARY FALSE)
WITH devices AS (
     SELECT DISTINCT device FROM disttable_replicated ORDER BY device
)
UPDATE disttable_replicated SET device = 2 WHERE device = (SELECT device FROM devices LIMIT 1);


-- Test inserts with smaller batch size and more tuples to reach full
-- batch
SET timescaledb.max_insert_batch_size=4;

CREATE TABLE twodim (time timestamptz DEFAULT '2019-02-10 10:11', "Color" int DEFAULT 11 CHECK ("Color" > 0), temp float DEFAULT 22.1);
-- Create a replicated table to ensure we handle that case correctly
-- with batching
SELECT * FROM create_hypertable('twodim', 'time', 'Color', 3, replication_factor => 2, servers => '{ "server_1", "server_2", "server_3" }');

SELECT * FROM twodim
ORDER BY time;

-- INSERT enough data to stretch across multiple batches per
-- server. Also return a system column. Although we write tuples to
-- multiple servers, the returned tuple should only be the ones in the
-- original insert statement (without the replica tuples).
WITH result AS (
     INSERT INTO twodim VALUES
       ('2017-02-01 06:01', 1, 1.1),
       ('2017-02-01 08:01', 1, 1.2),
       ('2018-02-02 08:01', 2, 1.3),
       ('2019-02-01 09:11', 3, 2.1),
       ('2019-02-02 09:11', 3, 2.1),
       ('2019-02-02 10:01', 5, 1.2),
       ('2019-02-03 11:11', 6, 3.5),
       ('2019-02-04 08:21', 4, 6.6),
       ('2019-02-04 10:11', 7, 7.4),
       ('2019-02-04 12:11', 8, 2.1),
       ('2019-02-05 13:31', 8, 6.3),
       ('2019-02-06 02:11', 5, 1.8),
       ('2019-02-06 01:13', 7, 7.9),
       ('2019-02-06 19:24', 9, 5.9),
       ('2019-02-07 18:44', 5, 9.7),
       ('2019-02-07 20:24', 6, NULL),
       ('2019-02-07 09:33', 7, 9.5),
       ('2019-02-08 08:54', 1, 7.3),
       ('2019-02-08 18:14', 4, 8.2),
       ('2019-02-09 19:23', 8, 9.1)
     RETURNING tableoid = 'twodim'::regclass AS is_tableoid, time, temp, "Color"
) SELECT * FROM result ORDER BY time;

-- Test insert with default values and a batch size of 1.
SET timescaledb.max_insert_batch_size=1;
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO twodim DEFAULT VALUES;
INSERT INTO twodim DEFAULT VALUES;

-- Reset the batch size
SET timescaledb.max_insert_batch_size=4;

-- Constraint violation
\set ON_ERROR_STOP 0
INSERT INTO twodim VALUES ('2019-02-10 17:54', 0, 10.2);
\set ON_ERROR_STOP 1

-- Disable batching, reverting to FDW tuple-by-tuple inserts.
-- First EXPLAIN with batching turned on.
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO twodim VALUES
       ('2019-02-10 16:23', 5, 7.1),
       ('2019-02-10 17:11', 7, 3.2);

SET timescaledb.max_insert_batch_size=0;

-- Compare without batching
EXPLAIN (VERBOSE, COSTS OFF, TIMING OFF, SUMMARY OFF)
INSERT INTO twodim VALUES
       ('2019-02-10 16:23', 5, 7.1),
       ('2019-02-10 17:11', 7, 3.2);

-- Insert without batching
INSERT INTO twodim VALUES
       ('2019-02-10 16:23', 5, 7.1),
       ('2019-02-10 17:11', 7, 3.2);

-- Check results
SELECT * FROM twodim
ORDER BY time;

SELECT count(*) FROM twodim;

-- Show distribution across servers
\c server_1
SELECT * FROM twodim
ORDER BY time;
SELECT count(*) FROM twodim;

\c server_2
SELECT * FROM twodim
ORDER BY time;
SELECT count(*) FROM twodim;

\c server_3
SELECT * FROM twodim
ORDER BY time;
SELECT count(*) FROM twodim;
