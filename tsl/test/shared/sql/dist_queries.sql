-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
-- Function for testing command execution on data nodes
CREATE OR REPLACE PROCEDURE test.data_node_exec(node_name NAME, command TEXT)
AS :TSL_MODULE_PATHNAME, 'ts_data_node_exec' LANGUAGE C;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Test DataNodeScan with subquery with one-time filter
SELECT
  id
FROM
  insert_test
WHERE
  NULL::int2 >= NULL::int2 OR
  EXISTS (SELECT 1 from dist_chunk_copy WHERE insert_test.id IS NOT NULL)
ORDER BY id;

-- Test query that inserts a Result node between ChunkDispatch and
-- DataNodeDispatch/DataNodeCopy. Fix for bug
-- https://github.com/timescale/timescaledb/issues/4339

SET timescaledb.enable_distributed_insert_with_copy=false;

BEGIN;
WITH upsert AS (
  UPDATE matches
  SET day = day - 1
  WHERE location = 'old trafford'
  RETURNING *
) INSERT INTO matches (day, location, team1, team2)
SELECT 9, 'old trafford', 'MNU', 'MNC'
WHERE NOT EXISTS (SELECT 1 FROM upsert);
SELECT * FROM matches ORDER BY 1,2,3,4;
ROLLBACK;

SET timescaledb.enable_distributed_insert_with_copy=true;

BEGIN;
WITH upsert AS (
  UPDATE matches
  SET day = day - 1
  WHERE location = 'old trafford'
  RETURNING *
) INSERT INTO matches (day, location, team1, team2)
SELECT 9, 'old trafford', 'MNU', 'MNC'
WHERE NOT EXISTS (SELECT 1 FROM upsert);
SELECT * FROM matches ORDER BY 1,2,3,4;
ROLLBACK;

-- Reference. The two queries above should be like this one:
BEGIN;
WITH upsert AS (
  UPDATE matches_reference
  SET day = day - 1
  WHERE location = 'old trafford'
  RETURNING *
) INSERT INTO matches_reference (day, location, team1, team2)
SELECT 9, 'old trafford', 'MNU', 'MNC'
WHERE NOT EXISTS (SELECT 1 FROM upsert);
SELECT * FROM matches_reference ORDER BY 1,2,3,4;
ROLLBACK;

-- Test query/command cancelation with statement_timeout
SET statement_timeout=200;
-- Execute long-running query on data nodes
\set ON_ERROR_STOP 0
-- distribute_exec() uses async functions
CALL distributed_exec('SELECT pg_sleep(200)');
-- data_node_exec() directly calls PQexec-style functions
CALL test.data_node_exec('data_node_1', 'SELECT pg_sleep(200)');
-- test weird parameters
CALL test.data_node_exec(NULL, 'SELECT pg_sleep(200)');
CALL test.data_node_exec('-', 'SELECT pg_sleep(200)');
CALL test.data_node_exec('data_node_1', NULL);
RESET statement_timeout;
\set ON_ERROR_STOP 1
-- Data node connections should be IDLE
SELECT node_name, database, connection_status, transaction_status, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1;
