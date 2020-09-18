-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

DROP TABLE IF EXISTS conditions;

SELECT * FROM add_data_node('data1', host => 'localhost', database => 'data1');
SELECT * FROM add_data_node('data2', host => 'localhost', database => 'data2');
SELECT * FROM add_data_node('data3', host => 'localhost', database => 'data3');

CREATE TABLE conditions(time TIMESTAMPTZ NOT NULL, device INTEGER, temperature FLOAT, humidity FLOAT);
GRANT SELECT ON conditions TO :ROLE_1;
GRANT INSERT, DELETE ON conditions TO :ROLE_2;
SELECT relname, relacl FROM pg_class WHERE relname = 'conditions';

SELECT * FROM create_distributed_hypertable('conditions', 'time', 'device');
SELECT has_table_privilege(:'ROLE_1', 'conditions', 'SELECT') AS "SELECT"
     , has_table_privilege(:'ROLE_1', 'conditions', 'DELETE') AS "DELETE"
     , has_table_privilege(:'ROLE_1', 'conditions', 'INSERT') AS "INSERT";

SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'conditions', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'conditions', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'conditions', 'INSERT') AS "INSERT";
$$, :'ROLE_1', :'ROLE_1', :'ROLE_1'));

SELECT has_table_privilege(:'ROLE_2', 'conditions', 'SELECT') AS "SELECT"
     , has_table_privilege(:'ROLE_2', 'conditions', 'DELETE') AS "DELETE"
     , has_table_privilege(:'ROLE_2', 'conditions', 'INSERT') AS "INSERT";

SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'conditions', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'conditions', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'conditions', 'INSERT') AS "INSERT";
$$, :'ROLE_2', :'ROLE_2', :'ROLE_2'));

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80
FROM generate_series('2019-01-01 00:00:00'::timestamptz, '2019-02-01 00:00:00', '1 min') AS time;

-- Check that we can actually execute a select as non-owner
SET ROLE :ROLE_1;
SELECT COUNT(*) FROM conditions;

SET ROLE :ROLE_CLUSTER_SUPERUSER;
GRANT UPDATE ON conditions TO :ROLE_2;
BEGIN;
GRANT TRUNCATE ON conditions TO :ROLE_2;
ROLLBACK;

-- Should have UPDATE, but not TRUNCATE
SELECT has_table_privilege(:'ROLE_2', 'conditions', 'SELECT') AS "SELECT"
     , has_table_privilege(:'ROLE_2', 'conditions', 'DELETE') AS "DELETE"
     , has_table_privilege(:'ROLE_2', 'conditions', 'INSERT') AS "INSERT"
     , has_table_privilege(:'ROLE_2', 'conditions', 'UPDATE') AS "UPDATE"
     , has_table_privilege(:'ROLE_2', 'conditions', 'TRUNCATE') AS "TRUNCATE";

SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'conditions', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'conditions', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'conditions', 'INSERT') AS "INSERT"
       , has_table_privilege('%s', 'conditions', 'UPDATE') AS "UPDATE"
       , has_table_privilege('%s', 'conditions', 'TRUNCATE') AS "TRUNCATE";
$$, :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2'));

-- Add another data node and check that grants are propagated when the
-- data node is attached to an existing table.
SELECT * FROM add_data_node('data4', host => 'localhost', database => 'data4');

\set ON_ERROR_STOP 0
SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'conditions', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'conditions', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'conditions', 'INSERT') AS "INSERT"
       , has_table_privilege('%s', 'conditions', 'UPDATE') AS "UPDATE"
       , has_table_privilege('%s', 'conditions', 'TRUNCATE') AS "TRUNCATE";
$$, :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2'));
\set ON_ERROR_STOP 1

SELECT * FROM attach_data_node('data4', 'conditions');

INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80
FROM generate_series('2019-02-01 00:00:00'::timestamptz, '2019-03-01 00:00:00', '1 min') AS time;

SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'conditions', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'conditions', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'conditions', 'INSERT') AS "INSERT"
       , has_table_privilege('%s', 'conditions', 'UPDATE') AS "UPDATE"
       , has_table_privilege('%s', 'conditions', 'TRUNCATE') AS "TRUNCATE";
$$, :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2', :'ROLE_2'));

-- Check that grants are not propagated when enable_grant_propagation
-- is false.
SET timescaledb.enable_grant_propagation = false;

CREATE TABLE no_grants(time TIMESTAMPTZ NOT NULL, device INTEGER, temperature FLOAT);
GRANT SELECT ON no_grants TO :ROLE_1;

-- First case is when table is created. Grants should not be propagated.
SELECT * FROM create_distributed_hypertable('no_grants', 'time', 'device');

SELECT has_table_privilege(:'ROLE_1', 'no_grants', 'SELECT') AS "SELECT"
     , has_table_privilege(:'ROLE_1', 'no_grants', 'DELETE') AS "DELETE"
     , has_table_privilege(:'ROLE_1', 'no_grants', 'INSERT') AS "INSERT";
SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'no_grants', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'no_grants', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'no_grants', 'INSERT') AS "INSERT";
$$, :'ROLE_1', :'ROLE_1', :'ROLE_1'));

-- Second case is when grants is done on an existing table. The grant
-- should not be propagated.
GRANT INSERT ON no_grants TO :ROLE_1;

SELECT has_table_privilege(:'ROLE_1', 'no_grants', 'SELECT') AS "SELECT"
     , has_table_privilege(:'ROLE_1', 'no_grants', 'DELETE') AS "DELETE"
     , has_table_privilege(:'ROLE_1', 'no_grants', 'INSERT') AS "INSERT";
SELECT * FROM test.remote_exec(NULL, format($$
  SELECT has_table_privilege('%s', 'no_grants', 'SELECT') AS "SELECT"
       , has_table_privilege('%s', 'no_grants', 'DELETE') AS "DELETE"
       , has_table_privilege('%s', 'no_grants', 'INSERT') AS "INSERT";
$$, :'ROLE_1', :'ROLE_1', :'ROLE_1'));

DROP TABLE conditions;
DROP TABLE no_grants;

-- Check that grants and revokes are copied properly to the chunks and
-- that newly created chunks have the right privileges.
CREATE TABLE conditions(
    time TIMESTAMPTZ NOT NULL,
    device INTEGER,
    temperature FLOAT
);

-- Create a hypertable and show that it does not have any privileges
SELECT * FROM create_hypertable('conditions', 'time', chunk_time_interval => '5 days'::interval);
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-10 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Add privileges and show that they propagate to the chunks
GRANT SELECT, INSERT ON conditions TO PUBLIC;
\z conditions
\z _timescaledb_internal.*chunk

-- Create some more chunks and show that they also get the privileges.
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-10 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Revoke one of the privileges and show that it propagate to the
-- chunks.
REVOKE INSERT ON conditions FROM PUBLIC;
\z conditions
\z _timescaledb_internal.*chunk

-- Add some more chunks and show that it inherits the grants from the
-- hypertable.
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-20 00:00'::timestamp, '2018-12-30 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Change grants of one chunk explicitly and check that it is possible

\z _timescaledb_internal._hyper_3_35_chunk
GRANT UPDATE ON _timescaledb_internal._hyper_3_35_chunk TO PUBLIC;
\z _timescaledb_internal._hyper_3_35_chunk
REVOKE SELECT ON _timescaledb_internal._hyper_3_35_chunk FROM PUBLIC;
\z _timescaledb_internal._hyper_3_35_chunk

DROP TABLE conditions;

-- Test that we can create a writer role, assign users to that role,
-- and allow the users to insert data and create new chunks.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

CREATE TABLE conditions(
       time timestamptz,
       device int CHECK (device > 0),
       temp float,
       PRIMARY KEY (time,device)
);

SELECT * FROM create_distributed_hypertable('conditions', 'time', 'device', 3);

-- Test that we can create a writer role, assign users to that role,
-- and allow the users to insert data and create new chunks.

SET ROLE :ROLE_DEFAULT_PERM_USER_2;
\set ON_ERROR_STOP 0
INSERT INTO conditions
SELECT time, 1 + (random()*30)::int, random()*80
FROM generate_series('2019-01-01 00:00:00'::timestamptz, '2019-02-01 00:00:00', '1 min') AS time;
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT INSERT ON conditions TO :ROLE_DEFAULT_PERM_USER_2;

SET ROLE :ROLE_DEFAULT_PERM_USER_2;
INSERT INTO conditions
SELECT time, 1 + (random()*30)::int, random()*80
FROM generate_series('2019-01-01 00:00:00'::timestamptz, '2019-02-01 00:00:00', '1 min') AS time;
