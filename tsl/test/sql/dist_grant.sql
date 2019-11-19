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
