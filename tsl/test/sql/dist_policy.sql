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

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;
GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO :ROLE_1;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;

-- Create a fake clock that we can use below and make sure that it is
-- defined on the data nodes as well.
CREATE TABLE time_table (time BIGINT);
INSERT INTO time_table VALUES (1);
CREATE OR REPLACE FUNCTION dummy_now()
RETURNS BIGINT
LANGUAGE SQL
STABLE AS 'SELECT time FROM time_table';
GRANT ALL ON TABLE time_table TO PUBLIC;

SELECT * FROM test.remote_exec(NULL, $$
       CREATE TABLE time_table (time BIGINT);
       INSERT INTO time_table VALUES (1);
       CREATE OR REPLACE FUNCTION dummy_now()
       RETURNS BIGINT
       LANGUAGE SQL
       STABLE AS 'SELECT time FROM time_table';
       GRANT ALL ON TABLE time_table TO PUBLIC;
$$);

SET ROLE :ROLE_1;
CREATE TABLE conditions(
    time BIGINT NOT NULL,
    device INT,
    value FLOAT
);

SELECT * FROM create_distributed_hypertable('conditions', 'time', 'device', 3,
       chunk_time_interval => 5);
SELECT set_integer_now_func('conditions', 'dummy_now');

INSERT INTO conditions
SELECT time, device, random()*80
FROM generate_series(1, 40) AS time,
     generate_series(1,3) AS device
ORDER BY time, device;

SELECT add_retention_policy('conditions', 5, true) as retention_job_id \gset

-- Now simulate drop_chunks running automatically by calling it
-- explicitly. Show chunks before and after.
SELECT show_chunks('conditions');
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('conditions'); $$);
UPDATE time_table SET time = 20;
SELECT * FROM test.remote_exec(NULL, $$ UPDATE time_table SET time = 20; $$);
CALL run_job(:retention_job_id);
SELECT show_chunks('conditions');
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('conditions'); $$);

SELECT remove_retention_policy('conditions');

-- Check that we can insert into the table without the retention
-- policy and not get an error. This will be a problem if the policy
-- did not propagate drop_chunks to data nodes.
INSERT INTO conditions
SELECT time, device, random()*80
FROM generate_series(1,10) AS time,
     generate_series(1,3) AS device;

-- Make sure reorder policy is blocked for distributed hypertable
\set ON_ERROR_STOP 0
SELECT add_reorder_policy('conditions', 'conditions_time_idx');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;

