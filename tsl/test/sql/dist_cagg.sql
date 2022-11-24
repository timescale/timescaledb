-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Support for execute_sql_and_filter_server_name_on_error()
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\ir include/filter_exec.sql
\o
\set ECHO all

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

CREATE SCHEMA some_schema AUTHORIZATION :ROLE_1;

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;
GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
GRANT CREATE ON SCHEMA public TO :ROLE_1;

-- test alter_data_node(unvailable) with caggs
--

-- create cagg on distributed hypertable
CREATE TABLE conditions_dist(day timestamptz NOT NULL, temperature INT NOT NULL);
SELECT create_distributed_hypertable('conditions_dist', 'day', chunk_time_interval => INTERVAL '1 day', replication_factor => 2);

INSERT INTO conditions_dist(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2010-01-01 00:00:00 MSK' :: timestamptz, '2010-03-01 00:00:00 MSK' :: timestamptz - interval '1 day', '1 day') as ts;

CREATE MATERIALIZED VIEW conditions_dist_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 month', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_dist
GROUP BY bucket;

SELECT * FROM conditions_dist_1m ORDER BY bucket;

-- case 1: ensure select works when data node is unavailable
SELECT alter_data_node(:'DATA_NODE_1');
SELECT alter_data_node(:'DATA_NODE_1', port => 55433, available => false);
SELECT * FROM conditions_dist_1m ORDER BY bucket;

-- case 2: ensure cagg can use real-time aggregation while data node is not available
INSERT INTO conditions_dist(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2010-03-01 00:00:00 MSK' :: timestamptz, '2010-04-01 00:00:00 MSK' :: timestamptz - interval '1 day', '1 day') as ts;

-- case 3: refreshing a cagg currently doesn't work when a data node is unavailable
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('conditions_dist_1m', '2010-01-01', '2010-04-01');
\set ON_ERROR_STOP 1

-- case 4: ensure that new data is visible using real-time aggregation
SELECT * FROM conditions_dist_1m ORDER BY bucket;

-- case 5: TRUNCATE will not work while data node is not available because invalidations are sent to the data nodes
\set ON_ERROR_STOP 0
TRUNCATE TABLE conditions_dist;
\set ON_ERROR_STOP 1

-- case 6: TRUNCATE cagg directly
--
-- Fails because it is trying to send invalidation to DN.
--
-- note: data in cagg is stored on AN, so the TRUNCATION itself doesn't fail,
-- but it fails as a result of the failed invalidation.
\set ON_ERROR_STOP 0
TRUNCATE conditions_dist_1m;
\set ON_ERROR_STOP 1

-- case 7: ensure data node update works when it becomes available again
SELECT alter_data_node(:'DATA_NODE_1', port => 55432);
SELECT alter_data_node(:'DATA_NODE_1', available => true);

CALL refresh_continuous_aggregate('conditions_dist_1m', '2010-01-01', '2010-04-01');

ALTER MATERIALIZED VIEW conditions_dist_1m SET(timescaledb.materialized_only = false);
SELECT * FROM conditions_dist_1m ORDER BY bucket;

ALTER MATERIALIZED VIEW conditions_dist_1m SET(timescaledb.materialized_only = true);
SELECT * FROM conditions_dist_1m ORDER BY bucket;

-- cleanup
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
