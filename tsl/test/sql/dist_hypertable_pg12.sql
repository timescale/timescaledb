-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests using features supported in PG12 and greater.
-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

-- Add data nodes using the TimescaleDB node management API
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

-- Create a new table access method by reusing heap handler
CREATE ACCESS METHOD test_am TYPE TABLE HANDLER heap_tableam_handler;

SELECT * FROM test.remote_exec('{ data_node_1, data_node_2, data_node_3 }', $$
CREATE ACCESS METHOD test_am TYPE TABLE HANDLER heap_tableam_handler;
$$);

-- Create distributed hypertable using non-default access method
CREATE TABLE disttable(time timestamptz NOT NULL, device int, temp_c float, temp_f float GENERATED ALWAYS AS (temp_c * 9 / 5 + 32) STORED) USING test_am;
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 3);

-- Make sure that distributed hypertable created on data nodes is
-- using the correct table access method
SELECT * FROM test.remote_exec('{ data_node_1, data_node_2, data_node_3 }', $$

SELECT amname AS hypertable_amname
FROM pg_class cl, pg_am am
WHERE cl.oid = 'disttable'::regclass
AND cl.relam = am.oid;
$$);

-- Check that basic operations are working as expected
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, -10.0),
       ('2017-01-01 09:11', 3, -5.0),
       ('2017-01-01 08:01', 1, 1.0),
       ('2017-01-02 08:01', 2, 5.0),
       ('2018-07-02 08:01', 87, 10.0),
       ('2018-07-01 06:01', 13, 15.0),
       ('2018-07-01 09:11', 90, 20.0),
       ('2018-07-01 08:01', 29, 24.0);

SELECT * FROM disttable ORDER BY time;

-- Show that GENERATED columns work for INSERT with RETURNING clause
TRUNCATE disttable;
EXPLAIN VERBOSE
INSERT INTO disttable VALUES ('2017-08-01 06:01', 1, 35.0) RETURNING *;
INSERT INTO disttable VALUES ('2017-08-01 06:01', 1, 35.0) RETURNING *;

-- Same values returned with SELECT:
SELECT * FROM disttable ORDER BY 1;

UPDATE disttable SET temp_c=40.0 WHERE device=1;
SELECT * FROM disttable ORDER BY 1;

-- Insert another value
INSERT INTO disttable VALUES ('2017-09-01 06:01', 2, 30.0);
SELECT * FROM disttable ORDER BY 1;
-- Delete a value based on the generated column
DELETE FROM disttable WHERE temp_f=104;
SELECT * FROM disttable ORDER BY 1;

DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
