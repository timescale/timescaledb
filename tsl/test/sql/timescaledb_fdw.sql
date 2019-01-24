-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SERVER some_server FOREIGN DATA WRAPPER timescaledb_fdw;
CREATE FOREIGN TABLE test_ft (c0 int, c1 varchar(10)) SERVER some_server;

SELECT * FROM test_ft;
INSERT INTO test_ft VALUES (1, 'test');
UPDATE test_ft SET c1 = 'new_test';
DELETE FROM test_ft WHERE c0 = 1;
EXPLAIN SELECT * FROM test_ft;
EXPLAIN INSERT INTO test_ft VALUES (1, 'test');
ANALYZE VERBOSE test_ft;
