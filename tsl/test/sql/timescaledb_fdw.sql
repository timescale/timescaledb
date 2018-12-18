-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

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
