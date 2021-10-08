-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'
\echo '%%% RUNNING TESTS on table:' :TABLE_NAME
\echo '%%% PREFIX:' :PREFIX
\echo '%%% ORDER_BY_1:' :ORDER_BY_1
\echo '%%% ORDER_BY_1_2:' :ORDER_BY_1_2
\echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'

-- Test SkipScan with SELECT DISTINCT in multi-node environment
-- Ensure that a Unique plan gets chosen on the access node
SET enable_hashagg TO false;
\qecho Unique plan on access node for SELECT DISTINCT
:PREFIX
SELECT DISTINCT device_id
FROM :TABLE_NAME
:ORDER_BY_1
LIMIT 10;
RESET enable_hashagg;

SET timescaledb.enable_per_data_node_queries = true;
-- SELECT DISTINCT on expressions won't be pushed down
\qecho SELECT DISTINCT on expressions is not pushed down
:PREFIX
SELECT DISTINCT device_id*v1
FROM :TABLE_NAME
:ORDER_BY_1
LIMIT 10;

SET timescaledb.enable_remote_explain = ON;
-- SELECT DISTINCT on column with index should use SkipScan
\qecho SELECT DISTINCT on column with index uses SkipScan
:PREFIX
SELECT DISTINCT device_id
FROM :TABLE_NAME
:ORDER_BY_1
LIMIT 10;

-- SELECT DISTINCT with constants and NULLs in targetlist should use SkipScan
\qecho SELECT DISTINCT with constants and NULLs in targetlist uses SkipScan
:PREFIX
SELECT DISTINCT device_id, NULL, 'const1'
FROM :TABLE_NAME
:ORDER_BY_1
LIMIT 10;

-- SELECT DISTINCT with a mix of constants and columns should send only
-- the columns to the remote side. However SkipScan won't be used because
-- right now only single column is supported in SkipScans
\qecho SELECT DISTINCT only sends columns to the data nodes
:PREFIX
SELECT DISTINCT device_id, time, NULL, 'const1'
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10;

-- SELECT DISTINCT will be pushed down in the attribute attno order. This
-- is ok because "DISTINCT SELECT col1, col2" returns the same values
-- (subject to ORDER BY clauses) as "DISTINCE SELECT col2, col1"
\qecho SELECT DISTINCE is pushed down in attribute attno order
:PREFIX
SELECT DISTINCT device_id, time
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10;

-- SELECT DISTINCT ON on multiple columns will be pushed to the remote side.
-- However SkipScan won't be used since only one column is supported
\qecho SELECT DISTINCT ON multiple columns is pushed to data nodes
:PREFIX
SELECT DISTINCT ON (device_id, time) device_id, time
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10;

-- Another variation with SELECT DISTINCT
\qecho SELECT DISTINCT within a sub-select
:PREFIX
SELECT device_id, time, 'const1' FROM (SELECT DISTINCT ON (device_id) device_id, time
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10) a;

-- Ensure that SELECT DISTINCT pushdown happens even with below disabled
SET timescaledb.enable_per_data_node_queries = false;
\qecho SELECT DISTINCT works with enable_per_data_node_queries disabled
:PREFIX
SELECT DISTINCT device_id
FROM :TABLE_NAME
:ORDER_BY_1
LIMIT 10;

SET timescaledb.enable_per_data_node_queries = true;
SET timescaledb.enable_remote_explain = OFF;

-- SELECT DISTINCT should not have duplicate columns
\qecho SELECT DISTINCT should not have duplicate columns
:PREFIX
SELECT DISTINCT device_id, device_id
FROM :TABLE_NAME
:ORDER_BY_1;

-- SELECT DISTINCT handles whole row correctly
\qecho SELECT DISTINCT handles whole row correctly
:PREFIX
SELECT DISTINCT *
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10;

-- SELECT DISTINCT ON handles whole row correctly
\qecho SELECT DISTINCT ON (expr) handles whole row correctly
:PREFIX
SELECT DISTINCT ON (device_id) *
FROM :TABLE_NAME
ORDER BY device_id, time
LIMIT 10;

-- SELECT DISTINCT RECORD works correctly
\qecho SELECT DISTINCT RECORD works correctly
SET enable_hashagg TO false;
:PREFIX
SELECT DISTINCT :TABLE_NAME r
FROM :TABLE_NAME
ORDER BY r
LIMIT 10;
RESET enable_hashagg;

-- SELECT DISTINCT function is not pushed down
\qecho SELECT DISTINCT FUNCTION_EXPR not pushed down currently
:PREFIX
SELECT DISTINCT time_bucket('1h',time) col1
FROM :TABLE_NAME
ORDER BY col1
LIMIT 10;

-- SELECT DISTINCT without any var references is handled correctly
\qecho SELECT DISTINCT without any var references is handled correctly
:PREFIX
SELECT DISTINCT 1, 'constx'
FROM :TABLE_NAME;
