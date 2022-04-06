-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (ANALYZE,VERBOSE,SUMMARY OFF,TIMING OFF,COSTS OFF)'

\i include/modify_exclusion_load.sql

-- immutable constraints
-- should not have ChunkAppend since constraint is immutable and postgres already does the exclusion
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_int2 WHERE time = 15;
:PREFIX DELETE FROM metrics_int4 WHERE time = 15;
:PREFIX DELETE FROM metrics_int8 WHERE time = 15;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int2 SET value = 0.5 WHERE time = 15;
:PREFIX UPDATE metrics_int4 SET value = 0.5 WHERE time = 15;
:PREFIX UPDATE metrics_int8 SET value = 0.5 WHERE time = 15;
ROLLBACK;

-- stable constraints
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_int2 WHERE time = length(substring(version(),1,23));
:PREFIX DELETE FROM metrics_int4 WHERE time = length(substring(version(),1,23));
:PREFIX DELETE FROM metrics_int8 WHERE time = length(substring(version(),1,23));
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int2 SET value = 0.3 WHERE time = length(substring(version(),1,23));
:PREFIX UPDATE metrics_int4 SET value = 0.3 WHERE time = length(substring(version(),1,23));
:PREFIX UPDATE metrics_int8 SET value = 0.3 WHERE time = length(substring(version(),1,23));
ROLLBACK;

-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table, toplevel rows should be 1
BEGIN;
:PREFIX DELETE FROM metrics_int2 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_int4 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_int8 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int2 SET value = 0.4 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX UPDATE metrics_int4 SET value = 0.4 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX UPDATE metrics_int8 SET value = 0.4 WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int2 SET value = 0.1 * value, data = 'update' WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX UPDATE metrics_int4 SET value = 0.1 * value, data = 'update' WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
:PREFIX UPDATE metrics_int8 SET value = 0.1 * value, data = 'update' WHERE time = length(substring(version(),1,20)) RETURNING 'returning', time;
ROLLBACK;

-- immutable constraints
-- should not have ChunkAppend since constraint is immutable and postgres already does the exclusion
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01';
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01' AND device = '1';
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01';
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_date SET value = 0.6 WHERE time = '2000-01-01';
:PREFIX UPDATE metrics_timestamp SET value = 0.6 WHERE time = '2000-01-01';
:PREFIX UPDATE metrics_timestamptz SET value = 0.6 WHERE time = '2000-01-01';
:PREFIX UPDATE metrics_space SET value = 0.6 WHERE time = '2000-01-01' AND device = '1';
:PREFIX UPDATE metrics_space SET value = 0.6 WHERE time = '2000-01-01';
ROLLBACK;

-- stable constraints
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01'::text::date;
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01'::text::timestamp;
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01'::text::timestamptz;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_date SET value = 0.9 WHERE time = '2000-01-01'::text::date;
:PREFIX UPDATE metrics_timestamp SET value = 0.9 WHERE time = '2000-01-01'::text::timestamp;
:PREFIX UPDATE metrics_timestamptz SET value = 0.9 WHERE time = '2000-01-01'::text::timestamptz;
ROLLBACK;

-- space partitioning
-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table
BEGIN;
:PREFIX DELETE FROM metrics_space WHERE time = '2000-01-01'::text::timestamptz AND device = format('1');
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_space WHERE device = format('1');
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_space SET value = 0.1 WHERE time = '2000-01-01'::text::timestamptz AND device = format('1');
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_space SET value = 0.1 WHERE device = format('1');
ROLLBACK;

-- should have ChunkAppend since constraint is stable
-- should only hit 1 chunk and base table, toplevel rows should be 1
BEGIN;
:PREFIX DELETE FROM metrics_date WHERE time = '2000-01-01'::text::date RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_timestamp WHERE time = '2000-01-01'::text::timestamp RETURNING 'returning', time;
:PREFIX DELETE FROM metrics_timestamptz WHERE time = '2000-01-01'::text::timestamptz RETURNING 'returning', time;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_date SET value = 0.2 WHERE time = '2000-01-01'::text::date RETURNING 'returning', time;
:PREFIX UPDATE metrics_timestamp SET value = 0.2 WHERE time = '2000-01-01'::text::timestamp RETURNING 'returning', time;
:PREFIX UPDATE metrics_timestamptz SET value = 0.2 WHERE time = '2000-01-01'::text::timestamptz RETURNING 'returning', time;
ROLLBACK;

-- subselects
-- no chunk exclusion for subqueries joins atm
BEGIN;
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2) AND time < length(version());
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version()));
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_int4 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version())) AND time < length(version());
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 SET value = 0.1 WHERE time IN (SELECT time FROM metrics_int2) AND time < length(version());
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 SET value = 0.1 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version()));
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 SET value = 0.1 WHERE time IN (SELECT time FROM metrics_int2 WHERE time < length(version())) AND time < length(version());
ROLLBACK;

-- join
-- no chunk exclusion for subqueries joins atm
BEGIN;
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2;
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2 WHERE m4.time = m2.time;
ROLLBACK;
BEGIN;
:PREFIX DELETE FROM metrics_int4 m4 USING metrics_int2 m2 WHERE m4.time = m2.time AND m4.time < length(version());
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 m4 SET value = 0.15 FROM metrics_int2 m2;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 m4 SET value = 0.15 FROM metrics_int2 m2 WHERE m4.time = m2.time;
ROLLBACK;
BEGIN;
:PREFIX UPDATE metrics_int4 m4 SET value = 0.15 FROM metrics_int2 m2 WHERE m4.time = m2.time AND m4.time < length(version());
ROLLBACK;

-- cte
-- should all have chunkappend nodes
BEGIN;
:PREFIX WITH d AS (DELETE FROM metrics_int4 m4 WHERE m4.time < length(version()) RETURNING time)
SELECT * FROM d;
ROLLBACK;
BEGIN;
:PREFIX WITH d AS (DELETE FROM metrics_int4 m4 WHERE m4.time < length(version()))
DELETE FROM metrics_int2 m2 WHERE m2.time < length(version());
ROLLBACK;
BEGIN;
:PREFIX WITH u AS (UPDATE metrics_int4 m4 SET data = 'cte update' WHERE m4.time < length(version()) RETURNING time)
SELECT * FROM u;
ROLLBACK;
BEGIN;
:PREFIX WITH u AS (UPDATE metrics_int4 m4 SET data = 'cte update 1' WHERE m4.time < length(version()))
UPDATE metrics_int2 m2 SET data = 'cte update 2' WHERE m2.time < length(version());
ROLLBACK;
BEGIN;
:PREFIX WITH d AS (DELETE FROM metrics_int4 m4 WHERE m4.time < length(version()) RETURNING time)
UPDATE metrics_int2 m2 SET data = 'cte update' WHERE m2.time < length(version());
ROLLBACK;
BEGIN;
:PREFIX WITH u AS (UPDATE metrics_int4 m4 SET data = 'cte update' WHERE m4.time < length(version()) RETURNING time)
DELETE FROM metrics_int2 m2 WHERE m2.time < length(version());
ROLLBACK;

-- test interaction with compression
-- with chunk exclusion for compressed chunks operations that would
-- error because they hit compressed chunks before can succeed now
-- if those chunks get excluded

-- delete from uncompressed chunks with non-immutable constraints
BEGIN;
:PREFIX DELETE FROM metrics_compressed WHERE time > '2005-01-01'::text::timestamptz;
ROLLBACK;

-- update uncompressed chunks with non-immutable constraints
BEGIN;
:PREFIX UPDATE metrics_compressed SET value = 2 * value WHERE time > '2005-01-01'::text::timestamptz;
ROLLBACK;
