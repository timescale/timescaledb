-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test for vectorized aggregation under ModifyHypertable plan node.
-- This validates that INSERT INTO hypertable SELECT ... GROUP BY ...
-- can use vectorized aggregation for the SELECT portion.
--
-- PR #9137 adds ModifyHypertable to the list of custom scan nodes
-- that try_insert_vector_agg_node() recurses into.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Create a source hypertable with compressed data
CREATE TABLE source_ht(time int NOT NULL, device_id int, value int);
SELECT create_hypertable('source_ht', 'time', chunk_time_interval => 10000);

INSERT INTO source_ht
SELECT t, d, t * 10 + d
FROM generate_series(1, 20000) t,
     generate_series(1, 5) d;

ALTER TABLE source_ht SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time'
);

SELECT count(compress_chunk(ch)) FROM show_chunks('source_ht') ch;
VACUUM ANALYZE source_ht;

-- Create a target hypertable for aggregated data
-- Use device_id as the time column so GROUP BY device_id works
CREATE TABLE target_ht(device_id int NOT NULL, total bigint, cnt bigint);
SELECT create_hypertable('target_ht', 'device_id', chunk_time_interval => 10);

-- First verify a simple SELECT uses VectorAgg (baseline test)
EXPLAIN (costs off)
SELECT sum(value) FROM source_ht;

-- Now verify INSERT INTO hypertable SELECT ... GROUP BY also uses VectorAgg
-- The plan should show:
--   Custom Scan (ModifyHypertable)
--     -> Insert
--          -> Finalize GroupAggregate
--               -> Merge Append
--                    -> Custom Scan (VectorAgg)  <-- THIS is what PR #9137 enables
--
-- Without PR #9137, we would see Partial GroupAggregate instead of VectorAgg

EXPLAIN (costs off)
INSERT INTO target_ht
SELECT device_id, sum(value), count(*)
FROM source_ht
GROUP BY device_id;

-- Execute the INSERT to verify correctness
INSERT INTO target_ht
SELECT device_id, sum(value), count(*)
FROM source_ht
GROUP BY device_id;

-- Verify data was inserted correctly
SELECT count(*) FROM target_ht;

-- Verify aggregated values are correct
SELECT device_id, total, cnt
FROM target_ht
ORDER BY device_id;

-- Cleanup
DROP TABLE target_ht;
DROP TABLE source_ht;
