-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test for vectorized aggregation under ModifyHypertable plan node.
-- This validates that INSERT INTO hypertable SELECT ... GROUP BY ...
-- can use vectorized aggregation for the SELECT portion.

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

CREATE TABLE target_ht(device_id int NOT NULL, total bigint, cnt bigint);
SELECT create_hypertable('target_ht', 'device_id', chunk_time_interval => 10);

SET timescaledb.debug_require_vector_agg = 'require';

INSERT INTO target_ht
SELECT device_id, sum(value), count(*)
FROM source_ht
GROUP BY device_id;

RESET timescaledb.debug_require_vector_agg;

-- Verify data was inserted correctly
SELECT count(*) FROM target_ht;

-- Verify aggregated values are correct
SELECT device_id, total, cnt
FROM target_ht
ORDER BY device_id;

-- Cleanup
DROP TABLE target_ht;
DROP TABLE source_ht;
