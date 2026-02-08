-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- These tests verify Apache-licensed CREATE STATISTICS propagation behavior
-- on Continuous Aggregates and compressed chunks; they live under TSL tests
-- only because they need TSL to create those objects (CA, compression).

-- =============================================
-- Test 11: CREATE STATISTICS on Continuous Aggregate
-- =============================================
SELECT '=== Test 11: Continuous Aggregate propagation ===' AS test_name;

CREATE TABLE ca_stats_src (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER,
    temp FLOAT
);
SELECT create_hypertable('ca_stats_src', 'time', chunk_time_interval => INTERVAL '1 day');
INSERT INTO ca_stats_src VALUES
    ('2024-01-01 00:00:00', 1, 20.0),
    ('2024-01-02 00:00:00', 2, 21.0),
    ('2024-01-03 00:00:00', 3, 19.0);

CREATE MATERIALIZED VIEW ca_stats_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket,
       device_id,
       avg(temp) AS avg_temp
FROM ca_stats_src
GROUP BY 1, 2
WITH NO DATA;

CALL refresh_continuous_aggregate('ca_stats_daily', NULL, NULL);

CREATE STATISTICS ca_stat ON device_id, avg_temp FROM ca_stats_daily;

-- Stats must be on materialization hypertable + its chunks only (1 + N chunks)
SELECT count(*) AS ca_stat_count
FROM pg_statistic_ext s
WHERE s.stxname LIKE '%ca_stat%';

-- All such stats must be on the materialization hypertable or its chunks
SELECT s.stxrelid::regclass AS table_name, s.stxname
FROM pg_statistic_ext s
WHERE s.stxname LIKE '%ca_stat%'
ORDER BY s.stxrelid;

-- =============================================
-- Test 12: Compressed chunk skip (no stats on compressed chunk tables)
-- =============================================
SELECT '=== Test 12: Compressed chunk skip ===' AS test_name;

CREATE TABLE comp_skip (
    time TIMESTAMPTZ NOT NULL,
    a INTEGER,
    b INTEGER
);
SELECT create_hypertable('comp_skip', 'time', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE comp_skip SET (timescaledb.compress);

INSERT INTO comp_skip VALUES
    ('2024-01-01 00:00:00', 10, 100),
    ('2024-01-02 00:00:00', 20, 200);

-- Compress one chunk so we have one compressed chunk table
SELECT compress_chunk(c) FROM show_chunks('comp_skip') c LIMIT 1;

CREATE STATISTICS comp_skip_stat ON a, b FROM comp_skip;

-- Verify chunk tables: 3 (2 uncompressed + 1 compressed for the one we compressed)
SELECT count(*) AS chunk_table_count
FROM _timescaledb_catalog.chunk c
WHERE c.hypertable_id IN (
    (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'comp_skip' LIMIT 1),
    (SELECT compressed_hypertable_id FROM _timescaledb_catalog.hypertable WHERE table_name = 'comp_skip' LIMIT 1)
);

-- Total relations: 1 ht + 3 chunk tables = 4. Stat only on ht + 2 uncompressed chunks → expect 3.
SELECT count(*) AS comp_skip_stat_count
FROM pg_statistic_ext s
WHERE s.stxname LIKE '%comp_skip_stat%';

-- =============================================
-- Cleanup
-- =============================================
SELECT '=== Cleanup ===' AS test_name;

DROP MATERIALIZED VIEW IF EXISTS ca_stats_daily;
DROP TABLE IF EXISTS ca_stats_src CASCADE;
DROP TABLE IF EXISTS comp_skip CASCADE;

SELECT '=== All tests completed ===' AS test_name;
