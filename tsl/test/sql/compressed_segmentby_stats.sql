-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Group count estimates for segmentby columns of compressed chunks.
-- The rows live in the compressed chunk, so the chunk and the hypertable
-- have no statistics of their own for these columns.

SET max_parallel_workers_per_gather = 0;

-- Estimated row count of the top plan node
CREATE FUNCTION est_rows(query text) RETURNS bigint LANGUAGE plpgsql AS
$$
DECLARE
  plan json;
BEGIN
  EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
  RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
END
$$;

-- 500 sensors, 20 rows each, one chunk
CREATE TABLE m (time timestamptz NOT NULL, sensor_id int NOT NULL, region int NOT NULL, val float8);
SELECT create_hypertable('m', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE m SET (timescaledb.compress,
                   timescaledb.compress_orderby = 'time',
                   timescaledb.compress_segmentby = 'sensor_id, region');

INSERT INTO m
SELECT '2024-01-01'::timestamptz + t * interval '1 minute', s, s % 5, s + t
FROM generate_series(1, 500) s, generate_series(0, 19) t;

SELECT count(compress_chunk(c)) FROM show_chunks('m') c;
VACUUM ANALYZE m;

-- Segmentby column, through the hypertable and on the chunk directly
SELECT est_rows('SELECT sensor_id, count(*) FROM m GROUP BY sensor_id');
SELECT format('SELECT sensor_id, count(*) FROM %s GROUP BY sensor_id', c) AS chunk_query
FROM show_chunks('m') c \gset
SELECT est_rows(:'chunk_query');

-- Two segmentby columns
SELECT est_rows('SELECT sensor_id, region, count(*) FROM m GROUP BY sensor_id, region');

-- Not a segmentby column: no estimate available
SELECT est_rows('SELECT val, count(*) FROM m GROUP BY val');

-- Three chunks with the same sensors: the group count stays at 500
INSERT INTO m
SELECT '2024-01-02'::timestamptz + d * interval '1 day' + t * interval '1 minute', s, s % 5, s + t
FROM generate_series(0, 1) d, generate_series(1, 500) s, generate_series(0, 19) t;

SELECT count(compress_chunk(c)) FROM show_chunks('m') c;
VACUUM ANALYZE m;

SELECT est_rows('SELECT sensor_id, count(*) FROM m GROUP BY sensor_id');

-- A partially compressed chunk falls back to the default estimate
INSERT INTO m VALUES ('2024-01-01 12:00', 1, 1, 1);
SELECT est_rows('SELECT sensor_id, count(*) FROM m GROUP BY sensor_id');
SELECT est_rows(:'chunk_query');

-- Real statistics are used as they are
CREATE TABLE plain (time timestamptz NOT NULL, sensor_id int NOT NULL);
SELECT create_hypertable('plain', 'time', chunk_time_interval => interval '1 day');
INSERT INTO plain
SELECT '2024-01-01'::timestamptz + t * interval '1 minute', s
FROM generate_series(1, 50) s, generate_series(0, 19) t;
VACUUM ANALYZE plain;

SELECT est_rows('SELECT sensor_id, count(*) FROM plain GROUP BY sensor_id');

DROP TABLE m;
DROP TABLE plain;
DROP FUNCTION est_rows;
