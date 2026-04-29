-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- _timescaledb_functions.decompress_batch(record) RETURNS SETOF record
-- expands a single row of a compressed chunk into the user-visible rows it
-- was compressed from.

SET datestyle TO ISO;

CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, value float)
    WITH (tsdb.hypertable, tsdb.orderby = 'time', tsdb.segmentby = 'device_id');

INSERT INTO metrics
SELECT '2025-01-01'::timestamptz + (g || ' minute')::interval, g % 3, g::float
FROM generate_series(1, 30) g;

-- A row with NULLs in user columns to verify round-trip of NULLs.
INSERT INTO metrics VALUES ('2025-01-01 02:00', NULL, NULL);

SELECT count(*) AS source_rows FROM metrics;

SELECT count(compress_chunk(ch)) FROM show_chunks('metrics') ch;

-- Capture the compressed chunk relation name.
SELECT format('%I.%I', cc.schema_name, cc.table_name) AS compressed_chunk
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk cc ON c.compressed_chunk_id = cc.id
JOIN _timescaledb_catalog.hypertable h ON c.hypertable_id = h.id
WHERE h.table_name = 'metrics' \gset

-- Expanding every compressed row reproduces the source rowcount.
SELECT count(*) AS round_trip_count
FROM :compressed_chunk t,
     LATERAL _timescaledb_functions.decompress_batch(t)
         AS r(time timestamptz, device_id int, value float);

-- Set equality: no source row missing, no extra row introduced.
SELECT count(*) AS missing FROM (
    TABLE metrics
    EXCEPT ALL
    SELECT r.time, r.device_id, r.value
    FROM :compressed_chunk t,
         LATERAL _timescaledb_functions.decompress_batch(t)
             AS r(time timestamptz, device_id int, value float)
) m;

SELECT count(*) AS extras FROM (
    SELECT r.time, r.device_id, r.value
    FROM :compressed_chunk t,
         LATERAL _timescaledb_functions.decompress_batch(t)
             AS r(time timestamptz, device_id int, value float)
    EXCEPT ALL
    TABLE metrics
) e;

-- Per-batch counts: each compressed row is one segmentby batch.
SELECT t.device_id AS segment, count(*) AS batch_rows
FROM :compressed_chunk t,
     LATERAL _timescaledb_functions.decompress_batch(t)
         AS r(time timestamptz, device_id int, value float)
GROUP BY t.device_id
ORDER BY t.device_id NULLS LAST;

-- Decompressing a single batch yields exactly that batch.
SELECT r.time, r.device_id, r.value
FROM (SELECT t FROM :compressed_chunk t WHERE t.device_id = 1) one,
     LATERAL _timescaledb_functions.decompress_batch(one.t)
         AS r(time timestamptz, device_id int, value float)
ORDER BY r.time;

DROP TABLE metrics CASCADE;
