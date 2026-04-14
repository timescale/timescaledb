-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test a table with auto sparse indexes
CREATE TABLE bloom (
    x     INT,
    v1    TEXT,
    u     UUID,
    ts    TIMESTAMP,
	-- There's an upgrade blocker on 2.27 for int2 bloom filter sparse indexes,
	-- so keep a smallint column w/o a bloom filter here as a cheap false positive
	-- test.
    si    SMALLINT
);

SELECT create_hypertable('bloom', 'x');

INSERT INTO bloom
SELECT
    x,
    md5(x::text),
    CASE
        WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid
    END,
    '2021-01-01'::timestamp + (INTERVAL '1 hour') * x,
    (x % 100)::smallint
FROM generate_series(1, 10000) x;

CREATE INDEX ON bloom USING brin(v1 text_bloom_ops);
CREATE INDEX ON bloom USING brin(u uuid_bloom_ops);
CREATE INDEX ON bloom USING brin(ts timestamp_minmax_ops);

ALTER TABLE bloom SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'v1',
    timescaledb.compress_orderby = 'x'
);

SELECT (split_part(extversion, '.', 1)::int,
      split_part(extversion, '.', 2)::int) >= (2, 22) AS has_configurable_sparse_index
FROM pg_extension WHERE extname = 'timescaledb' \gset

\if :has_configurable_sparse_index
ALTER TABLE bloom SET (timescaledb.sparse_index = 'minmax(si)');
\endif

SELECT COUNT(compress_chunk(x)) FROM show_chunks('bloom') x;

VACUUM FULL ANALYZE bloom;

SELECT * FROM _timescaledb_catalog.compression_settings;

SELECT
    schema_name || '.' || table_name AS chunk
FROM _timescaledb_catalog.chunk
WHERE id = (
    SELECT compressed_chunk_id
    FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = (
        SELECT id
        FROM _timescaledb_catalog.hypertable
        WHERE table_name = 'bloom'
    )
    LIMIT 1
)
\gset

\d+ :chunk
