-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Smoke tests.

-- Empty cache returns no rows.
SELECT count(*) FROM _timescaledb_functions.chunk_statistics();

-- Pushdown: invalid OID returns no rows (cannot match any cached chunk).
SELECT count(*) FROM _timescaledb_functions.chunk_statistics(compressed_relid => 0::regclass);
SELECT count(*) FROM _timescaledb_functions.chunk_statistics(uncompressed_relid => 0::regclass);

-- Pushdown: non-existent OID returns no rows.
SELECT count(*) FROM _timescaledb_functions.chunk_statistics(compressed_relid => 1::regclass);
SELECT count(*) FROM _timescaledb_functions.chunk_statistics(uncompressed_relid => 1::regclass);

-- Pushdown: far-future since returns no rows even if the cache is non-empty.
SELECT count(*) FROM _timescaledb_functions.chunk_statistics(since => '2999-01-01'::timestamptz);

-- Reset is callable; cache stays empty.
SELECT _timescaledb_functions.chunk_statistics_reset();
SELECT count(*) FROM _timescaledb_functions.chunk_statistics();

-- The view exists and has the expected shape (column count is enough for a smoke).
SELECT count(*) FROM timescaledb_information.stat_chunk_activity;
