-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Setup for testing sparse index migration fix where there was an orderby bug #9639
-- Test ALTER
CREATE TABLE fix_sparse_alter (created_at timestamptz NOT NULL,c1 float);
SELECT create_hypertable('fix_sparse_alter', 'created_at', chunk_time_interval => '6 hour'::interval);
ALTER TABLE fix_sparse_alter SET (timescaledb.compress, timescaledb.compress_orderby = 'created_at DESC');

-- Test default orderby with auto sparse indexes disabled
-- Requires >= 2.22.0 (auto_sparse_indexes GUC and sparse index feature)
SET timescaledb.auto_sparse_indexes = false;
CREATE TABLE fix_sparse_default(x int, value float);
SELECT create_hypertable('fix_sparse_default', 'x');
-- No explicit orderby — triggers compression_settings_set_defaults path
ALTER TABLE fix_sparse_default SET (timescaledb.compress);
INSERT INTO fix_sparse_default SELECT i, random() FROM generate_series(1, 10000) i;
SELECT count(compress_chunk(c)) FROM show_chunks('fix_sparse_default') c;
RESET timescaledb.auto_sparse_indexes;
