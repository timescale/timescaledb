-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Fix wrong migration by removing all sparse index configurations
-- which only contain auto sparse indexing definitions on hypertable
DO $$
DECLARE
    rec RECORD;
    num_config INT;
BEGIN
	IF NOT EXISTS (
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = '_timescaledb_catalog'
		AND table_name = 'compression_settings'
		AND column_name = 'index') THEN
		RETURN;
	END IF;

    FOR rec IN
        SELECT relid, compress_relid, index
        FROM _timescaledb_catalog.compression_settings
		WHERE compress_relid IS NULL
    LOOP
		num_config:=0;

		SELECT count(*)
		INTO num_config
		FROM jsonb_array_elements(rec.index) AS idx
		WHERE idx::jsonb @> '{"storage":"config"}'::jsonb;

		IF num_config = 0 THEN
            UPDATE _timescaledb_catalog.compression_settings
            SET index = NULL
            WHERE relid = rec.relid;
		END IF;
    END LOOP;
END $$;

-- Setup for testing sparse index migration fix where there was an orderby bug #9639
-- Test default
SET timescaledb.auto_sparse_indexes = false;
CREATE TABLE fix_sparse_default(x int, value float);
SELECT create_hypertable('fix_sparse_default', 'x');
-- No explicit orderby — triggers compression_settings_set_defaults path
ALTER TABLE fix_sparse_default SET (timescaledb.compress);
INSERT INTO fix_sparse_default SELECT i, random() FROM generate_series(1, 10000) i;
SELECT count(compress_chunk(c)) FROM show_chunks('fix_sparse_default') c;
-- Test CREATE
CREATE TABLE fix_sparse_create(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.partition_column='time',tsdb.order_by='value');
-- Test ALTER
CREATE TABLE fix_sparse_alter (created_at timestamptz NOT NULL,c1 float);
SELECT create_hypertable('fix_sparse_alter', 'created_at', chunk_time_interval => '6 hour'::interval);
ALTER TABLE fix_sparse_alter SET (timescaledb.compress, timescaledb.compress_orderby = 'created_at DESC');
