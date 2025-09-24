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
