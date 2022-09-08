
-- gapfill with timezone support
DROP FUNCTION @extschema@.time_bucket_gapfill(INTERVAL,TIMESTAMPTZ,TEXT,TIMESTAMPTZ,TIMESTAMPTZ);

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id,compressed_chunk_id);

