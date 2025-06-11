-- Make chunk_id use NULL to mark special entries instead of 0
-- (Invalid chunk) since that doesn't work with the FK constraint on
-- chunk_id.
ALTER TABLE _timescaledb_catalog.chunk_column_stats ALTER COLUMN chunk_id DROP NOT NULL;
UPDATE _timescaledb_catalog.chunk_column_stats SET chunk_id = NULL WHERE chunk_id = 0;
