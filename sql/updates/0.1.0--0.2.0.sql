ALTER TABLE IF EXISTS _timescaledb_catalog.hypertable ADD UNIQUE (id, schema_name);

ALTER TABLE IF EXISTS _timescaledb_catalog.hypertable_index
DROP CONSTRAINT hypertable_index_hypertable_id_fkey,
ADD CONSTRAINT hypertable_index_hypertable_id_fkey
FOREIGN KEY (hypertable_id, main_schema_name)
REFERENCES _timescaledb_catalog.hypertable(id, schema_name)
ON UPDATE CASCADE
ON DELETE CASCADE;

ALTER TABLE IF EXISTS _timescaledb_catalog.chunk_index
DROP CONSTRAINT chunk_index_main_schema_name_fkey,
ADD CONSTRAINT chunk_index_main_schema_name_fkey
FOREIGN KEY (main_schema_name, main_index_name)
REFERENCES _timescaledb_catalog.hypertable_index(main_schema_name, main_index_name)
ON UPDATE CASCADE
ON DELETE CASCADE;
