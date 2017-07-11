ALTER TABLE _timescaledb_catalog.hypertable ADD UNIQUE (id, schema_name);
ALTER TABLE _timescaledb_catalog.hypertable_index DROP CONSTRAINT hypertable_index_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_index ADD CONSTRAINT hypertable_index_hypertable_id_fkey FOREIGN KEY (hypertable_id, main_schema_name) REFERENCES _timescaledb_catalog.hypertable(id, schema_name) ON UPDATE CASCADE ON DELETE CASCADE;
