SELECT _timescaledb_internal.add_constraint(h.id, c.oid)
FROM _timescaledb_catalog.hypertable h
INNER JOIN pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass);

DELETE FROM _timescaledb_catalog.hypertable_index hi
WHERE EXISTS (
 SELECT 1 FROM pg_constraint WHERE conindid = format('%I.%I', hi.main_schema_name, hi.main_index_name)::regclass
);

ALTER TABLE _timescaledb_catalog.chunk
DROP CONSTRAINT chunk_hypertable_id_fkey,
ADD CONSTRAINT chunk_hypertable_id_fkey
  FOREIGN KEY (hypertable_id) 
  REFERENCES _timescaledb_catalog.hypertable(id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_chunk_id_fkey,
ADD CONSTRAINT chunk_constraint_chunk_id_fkey
  FOREIGN KEY (chunk_id) 
  REFERENCES _timescaledb_catalog.chunk(id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey,
ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
  FOREIGN KEY (dimension_slice_id) 
  REFERENCES _timescaledb_catalog.dimension_slice(id);


DROP EVENT TRIGGER ddl_check_drop_command;

DROP TRIGGER trigger_main_on_change_chunk ON _timescaledb_catalog.chunk;

DROP FUNCTION _timescaledb_internal.chunk_create_table(int);
DROP FUNCTION _timescaledb_internal.ddl_process_drop_table();
DROP FUNCTION _timescaledb_internal.on_change_chunk();
DROP FUNCTION _timescaledb_internal.drop_hypertable(name, name);
