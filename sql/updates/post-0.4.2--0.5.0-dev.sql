ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_create(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_create_after_lock(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(BIGINT, SMALLINT, BIGINT, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.dimension_calculate_default_range(INTEGER, BIGINT, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_calculate_new_ranges(INTEGER, BIGINT, INTEGER[], BIGINT[], BOOLEAN, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_id_get_by_dimensions(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get_dimensions_constraint_sql(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get_dimension_constraint_sql(INTEGER, BIGINT);

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create_after_lock(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range_closed(BIGINT, SMALLINT, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range(INTEGER, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_calculate_new_ranges(INTEGER, BIGINT, INTEGER[], BIGINT[], BOOLEAN, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_id_get_by_dimensions(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimensions_constraint_sql(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimension_constraint_sql(INTEGER, BIGINT) CASCADE;

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

DROP EVENT TRIGGER ddl_create_trigger;
DROP EVENT TRIGGER ddl_drop_trigger;

DROP FUNCTION _timescaledb_internal.add_trigger(int, oid);
DROP FUNCTION _timescaledb_internal.create_chunk_trigger(int, name, text);
DROP FUNCTION _timescaledb_internal.create_trigger_on_all_chunks(int, name, text);
DROP FUNCTION _timescaledb_internal.ddl_process_create_trigger();
DROP FUNCTION _timescaledb_internal.ddl_process_drop_trigger();
DROP FUNCTION _timescaledb_internal.drop_chunk_trigger(int, name);
DROP FUNCTION _timescaledb_internal.drop_trigger_on_all_chunks(INTEGER, NAME);
DROP FUNCTION _timescaledb_internal.get_general_trigger_definition(regclass);
DROP FUNCTION _timescaledb_internal.get_trigger_definition_for_table(INTEGER, text);
DROP FUNCTION _timescaledb_internal.need_chunk_trigger(int, oid);
