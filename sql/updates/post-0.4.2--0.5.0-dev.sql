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
CREATE TRIGGER trigger_main_on_change_chunk_constraint
AFTER UPDATE OR DELETE OR INSERT ON _timescaledb_catalog.chunk_constraint
FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_chunk_constraint();

SELECT _timescaledb_internal.add_constraint(h.id, c.oid)
FROM _timescaledb_catalog.hypertable h
INNER JOIN pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass);

DELETE FROM _timescaledb_catalog.hypertable_index hi
WHERE EXISTS (
 SELECT 1 FROM pg_constraint WHERE conindid = format('%I.%I', hi.main_schema_name, hi.main_index_name)::regclass
);

