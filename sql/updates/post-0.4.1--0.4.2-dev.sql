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
