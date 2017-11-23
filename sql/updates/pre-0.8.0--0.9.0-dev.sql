-- Tablespace changes
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer[]);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer);

-- Chunk functions
DROP FUNCTION _timescaledb_internal.chunk_create(integer, integer, name, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_metadata(int);

-- Chunk constraint functions
DROP FUNCTION _timescaledb_internal.create_chunk_constraint(integer, oid);
DROP FUNCTION _timescaledb_internal.drop_constraint(integer, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_constraint(integer, name, boolean);
DROP FUNCTION _timescaledb_internal.chunk_constraint_drop_table_constraint(_timescaledb_catalog.chunk_constraint);

-- Dimension functions
DROP FUNCTION _timescaledb_internal.change_column_type(int, name, regtype);
DROP FUNCTION _timescaledb_internal.rename_column(int, name, name);
