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

-- Dimension and time functions
DROP FUNCTION _timescaledb_internal.change_column_type(int, name, regtype);
DROP FUNCTION _timescaledb_internal.rename_column(int, name, name);
DROP FUNCTION _timescaledb_internal.set_time_column_constraint(regclass, name);
DROP FUNCTION _timescaledb_internal.add_dimension(regclass, _timescaledb_catalog.hypertable, name, integer, anyelement, regproc, boolean, boolean);
DROP FUNCTION add_dimension(regclass, name, integer, anyelement, regproc);
DROP FUNCTION _timescaledb_internal.time_interval_specification_to_internal(regtype, anyelement, interval, text, boolean);
DROP FUNCTION _timescaledb_internal.time_interval_specification_to_internal_with_default_time(regtype, anyelement, text, boolean);
DROP FUNCTION _timescaledb_internal.create_hypertable(regclass, name, name, name, name, integer, name, name, bigint, name, boolean, regproc);
DROP FUNCTION set_chunk_time_interval(regclass, anyelement);
