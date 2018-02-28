-- Cache invalidation functions and triggers
DROP FUNCTION IF EXISTS _timescaledb_cache.invalidate_relcache_trigger();
DROP FUNCTION IF EXISTS _timescaledb_cache.invalidate_relcache(regclass);

-- Tablespace changes
DROP FUNCTION IF EXISTS _timescaledb_internal.select_tablespace(integer, integer[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.select_tablespace(integer, integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.select_tablespace(integer);

-- Chunk functions
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create(integer, integer, name, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunk_metadata(int);

-- Chunk constraint functions
DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_constraint(integer, oid);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_constraint(integer, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_chunk_constraint(integer, name, boolean);
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_constraint_drop_table_constraint(_timescaledb_catalog.chunk_constraint);

-- Dimension and time functions
DROP FUNCTION IF EXISTS _timescaledb_internal.change_column_type(int, name, regtype);
DROP FUNCTION IF EXISTS _timescaledb_internal.rename_column(int, name, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.set_time_column_constraint(regclass, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.add_dimension(regclass, _timescaledb_catalog.hypertable, name, integer, anyelement, regproc, boolean, boolean);
DROP FUNCTION IF EXISTS add_dimension(regclass, name, integer, anyelement, regproc);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_interval_specification_to_internal(regtype, anyelement, interval, text, boolean);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_interval_specification_to_internal_with_default_time(regtype, anyelement, text, boolean);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_hypertable(regclass, name, name, name, name, integer, name, name, bigint, name, boolean, regproc);
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc);
DROP FUNCTION IF EXISTS set_chunk_time_interval(regclass, anyelement);

-- Hypertable and related functions
DROP FUNCTION IF EXISTS _timescaledb_internal.set_time_columns_not_null();
DROP FUNCTION IF EXISTS _timescaledb_internal.create_schema(name);
DROP FUNCTION IF EXISTS _timescaledb_internal.check_role(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.attach_tablespace(name,regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_default_indexes(_timescaledb_catalog.hypertable,regclass,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_hypertable_schema(name);
DROP FUNCTION IF EXISTS _timescaledb_internal.detach_tablespace(name,regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.detach_tablespaces(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_type(regclass,name,boolean);
DROP FUNCTION IF EXISTS _timescaledb_internal.show_tablespaces(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.verify_hypertable_indexes(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.validate_triggers(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create_table(int, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_change_owner(oid, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.truncate_hypertable(name,name,boolean);
DROP FUNCTION IF EXISTS attach_tablespace(name,regclass);
DROP FUNCTION IF EXISTS detach_tablespace(name,regclass);

-- Remove redundant index
DROP INDEX IF EXISTS _timescaledb_catalog.dimension_slice_dimension_id_range_start_range_end_idx;

DROP FUNCTION IF EXISTS _timescaledb_internal.drop_hypertable(int,boolean);

DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN
(SELECT ds.id FROM _timescaledb_catalog.chunk_constraint cc
 RIGHT JOIN _timescaledb_catalog.dimension_slice ds
 ON (ds.id = cc.dimension_slice_id)
 WHERE dimension_slice_id IS NULL);

-- Post script
DROP FUNCTION IF EXISTS _timescaledb_internal.ddl_command_end();
