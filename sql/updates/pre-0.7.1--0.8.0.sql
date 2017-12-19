DROP FUNCTION _timescaledb_cache.invalidate_relcache(oid);


DROP FUNCTION set_chunk_time_interval(REGCLASS, BIGINT);
DROP FUNCTION add_dimension(REGCLASS, NAME, INTEGER, BIGINT, REGPROC);
DROP FUNCTION _timescaledb_internal.add_dimension(REGCLASS, _timescaledb_catalog.hypertable, NAME, INTEGER, BIGINT, REGPROC, BOOLEAN);
DROP FUNCTION _timescaledb_internal.time_interval_specification_to_internal(REGTYPE, anyelement, INTERVAL, TEXT);

-- Tablespace changes
DROP FUNCTION _timescaledb_internal.attach_tablespace(integer, name);
DROP FUNCTION attach_tablespace(regclass, name);
