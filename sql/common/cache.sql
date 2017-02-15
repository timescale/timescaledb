-- cache invalidation proxy table
CREATE TABLE  _iobeamdb_cache.cache_inval_hypertable();
CREATE TABLE  _iobeamdb_cache.cache_inval_chunk();

--not actually strictly needed but good for sanity as all tables should be dumped.
SELECT pg_catalog.pg_extension_config_dump('_iobeamdb_cache.cache_inval_hypertable', '');
SELECT pg_catalog.pg_extension_config_dump('_iobeamdb_cache.cache_inval_chunk', '');

CREATE OR REPLACE FUNCTION _iobeamdb_cache.invalidate_relcache_trigger()
 RETURNS TRIGGER AS '$libdir/iobeamdb', 'invalidate_relcache_trigger' LANGUAGE C;

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _iobeamdb_catalog.hypertable
FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _iobeamdb_catalog.partition_epoch
FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _iobeamdb_catalog.partition
FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _iobeamdb_catalog.chunk
FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_cache.invalidate_relcache_trigger('cache_inval_chunk');

CREATE TRIGGER "0_cache_inval_1" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _iobeamdb_catalog.chunk_replica_node
FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_cache.invalidate_relcache_trigger('cache_inval_chunk');
