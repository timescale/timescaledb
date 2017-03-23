-- cache invalidation proxy table
CREATE TABLE  _timescaledb_cache.cache_inval_hypertable();
CREATE TABLE  _timescaledb_cache.cache_inval_chunk();
-- This is pretty subtle. We create this dummy cache_inval_extension
-- table solely for the purpose of getting a relcache invalidation
-- event when it is deleted on DROP extension. It has no related
-- triggers. When the table is deleted, all backends will be notified
-- and will know that they must invalidate all cached information,
-- including catalog table and index OIDs, etc.
CREATE TABLE  _timescaledb_cache.cache_inval_extension();

--not actually strictly needed but good for sanity as all tables should be dumped.
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_hypertable', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_extension', '');

CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache_trigger()
 RETURNS TRIGGER AS '$libdir/timescaledb', 'invalidate_relcache_trigger' LANGUAGE C;

--for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(proxy_oid OID)
 RETURNS BOOLEAN AS '$libdir/timescaledb', 'invalidate_relcache' LANGUAGE C;

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.hypertable
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.partition_epoch
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.partition
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.hypertable_replica
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.default_replica_node
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

CREATE TRIGGER "0_cache_inval" AFTER UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_chunk');

CREATE TRIGGER "0_cache_inval_1" AFTER UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk_replica_node
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_chunk');

CREATE OR REPLACE FUNCTION _timescaledb_cache.extension_event_trigger()
RETURNS EVENT_TRIGGER AS '$libdir/timescaledb', 'extension_event_trigger' LANGUAGE C;

CREATE EVENT TRIGGER "0_extension_create" ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION')
EXECUTE PROCEDURE _timescaledb_cache.extension_event_trigger();

CREATE EVENT TRIGGER "0_extension_drop" ON ddl_command_start WHEN TAG IN ('DROP EXTENSION')
EXECUTE PROCEDURE _timescaledb_cache.extension_event_trigger();
