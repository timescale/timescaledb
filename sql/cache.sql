-- This file contains infrastructure for cache invalidation of TimescaleDB
-- metadata caches kept in C. Please look at cache_invalidate.c for a
-- description of how this works.
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_hypertable();

-- This is pretty subtle. We create this dummy cache_inval_extension table
-- solely for the purpose of getting a relcache invalidation event when it is
-- deleted on DROP extension. It has no related triggers. When the table is
-- invalidated, all backends will be notified and will know that they must
-- invalidate all cached information, including catalog table and index OIDs,
-- etc.
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_extension();

-- not actually strictly needed but good for sanity as all tables should be dumped.
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_hypertable', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_extension', '');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.hypertable;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.hypertable
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk_constraint;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk_constraint
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension_slice;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension_slice
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();
