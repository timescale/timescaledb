-- this trigger function causes an invalidation event on the table whose name is
-- passed in as the first element.
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache_trigger()
RETURNS TRIGGER AS '$libdir/timescaledb', 'invalidate_relcache_trigger' LANGUAGE C STRICT;

-- This function is only used for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(catalog_table REGCLASS)
RETURNS BOOLEAN AS '$libdir/timescaledb', 'invalidate_relcache' LANGUAGE C STRICT;
