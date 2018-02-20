-- This function is only used for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(catalog_table REGCLASS)
RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'invalidate_relcache' LANGUAGE C STRICT;

