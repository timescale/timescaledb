CREATE OR REPLACE FUNCTION _timescaledb_catalog.get_partition_for_key(text, int) RETURNS smallint
	AS '$libdir/timescaledb', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT;
