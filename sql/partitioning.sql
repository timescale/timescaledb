-- our default partitioning function.
-- returns a hash of val modulous the mod_factor
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val text, mod_factor int) RETURNS smallint
	AS '$libdir/timescaledb', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT;
