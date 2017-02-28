CREATE OR REPLACE FUNCTION _timescaledb_catalog.get_partition_for_key(text, int) RETURNS smallint
	AS '$libdir/timescaledb', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_catalog.array_position_least(smallint[], smallint) RETURNS smallint
	AS '$libdir/timescaledb', 'array_position_least' LANGUAGE C IMMUTABLE STRICT;
