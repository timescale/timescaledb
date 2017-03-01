CREATE OR REPLACE FUNCTION _iobeamdb_catalog.get_partition_for_key(text, int) RETURNS smallint
	AS '$libdir/iobeamdb', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _iobeamdb_catalog.array_position_least(smallint[], smallint) RETURNS smallint
	AS '$libdir/iobeamdb', 'array_position_least' LANGUAGE C IMMUTABLE STRICT;
