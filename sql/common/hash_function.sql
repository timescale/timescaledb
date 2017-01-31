CREATE OR REPLACE FUNCTION _iobeamdb_internal.murmur3_hash_string(text, int4) RETURNS int4
	AS '$libdir/iobeamdb', 'pg_murmur3_hash_string' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION hash_string(bytea, int4) RETURNS int4
	AS '$libdir/iobeamdb', 'pg_murmur3_hash_string' LANGUAGE C IMMUTABLE STRICT;
