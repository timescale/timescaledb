CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit() RETURNS TEXT
	AS '$libdir/timescaledb', 'get_git_commit' LANGUAGE C IMMUTABLE STRICT;
