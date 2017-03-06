CREATE OR REPLACE FUNCTION gethostname() RETURNS TEXT
	AS '$libdir/timescaledb', 'pg_gethostname' LANGUAGE C IMMUTABLE STRICT;
