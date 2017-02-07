CREATE OR REPLACE FUNCTION gethostname() RETURNS TEXT
	AS '$libdir/iobeamdb', 'pg_gethostname' LANGUAGE C IMMUTABLE STRICT;