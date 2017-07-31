CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_sfunc (state INTEGER[], val REAL, MIN REAL, MAX REAL, nbuckets INTEGER) 
RETURNS INTEGER[] 
AS '$libdir/timescaledb', 'hist_sfunc'
LANGUAGE C IMMUTABLE;

-- Tell Postgres how to use the new function
DROP AGGREGATE IF EXISTS histogram (REAL, REAL, REAL, INTEGER);
CREATE AGGREGATE histogram (REAL, REAL, REAL, INTEGER) (
       SFUNC = _timescaledb_internal.hist_sfunc,
       STYPE = INTEGER[]
);