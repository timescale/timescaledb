CREATE OR REPLACE FUNCTION _timescaledb_internal.median_numeric_finalfunc(state INTERNAL, var NUMERIC)
RETURNS NUMERIC
AS '@MODULE_PATHNAME@', 'median_numeric_finalfunc'
LANGUAGE C IMMUTABLE;

-- Tell Postgres how to use the new function
DROP AGGREGATE IF EXISTS median (NUMERIC);
CREATE AGGREGATE median (NUMERIC)
(
    SFUNC = array_agg_transfn,
    STYPE = INTERNAL,
    FINALFUNC = _timescaledb_internal.median_numeric_finalfunc,
    FINALFUNC_EXTRA
);
