CREATE OR REPLACE FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb', 'first_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb', 'first_combinefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/timescaledb', 'last_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/timescaledb', 'last_combinefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '$libdir/timescaledb', 'bookend_finalfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '$libdir/timescaledb', 'bookend_serializefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '$libdir/timescaledb', 'bookend_deserializefunc'
LANGUAGE C IMMUTABLE;

--This aggregate returns the "first" element of the first argument when ordered by the second argument.
--Ex. first(temp, time) returns the temp value for the row with the lowest time
DROP AGGREGATE IF EXISTS first(anyelement, "any");
CREATE AGGREGATE first(anyelement, "any") (
    SFUNC = _timescaledb_internal.first_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_internal.first_combinefunc,
    SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);

--This aggregate returns the "last" element of the first argument when ordered by the second argument.
--Ex. last(temp, time) returns the temp value for the row with highest time
DROP AGGREGATE IF EXISTS last(anyelement, "any");
CREATE AGGREGATE last(anyelement, "any") (
    SFUNC = _timescaledb_internal.last_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_internal.last_combinefunc,
    SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);


