
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

--- T-Digest Type, Aggregate, and Operators

-- T-Digest Shell Type
CREATE TYPE @extschema@.tdigest;

-- T-Digest Type Functions
CREATE FUNCTION _timescaledb_internal.tdigest_in(cstring)
RETURNS @extschema@.tdigest
AS '@MODULE_PATHNAME@', 'ts_tdigest_in_sql'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION _timescaledb_internal.tdigest_out(@extschema@.tdigest)
RETURNS cstring
AS '@MODULE_PATHNAME@', 'ts_tdigest_out_sql'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION _timescaledb_internal.tdigest_send(@extschema@.tdigest)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'ts_tdigest_send_sql'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION _timescaledb_internal.tdigest_recv(internal)
RETURNS @extschema@.tdigest
AS '@MODULE_PATHNAME@', 'ts_tdigest_recv_sql'
LANGUAGE C STRICT IMMUTABLE;

-- Complete T-Digest Type
CREATE TYPE @extschema@.tdigest (
  input = _timescaledb_internal.tdigest_in,
  output = _timescaledb_internal.tdigest_out,
  receive = _timescaledb_internal.tdigest_recv,
  send = _timescaledb_internal.tdigest_send,
  alignment = int4,
  storage = extended
);

-- T-Digest Aggregate support functions
CREATE FUNCTION _timescaledb_internal.tdigest_sfunc(internal, float8)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_sfunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_sfunc(internal, float8, integer)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_sfunc_comp_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_finalfunc(internal)
RETURNS @extschema@.tdigest
AS '@MODULE_PATHNAME@', 'ts_tdigest_finalfunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_serializefunc(internal)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'ts_tdigest_serializefunc_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_deserializefunc(bytea, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_deserializefunc_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_combinefunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- T-Digest Aggregates
CREATE AGGREGATE tdigest_create(value float8) (
      sfunc = _timescaledb_internal.tdigest_sfunc,
      stype = internal,
      serialfunc = _timescaledb_internal.tdigest_serializefunc,
      deserialfunc = _timescaledb_internal.tdigest_deserializefunc,
      combinefunc = _timescaledb_internal.tdigest_combinefunc,
      finalfunc = _timescaledb_internal.tdigest_finalfunc,
      parallel = safe
);

CREATE AGGREGATE tdigest_create(value float8, compression integer) (
      sfunc = _timescaledb_internal.tdigest_sfunc,
      stype = internal,
      serialfunc = _timescaledb_internal.tdigest_serializefunc,
      deserialfunc = _timescaledb_internal.tdigest_deserializefunc,
      combinefunc = _timescaledb_internal.tdigest_combinefunc,
      finalfunc = _timescaledb_internal.tdigest_finalfunc,
      parallel = safe
);

--- Public Facing API
CREATE FUNCTION percentile_approx(@extschema@.tdigest, float8)
RETURNS float8
AS '@MODULE_PATHNAME@', 'ts_tdigest_percentile_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cdf_approx(@extschema@.tdigest, float8)
RETURNS float8
AS '@MODULE_PATHNAME@', 'ts_tdigest_cdf_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Exact count of number of data points added to the TDigest
CREATE FUNCTION tdigest_count(@extschema@.tdigest)
RETURNS bigint
AS '@MODULE_PATHNAME@', 'ts_tdigest_count_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- User friendly output of key values in a T-Digest
CREATE FUNCTION tdigest_print(@extschema@.tdigest)
RETURNS varchar
AS '@MODULE_PATHNAME@', 'ts_tdigest_print_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- T-Digest Comparison Functions
CREATE FUNCTION _timescaledb_internal.tdigest_cmp(tdigest, tdigest)
RETURNS integer
AS '@MODULE_PATHNAME@', 'ts_tdigest_cmp_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_equal(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_equal_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_lt(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_lt_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_gt(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_gt_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_ge(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_ge_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION _timescaledb_internal.tdigest_le(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_le_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;


-- T-Digest Operator Class
CREATE OPERATOR < (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_lt,
    commutator = >,
    NEGATOR = >=
);

CREATE OPERATOR <= (
    leftarg = @extschema@.tdigest,
    rightarg = @extschema@.tdigest,
    procedure = _timescaledb_internal.tdigest_le,
    commutator = >=,
    NEGATOR = >
);

CREATE OPERATOR = (
    leftarg = @extschema@.tdigest,
    rightarg = @extschema@.tdigest,
    procedure = _timescaledb_internal.tdigest_equal,
    commutator = =,
    negator = !=
);

CREATE OPERATOR >= (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_ge,
    commutator = <=,
    NEGATOR = < 
);

CREATE OPERATOR > (
    LEFTARG = @extschema@.tdigest,
    RIGHTARG = @extschema@.tdigest,
    PROCEDURE = _timescaledb_internal.tdigest_gt,
    commutator = <,
    NEGATOR = <= 
);

CREATE OPERATOR CLASS @extschema@.tdigest_ops
    DEFAULT FOR TYPE @extschema@.tdigest USING btree AS
        OPERATOR 1 <,
        OPERATOR 2 <=,
        OPERATOR 3 =,
        OPERATOR 4 >=,
        OPERATOR 5 >,
        FUNCTION 1 _timescaledb_internal.tdigest_cmp(tdigest, tdigest);

