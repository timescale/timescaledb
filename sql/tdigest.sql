/*
 * T-Digest aggregates and associated functionality
 */

-- We need to create the shell type first so that we can use tdigest in the support function signatures
CREATE TYPE tdigest;

-- do we need to make this strict?
CREATE FUNCTION tdigest_in(cstring)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_out(tdigest)
RETURNS cstring
AS 'MODULE_PATHNAME', 'tdigest_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_send(tdigest)
RETURNS bytea
AS 'MODULE_PATHNAME', 'tdigest_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_recv(internal)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE tdigest (
  input = tdigest_in,
  output = tdigest_out,
  receive = tdigest_recv,
  send = tdigest_send,
  alignment = int4,
  storage = extended
);

CREATE FUNCTION tdigest_empty()
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_empty(integer)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_compress(tdigest)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_compress'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_add(tdigest, float8)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_add(tdigest, float8, integer)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_addn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_cdf(tdigest, float8)
RETURNS float8
AS 'MODULE_PATHNAME', 'dist_cdf'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_quantile(tdigest, float8)
RETURNS float8
AS 'MODULE_PATHNAME', 'dist_quantile'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_trans(internal, float8)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_trans(internal, float8, integer)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_final(internal)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_agg_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'tdigest_serialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'tdigest_deserialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE dist_agg(float8) (
  sfunc = dist_agg_trans,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE dist_agg(float8, integer) (
  sfunc = dist_agg_trans,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE combine_dist_agg(internal) (
  sfunc = dist_combine,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_dist_agg(internal) (
  sfunc = dist_combine,
  stype = internal,
  finalfunc = tdigest_serialize,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);
