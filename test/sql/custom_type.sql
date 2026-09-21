-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SET client_min_messages TO WARNING;

CREATE OR REPLACE FUNCTION customtype_in(cstring) RETURNS customtype AS
'timestamptz_in'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION customtype_out(customtype) RETURNS cstring AS
'timestamptz_out'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION customtype_recv(internal) RETURNS customtype AS
'timestamptz_recv'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION customtype_send(customtype) RETURNS bytea AS
'timestamptz_send'
LANGUAGE internal IMMUTABLE STRICT;

SET client_min_messages TO DEFAULT;

CREATE TYPE customtype (
 INPUT = customtype_in,
 OUTPUT = customtype_out,
 RECEIVE = customtype_recv,
 SEND = customtype_send,
 LIKE = TIMESTAMPTZ
);

CREATE CAST (customtype AS bigint)
WITHOUT FUNCTION AS ASSIGNMENT;
CREATE CAST (bigint AS customtype)
WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (customtype AS timestamptz)
WITHOUT FUNCTION AS ASSIGNMENT;
CREATE CAST (timestamptz AS customtype)
WITHOUT FUNCTION AS ASSIGNMENT;

CREATE OR REPLACE FUNCTION customtype_lt(customtype, customtype) RETURNS bool AS
'timestamp_lt'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OPERATOR < (
	LEFTARG = customtype,
	RIGHTARG = customtype,
	PROCEDURE = customtype_lt,
	COMMUTATOR = >,
	NEGATOR = >=,
	RESTRICT = scalarltsel,
	JOIN = scalarltjoinsel
);

CREATE OR REPLACE FUNCTION customtype_ge(customtype, customtype) RETURNS bool AS
'timestamp_ge'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OPERATOR >= (
	LEFTARG = customtype,
	RIGHTARG = customtype,
	PROCEDURE = customtype_ge,
	COMMUTATOR = <=,
	NEGATOR = <,
	RESTRICT = scalargtsel,
	JOIN = scalargtjoinsel
);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE customtype_test(time_custom customtype, val int);
\set ON_ERROR_STOP 0
-- Using interval type for chunk time interval should fail with custom time type
SELECT create_hypertable('customtype_test', 'time_custom', chunk_time_interval => INTERVAL '1 day', create_default_indexes=>false);
\set ON_ERROR_STOP 1

SELECT create_hypertable('customtype_test', 'time_custom', chunk_time_interval => 10e6::bigint, create_default_indexes=>false);

INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
EXPLAIN (buffers off, costs off) SELECT * FROM customtype_test;
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:23'::customtype, 11);
EXPLAIN (buffers off, costs off) SELECT * FROM customtype_test;

SELECT * FROM customtype_test;

-- runtime chunk exclusion on a custom type compared through the operators of
-- bigint, which puts a relabel around the column
\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages TO WARNING;
CREATE TYPE myint;
CREATE FUNCTION myint_in(cstring) RETURNS myint AS 'int8in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION myint_out(myint) RETURNS cstring AS 'int8out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE myint (INPUT = myint_in, OUTPUT = myint_out, LIKE = bigint);
CREATE CAST (myint AS bigint) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (bigint AS myint) WITHOUT FUNCTION AS ASSIGNMENT;
RESET client_min_messages;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE myint_test(time myint NOT NULL, value int);
SELECT create_hypertable('myint_test', 'time', chunk_time_interval => 10);
INSERT INTO myint_test SELECT i::bigint, i FROM generate_series(0, 99) i;
ANALYZE myint_test;

-- only show the exclusion counts as the row counts vary between versions
CREATE FUNCTION exclusion_info(query text) RETURNS SETOF text LANGUAGE plpgsql AS
$$
DECLARE
  ln text;
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || query
  LOOP
    IF ln ~ 'ChunkAppend|excluded' THEN
      RETURN NEXT regexp_replace(ln, ' \(actual.*', '');
    END IF;
  END LOOP;
END;
$$;

SET enable_material TO off;
SET max_parallel_workers_per_gather TO 0;

SELECT exclusion_info($$
  SELECT (SELECT count(*) FROM myint_test t WHERE t.time >= v.x)
  FROM (VALUES (75::bigint), (85)) v(x)
$$);

SELECT (SELECT count(*) FROM myint_test t WHERE t.time >= v.x)
FROM (VALUES (75::bigint), (85)) v(x);

RESET enable_material;
RESET max_parallel_workers_per_gather;
DROP FUNCTION exclusion_info;
DROP TABLE myint_test;
