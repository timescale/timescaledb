-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

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
SELECT create_hypertable('customtype_test', 'time_custom', chunk_time_interval => 10e6::bigint, create_default_indexes=>false);

INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:03'::customtype, 10);
EXPLAIN (costs off) SELECT * FROM customtype_test;
INSERT INTO customtype_test VALUES ('2001-01-01 01:02:23'::customtype, 11);
EXPLAIN (costs off) SELECT * FROM customtype_test;

SELECT * FROM customtype_test;
