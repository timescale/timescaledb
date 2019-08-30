-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_segment_meta_min_max_append(internal, ANYELEMENT)
   RETURNS internal
   AS :TSL_MODULE_PATHNAME, 'tsl_segment_meta_min_max_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_segment_meta_min_max_finish(internal)
   RETURNS _timescaledb_internal.segment_meta_min_max
   AS :TSL_MODULE_PATHNAME, 'tsl_segment_meta_min_max_finish'
   LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE AGGREGATE _timescaledb_internal.segment_meta_min_max_agg(ANYELEMENT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.tsl_segment_meta_min_max_append,
    FINALFUNC = _timescaledb_internal.tsl_segment_meta_min_max_finish
);

\ir include/rand_generator.sql

--use a custom type without send and recv functions to test
--the input/output fallback path.
CREATE TYPE customtype_no_send_recv;

CREATE OR REPLACE FUNCTION customtype_in(cstring) RETURNS customtype_no_send_recv AS
'timestamptz_in'
LANGUAGE internal IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION customtype_out( customtype_no_send_recv) RETURNS cstring AS
'timestamptz_out'
LANGUAGE internal IMMUTABLE STRICT;

CREATE TYPE customtype_no_send_recv (
 INPUT = customtype_in,
 OUTPUT = customtype_out,
 INTERNALLENGTH = 8,
 PASSEDBYVALUE,
 ALIGNMENT = double,
 STORAGE = plain
);

CREATE CAST (customtype_no_send_recv AS bigint)
WITHOUT FUNCTION AS IMPLICIT;


\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE metric (i int);
insert into metric select i from generate_series(1, 10) i;

SELECT
    _timescaledb_internal.segment_meta_get_min(meta, NULL::int),
    _timescaledb_internal.segment_meta_get_max(meta, NULL::int),
    _timescaledb_internal.segment_meta_has_null(meta)
FROM
(
        SELECT
            _timescaledb_internal.segment_meta_min_max_agg(i) as meta
        FROM metric
) AS meta_gen;

\set TYPE int
\set TABLE metric
\ir include/compression_test_segment_meta.sql

----NULL tests
--First
truncate metric;
insert into metric select NULLIF(i,1) from generate_series(1, 10) i;
\ir include/compression_test_segment_meta.sql

--Last
truncate metric;
insert into metric select NULLIF(i,10) from generate_series(1, 10) i;
\ir include/compression_test_segment_meta.sql

--Middle
truncate metric;
insert into metric select NULLIF(i,5) from generate_series(1, 10) i;
\ir include/compression_test_segment_meta.sql

--All NULLS should return null object
truncate metric;
insert into metric select NULL from generate_series(1, 10) i;

SELECT
     _timescaledb_internal.segment_meta_min_max_agg(i) is NULL,
     _timescaledb_internal.segment_meta_min_max_agg(i)::text is NULL
FROM metric;

--accessor functions work on NULLs
SELECT
 _timescaledb_internal.segment_meta_get_min(NULL, NULL::int) IS NULL,
 _timescaledb_internal.segment_meta_get_max(NULL, NULL::int) IS NULL,
 _timescaledb_internal.segment_meta_has_null(NULL);



--
--type tests
--

--untoasted text
CREATE TABLE base_texts AS SELECT
    repeat(item::text, 1) as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series(1, 10) item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;

\set TYPE text
\set TABLE base_texts
\ir include/compression_test_segment_meta.sql

--toasted text
DROP TABLE base_texts;
CREATE TABLE base_texts AS SELECT
    repeat(item::text, 100000) as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series(1, 10) item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;
--make sure it's toasted
SELECT pg_total_relation_size(reltoastrelid)
      FROM pg_class c
      WHERE relname = 'base_texts';

\ir include/compression_test_segment_meta.sql

--name is a fixed-length pass by reference type
CREATE TABLE base_name AS SELECT
    item::name as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series(1, 10) item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;

\set TYPE name
\set TABLE base_name
\ir include/compression_test_segment_meta.sql


--array

CREATE TABLE text_array AS SELECT
    array[item::text, 'abab'] as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series(1, 10) item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;

\set TYPE text[]
\set TABLE text_array
\ir include/compression_test_segment_meta.sql

--Points doesn't have an ordering so make sure it errors
CREATE TABLE points AS SELECT
    point '(0,1)' as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series(1, 10) item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;

\set ON_ERROR_STOP 0
SELECT
     _timescaledb_internal.segment_meta_min_max_agg(i)
FROM points;
\set ON_ERROR_STOP 1

--test with a custom type with no send/recv

CREATE TABLE customtype_table AS SELECT
    item::text::customtype_no_send_recv as i
    FROM
        (SELECT sub.item from
            (SELECT generate_series('2001-01-01 01:01:01', '2001-01-02 01:01:01', INTERVAL '1 hour') item) as sub
          ORDER BY gen_rand_minstd()
        ) sub;

\set TYPE customtype_no_send_recv
\set TABLE customtype_table
\ir include/compression_test_segment_meta.sql
