-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_segment_meta_min_max_append(internal, ANYELEMENT)
   RETURNS internal
   AS :TSL_MODULE_PATHNAME, 'tsl_segment_meta_min_max_append'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_segment_meta_min_max_finish_max(internal, ANYELEMENT)
   RETURNS anyelement
   AS :TSL_MODULE_PATHNAME, 'tsl_segment_meta_min_max_finish_max'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_segment_meta_min_max_finish_min(internal, ANYELEMENT)
   RETURNS anyelement
   AS :TSL_MODULE_PATHNAME, 'tsl_segment_meta_min_max_finish_min'
   LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE _timescaledb_internal.segment_meta_min_max_agg_min(ANYELEMENT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.tsl_segment_meta_min_max_append,
    FINALFUNC = _timescaledb_internal.tsl_segment_meta_min_max_finish_min,
    FINALFUNC_EXTRA
);

CREATE AGGREGATE _timescaledb_internal.segment_meta_min_max_agg_max(ANYELEMENT) (
    STYPE = internal,
    SFUNC = _timescaledb_internal.tsl_segment_meta_min_max_append,
    FINALFUNC = _timescaledb_internal.tsl_segment_meta_min_max_finish_max,
    FINALFUNC_EXTRA
);

\ir include/rand_generator.sql

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE metric (i int);
insert into metric select i from generate_series(1, 10) i;

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
     _timescaledb_internal.segment_meta_min_max_agg_min(i) is null,
     _timescaledb_internal.segment_meta_min_max_agg_max(i) is null
FROM metric;

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
     _timescaledb_internal.segment_meta_min_max_agg_max(i)
FROM points;
SELECT
     _timescaledb_internal.segment_meta_min_max_agg_min(i)
FROM points;
\set ON_ERROR_STOP 1
