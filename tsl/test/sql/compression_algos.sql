-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--install necessary functions for tests
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_test_compression() RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

------------------
-- C unit tests --
------------------

SELECT ts_test_compression();

\ir include/rand_generator.sql

------------------------
-- BIGINT Compression --
------------------------
SELECT
  $$
  select item from base_ints order by rn
  $$ AS "QUERY"
\gset
\set TYPE BIGINT
\set COMPRESSION_CMD _timescaledb_internal.compress_deltadelta(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::BIGINT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::BIGINT)

-- random order
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::bigint FROM (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql
DROP TABLE base_ints;

-- ascending order with nulls
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::bigint FROM (SELECT generate_series(1, 1000) item) sub;
INSERT INTO base_ints VALUES (0, NULL), (10, NULL), (10000, NULL);
\ir include/compression_test.sql

SELECT c ints_text FROM compressed;

DROP TABLE base_ints;

-- really big deltas
SELECT  9223372036854775807 as big_int_max \gset
SELECT -9223372036854775808	 as big_int_min \gset
CREATE TABLE base_ints AS SELECT row_number() over () as rn, item FROM
    (
        VALUES
           --big deltas
           (0), (:big_int_max), (:big_int_min), (:big_int_max), (:big_int_min),
           (0), (:big_int_min), (32), (5), (:big_int_min), (-52), (:big_int_max),
           (1000),
           --big delta_deltas
            (0), (:big_int_max), (:big_int_max), (:big_int_min), (:big_int_min), (:big_int_max), (:big_int_max),
            (0), (:big_int_max-1), (:big_int_max-1), (:big_int_min), (:big_int_min), (:big_int_max-1), (:big_int_max-1)
    ) as t(item);

\ir include/compression_test.sql
DROP TABLE base_ints;

-- NULLs
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, NULLIF(i, 5) item FROM generate_series(1::BIGINT, 10) i;
\ir include/compression_test.sql
DROP TABLE base_ints;

CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, NULLIF(i, 1) item FROM generate_series(1::BIGINT, 10) i;
\ir include/compression_test.sql
DROP TABLE base_ints;

CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, NULLIF(i, 10) item FROM generate_series(1::BIGINT, 10) i;
\ir include/compression_test.sql
DROP TABLE base_ints;

CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, NULLIF(NULLIF(NULLIF(NULLIF(i, 2), 4), 5), 8) item FROM generate_series(1::BIGINT, 10) i;
\ir include/compression_test.sql
DROP TABLE base_ints;


------------------------
-- INT Compression --
------------------------

--kinda silly test for now since compressed as bigint anyway
--TODO add proper int support
--TODO add proper smallint support and tests
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::int FROM (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY gen_rand_minstd()) sub;
SELECT
  $$
  select item::bigint from base_ints order by rn
  $$ AS "QUERY"
\gset
\set TYPE BIGINT
\set COMPRESSION_CMD _timescaledb_internal.compress_deltadelta(item::bigint)
\ir include/compression_test.sql
DROP TABLE base_ints;

-----------------------------
-- TIMESTAMPTZ Compression --
-----------------------------
SELECT
  $$
  select item from base_time order by rn
  $$ AS "QUERY"
\gset
\set TYPE TIMESTAMPTZ
\set COMPRESSION_CMD _timescaledb_internal.compress_deltadelta(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TIMESTAMPTZ)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TIMESTAMPTZ)

CREATE TABLE base_time AS SELECT row_number() OVER() as rn, item FROM
    (select sub.item from (SELECT generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') item) as sub ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql
DROP TABLE base_time;


------------------------
-- DOUBLE Compression --
------------------------

SELECT
  $$
  select item from  base_doubles order by rn
  $$ AS "QUERY"
\gset
SELECT 'DOUBLE PRECISION' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_gorilla(item)
SELECT '_timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::DOUBLE PRECISION)' AS "DECOMPRESS_FORWARD_CMD" \gset
SELECT '_timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::DOUBLE PRECISION)' AS "DECOMPRESS_REVERSE_CMD" \gset

CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, item::double precision FROM
    (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql
SELECT c gorilla_text FROM compressed;
DROP TABLE  base_doubles;

--special values
CREATE TABLE base_doubles AS SELECT row_number() over () as rn, item FROM
    (
        VALUES
           --special
           (0::double precision), ('Infinity'), ('-Infinity'), ('NaN'),
           --big deltas
           (0), ('Infinity'), ('-Infinity'), ('Infinity'), ('-Infinity'),
           (0), ('-Infinity'), (32), (5), ('-Infinity'), (-52), ('Infinity'),
           (1000),
           --big delta_deltas
            (0), ('Infinity'), ('Infinity'), ('-Infinity'), ('-Infinity'), ('Infinity'), ('Infinity')
    ) as t(item);
\ir include/compression_test.sql
DROP TABLE  base_doubles;

-- all 0s
CREATE TABLE base_doubles AS SELECT row_number() over () as rn, 0::FLOAT(50) as item FROM (SELECT generate_series(1, 1000) ) j;
\ir include/compression_test.sql
DROP TABLE  base_doubles;

-- NULLs
CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, NULLIF(i, 5)::DOUBLE PRECISION item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_doubles;

CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, NULLIF(i, 1)::DOUBLE PRECISION item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_doubles;

CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, NULLIF(i, 10)::DOUBLE PRECISION item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_doubles;

CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, NULLIF(NULLIF(NULLIF(NULLIF(i, 2), 4), 5), 8)::DOUBLE PRECISION item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_doubles;

------------------------
-- Dictionary Compression --
------------------------

SELECT
  $$
  select item from  base_texts order by rn
  $$ AS "QUERY"
\gset
SELECT 'TEXT' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_dictionary(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TEXT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TEXT)


-- high cardinality
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql
SELECT c from compressed;
DROP TABLE base_texts;


-- low cardinality
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (SELECT i as item FROM generate_series(1, 10) i, generate_series(1, 100) j ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql

DROP TABLE base_texts;

-- high cardinality with toasted values
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, repeat(item::text, 100000) as item FROM
    (select sub.item from (SELECT generate_series(1, 10) item) as sub ORDER BY gen_rand_minstd()) sub;
--make sure it's toasted
SELECT pg_total_relation_size(reltoastrelid)
      FROM pg_class c
      WHERE relname = 'base_texts';

\ir include/compression_test.sql
DROP TABLE base_texts;

-- NULLs
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 5)::TEXT item FROM generate_series(1, 10) i, generate_series(1, 100) j;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 1)::TEXT item FROM generate_series(1, 10) i, generate_series(1, 100) j;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 10)::TEXT item FROM generate_series(1, 10) i, generate_series(1, 100) j;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(NULLIF(NULLIF(NULLIF(i, 2), 4), 5), 8)::TEXT item FROM generate_series(1, 10) i, generate_series(1, 100) j;
\ir include/compression_test.sql
DROP TABLE base_texts;

------------------------
-- Arrayy Compression --
------------------------

SELECT
  $$
  select item from  base_texts order by rn
  $$ AS "QUERY"
\gset
SELECT 'TEXT' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_array(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TEXT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TEXT)

--basic test
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (select sub.item from (SELECT generate_series(1, 100) item) as sub ORDER BY gen_rand_minstd()) sub;
\ir include/compression_test.sql
SELECT c from compressed;
DROP TABLE base_texts;

-- toasted values
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, repeat(item::text, 100000) as item FROM
    (select sub.item from (SELECT generate_series(1, 10) item) as sub ORDER BY gen_rand_minstd()) sub;
--make sure it's toasted
SELECT pg_total_relation_size(reltoastrelid)
      FROM pg_class c
      WHERE relname = 'base_texts';

\ir include/compression_test.sql
DROP TABLE base_texts;

-- NULLs
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 5)::TEXT item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 1)::TEXT item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(i, 10)::TEXT item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_texts;

CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, NULLIF(NULLIF(NULLIF(NULLIF(i, 2), 4), 5), 8)::TEXT item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_texts;
