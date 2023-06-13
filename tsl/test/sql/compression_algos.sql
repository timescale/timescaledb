-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--install necessary functions for tests
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_test_compression() RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- helper function: float -> pseudorandom float [0..1].
create or replace function mix(x float4) returns float4 as $$ select ((hashfloat4(x) / (pow(2., 31) - 1) + 1) / 2)::float4 $$ language sql;
create or replace function mix(x timestamptz) returns float4 as $$ select mix(extract(epoch from x)::float4) $$ language sql;

------------------
-- C unit tests --
------------------

SELECT ts_test_compression();

------------------------
-- BIGINT Compression --
------------------------
SELECT
  $$
  select item from base_ints order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_ints
\set TYPE BIGINT
\set COMPRESSION_CMD _timescaledb_internal.compress_deltadelta(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::BIGINT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::BIGINT)

-- random order
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::bigint FROM (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
DROP TABLE base_ints;

-- ascending order with nulls
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::bigint FROM (SELECT generate_series(1, 1000) item) sub;
INSERT INTO base_ints VALUES (0, NULL), (10, NULL), (10000, NULL);
\ir include/compression_test.sql

SELECT c ints_text FROM compressed;

DROP TABLE base_ints;

-- single element
CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::bigint FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
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

CREATE TABLE base_ints AS SELECT row_number() OVER() as rn, item::int FROM (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY mix(item)) sub;
SELECT
  $$
  select item::bigint from base_ints order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_ints
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
\set TABLE_NAME base_time
\set COMPRESSION_CMD _timescaledb_internal.compress_deltadelta(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TIMESTAMPTZ)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TIMESTAMPTZ)

CREATE TABLE base_time AS SELECT row_number() OVER() as rn, item FROM
    (select sub.item from (SELECT generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
DROP TABLE base_time;


------------------------
-- FLOAT4 Compression --
------------------------
SELECT
  $$
  select item from base_floats order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_floats
SELECT 'real' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_gorilla(item)
SELECT '_timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::float4)' AS "DECOMPRESS_FORWARD_CMD" \gset
SELECT '_timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::float4)' AS "DECOMPRESS_REVERSE_CMD" \gset

CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, item::float4 FROM
    (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
SELECT c gorilla_text FROM compressed;
DROP TABLE  base_floats;

-- single element
CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, item::float4 FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
DROP TABLE base_floats;

--special values
CREATE TABLE base_floats AS SELECT row_number() over () as rn, item FROM
    (
        VALUES
           --special
           (0::float4), ('Infinity'), ('-Infinity'), ('NaN'),
           --big deltas
           (0), ('Infinity'), ('-Infinity'), ('Infinity'), ('-Infinity'),
           (0), ('-Infinity'), (32), (5), ('-Infinity'), (-52), ('Infinity'),
           (1000),
           --big delta_deltas
            (0), ('Infinity'), ('Infinity'), ('-Infinity'), ('-Infinity'), ('Infinity'), ('Infinity')
    ) as t(item);
\ir include/compression_test.sql
DROP TABLE  base_floats;

-- all 0s
CREATE TABLE base_floats AS SELECT row_number() over () as rn, 0::float4 as item FROM (SELECT generate_series(1, 1000) ) j;
\ir include/compression_test.sql
DROP TABLE  base_floats;

-- NULLs
CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, NULLIF(i, 5)::float4 item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_floats;

CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, NULLIF(i, 1)::float4 item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_floats;

CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, NULLIF(i, 10)::float4 item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_floats;

CREATE TABLE base_floats AS SELECT row_number() OVER() as rn, NULLIF(NULLIF(NULLIF(NULLIF(i, 2), 4), 5), 8)::float4 item FROM generate_series(1, 10) i;
\ir include/compression_test.sql
DROP TABLE base_floats;

------------------------
-- DOUBLE Compression --
------------------------

SELECT
  $$
  select item from  base_doubles order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_doubles
SELECT 'DOUBLE PRECISION' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_gorilla(item)
SELECT '_timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::DOUBLE PRECISION)' AS "DECOMPRESS_FORWARD_CMD" \gset
SELECT '_timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::DOUBLE PRECISION)' AS "DECOMPRESS_REVERSE_CMD" \gset

CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, item::double precision FROM
    (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
SELECT c gorilla_text FROM compressed;
DROP TABLE  base_doubles;

-- single element
CREATE TABLE base_doubles AS SELECT row_number() OVER() as rn, item::double precision FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
DROP TABLE base_doubles;

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
\set TABLE_NAME base_texts
SELECT 'TEXT' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_dictionary(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TEXT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TEXT)


-- high cardinality
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (select sub.item from (SELECT generate_series(1, 1000) item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
SELECT c from compressed;
DROP TABLE base_texts;


-- low cardinality
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (SELECT i as item FROM generate_series(1, 10) i, generate_series(1, 100) j ORDER BY mix(i + j)) sub;
\ir include/compression_test.sql

DROP TABLE base_texts;

-- single element
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
DROP TABLE base_texts;

-- high cardinality with toasted values
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, repeat(item::text, 100000) as item FROM
    (select sub.item from (SELECT generate_series(1, 10) item) as sub ORDER BY mix(item)) sub;
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

-----------------------
-- Array Compression --
-----------------------

SELECT
  $$
  select item from  base_texts order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_texts
SELECT 'TEXT' as "TYPE" \gset
\set COMPRESSION_CMD _timescaledb_internal.compress_array(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::TEXT)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::TEXT)

--basic test
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM
    (select sub.item from (SELECT generate_series(1, 100) item) as sub ORDER BY mix(item)) sub;
\ir include/compression_test.sql
SELECT c from compressed;
DROP TABLE base_texts;

-- single element
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, item::text FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
DROP TABLE base_texts;

-- toasted values
CREATE TABLE base_texts AS SELECT row_number() OVER() as rn, repeat(item::text, 100000) as item FROM
    (select sub.item from (SELECT generate_series(1, 10) item) as sub ORDER BY mix(item)) sub;
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

-----------------------------------------------
-- Interesting corrupt data found by fuzzing --
-----------------------------------------------

\c :TEST_DBNAME :ROLE_SUPERUSER

create or replace function ts_read_compressed_data_directory(cstring, regtype, cstring)
returns table(path text, bytes int, rows int, sqlstate text, location text)
as :TSL_MODULE_PATHNAME, 'ts_read_compressed_data_directory' language c;

select count(*), coalesce((rows >= 0)::text, sqlstate) result
from ts_read_compressed_data_directory('gorilla', 'float8', (:'TEST_INPUT_DIR' || '/fuzzing/compression/gorilla-float8')::cstring)
group by 2 order by 1 desc;

select count(*), coalesce((rows >= 0)::text, sqlstate) result
from ts_read_compressed_data_directory('deltadelta', 'int8', (:'TEST_INPUT_DIR' || '/fuzzing/compression/deltadelta-int8')::cstring)
group by 2 order by 1 desc;

create or replace function ts_read_compressed_data_file(cstring, regtype, cstring) returns int
as :TSL_MODULE_PATHNAME, 'ts_read_compressed_data_file' language c;

select ts_read_compressed_data_file('gorilla', 'float8', '--nonexistent');