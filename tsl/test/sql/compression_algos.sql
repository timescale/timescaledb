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

----------------------
-- Bool Compression --
----------------------

SELECT
  $$
  select item from base_bools order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_bools
\set TYPE boolean
\set COMPRESSION_CMD _timescaledb_internal.compress_bool(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::boolean)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::boolean)

-- bool test, flipping values betweem true and false, no nulls
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, (item%2=0)::boolean as item FROM (SELECT generate_series(1, 1000) item) sub;
\ir include/compression_test.sql
DROP TABLE base_bools;

-- bool test, all true values, no nulls
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, true as item FROM (SELECT generate_series(1, 1000) item) sub;
\ir include/compression_test.sql
DROP TABLE base_bools;

-- bool test, all false, no nulls
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, false as item FROM (SELECT generate_series(1, 1000) item) sub;
\ir include/compression_test.sql
DROP TABLE base_bools;

-- a single true element
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, true as item FROM (SELECT generate_series(1, 1) item) sub;
\ir include/compression_test.sql
DROP TABLE base_bools;

-- all true, except every 43rd value is null
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, ((NULLIF(i, (CASE WHEN i%43=0 THEN i ELSE -1 END)))>0)::boolean item FROM generate_series(1, 1000) i;
\ir include/compression_test.sql
DROP TABLE base_bools;

-- all false, except every 29th value is null
CREATE TABLE base_bools AS SELECT row_number() OVER() as rn, ((NULLIF(i, (CASE WHEN i%29=0 THEN i ELSE -1 END)))<0)::boolean item FROM generate_series(1, 1000) i;
\ir include/compression_test.sql
DROP TABLE base_bools;

----------------------
-- UUID Compression --
----------------------

CREATE TABLE uuid_set (i int, u uuid);
INSERT INTO uuid_set (i, u) VALUES
(1, '0197a7a9-b48b-7c70-be05-e376afc66ee1'), (2, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (3, '0197a7a9-b48b-7c72-bd57-49f981f064fd'), (4, '0197a7a9-b48b-7c73-b521-91172c2e770a'),
(5, '0197a7a9-b48b-7c74-a2a4-dcdbce635d11'), (6, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (7, '0197a7a9-b48b-7c76-b616-69e64a802b5c'), (8, '0197a7a9-b48b-7c77-b54f-c5f3d64d68d0'),
(9, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (10, '0197a7a9-b48b-7c79-92c7-7dde3bea6252'), (11, '0197a7a9-b48b-7c7a-8d9e-5afc3bf15234'), (12, '0197a7a9-b48b-7c7b-bc49-7150f16d8d63'),
(13, '0197a7a9-b48b-7c7c-aa7a-60d47bf04ff8'), (14, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (15, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (16, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
(17, '0197a7a9-b48b-7c80-a534-4eda33d89b41'), (18, '0197a7a9-b48b-7c81-abaf-e4d27888f6ea'), (19, '0197a7a9-b48b-7c82-a07c-bb5278039b67'), (20, '0197a7a9-b48b-7c83-9df3-2826632fcb42'),
(21, '0197a7a9-b48b-7c84-8588-dc4e6f10a5be'), (22, '0197a7a9-b48b-7c85-98a4-5ba69598ba88'), (23, '0197a7a9-b48b-7c86-9a96-69906846edf4'), (24, '0197a7a9-b48b-7c87-843a-d0c409e538d2'),
(25, '0197a7a9-b48b-7c88-a9c9-8e03283979dd'), (26, '0197a7a9-b48c-7c88-b962-5f38a5f5bb19'), (27, '0197a7a9-b48c-7c89-ad12-19c425cfe319'), (28, '0197a7a9-b48c-7c8a-9652-43b070f806b6'),
(29, '0197a7a9-b48c-7c8b-8b95-2e4b27b7c359'), (30, '0197a7a9-b48c-7c8c-9230-5a1b6a126d4e'), (31, '0197a7a9-b48c-7c8d-98e9-3622fe1418ae'), (32, '0197a7a9-b48c-7c8e-b262-e91dcf84f985'),
(33, '0197a7a9-b48c-7c8f-90c0-1036d19e438e'), (34, '0197a7a9-b48c-7c90-9fcb-f092518ae1e6'), (35, '0197a7a9-b48c-7c91-bf68-433e366f751d'), (36, '0197a7a9-b48c-7c92-95b6-82cb29498e5a'),
(37, '0197a7a9-b48c-7c93-9397-6ebbb9d4194d'), (38, '0197a7a9-b48c-7c94-8484-f47122e2dea3'), (39, '0197a7a9-b48c-7c95-a6e5-fe8d062f4e3c'), (40, '0197a7a9-b48c-7c96-914c-690b7930262f'),
(41, '0197a7a9-b48c-7c97-ac2b-473d61e0c396'), (42, '0197a7a9-b48c-7c98-93bd-ca093b30f6e8'), (43, '0197a7a9-b48c-7c99-b906-7fa2180536d3'), (44, '0197a7a9-b48c-7c9a-a090-fe01428ccefc'),
(45, '0197a7a9-b48c-7c9b-9319-de9dd58deeee'), (46, '0197a7a9-b48c-7c9c-a9d4-ed6f3e6a41b7'), (47, '0197a7a9-b48c-7c9d-8036-4141e0780323'), (48, '0197a7a9-b48c-7c9e-bfbe-f00eb49ed7f2'),
(49, '0197a7a9-b48c-7c9f-8ffe-71cf00a0c0c0'), (50, '0197a7a9-b48c-7ca0-822f-95ced2f95702'), (51, '0197a7a9-b48c-7ca1-8c8a-66582aec95fa'), (52, '0197a7a9-b48c-7ca2-95c3-fe80362a2251'),
(53, '0197a7a9-b48c-7ca3-855f-f681b254a8c8'), (54, '0197a7a9-b48c-7ca4-856b-a562eca93c3f'), (55, '0197a7a9-b48c-7ca5-a30e-37c247fb4c46'), (56, '0197a7a9-b48c-7ca6-9e78-a148d54a44ac'),
(57, '0197a7a9-b48c-7ca7-badb-4b650bf5bf5f'), (58, '0197a7a9-b48c-7ca8-8275-a8590869ef13'), (59, '0197a7a9-b48c-7ca9-b328-b54c3901223c'), (60, '0197a7a9-b48c-7caa-a15d-f5564e4e552c'),
(61, '0197a7a9-b48c-7cab-a4ac-017259746322'), (62, '0197a7a9-b48c-7cac-bda5-74ef12abd6b8'), (63, '0197a7a9-b48c-7cad-a3cd-0e4c93eaba80'), (64, '0197a7a9-b48c-7cae-9667-de4a226418df'),
(65, '0197a7a9-b48c-7caf-8aa2-067619170f32'), (66, '0197a7a9-b48c-7cb0-ba8c-91d9e8920845'), (67, '0197a7a9-b48c-7cb1-9681-a62bfffe9237'), (68, '0197a7a9-b48c-7cb2-b78b-037e5ee26ff6'),
(69, '0197a7a9-b48c-7cb3-ac27-e24382445188'), (70, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (71, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (72, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
(73, '0197a7a9-b48b-7c80-a534-4eda33d89b41'), (74, '0197a7a9-b48b-7c81-abaf-e4d27888f6ea'), (75, '0197a7a9-b48b-7c82-a07c-bb5278039b67'), (76, '0197a7a9-b48b-7c83-9df3-2826632fcb42'),
(77, '0197a7a9-b48b-7c84-8588-dc4e6f10a5be'), (78, '0197a7a9-b48b-7c85-98a4-5ba69598ba88'), (79, '0197a7a9-b48b-7c86-9a96-69906846edf4'), (80, '0197a7a9-b48b-7c87-843a-d0c409e538d2'),
(81, '0197a7a9-b48b-7c88-a9c9-8e03283979dd'), (82, '0197a7a9-b48c-7c88-b962-5f38a5f5bb19'), (83, '0197a7a9-b48c-7c89-ad12-19c425cfe319'), (84, '0197a7a9-b48c-7c8a-9652-43b070f806b6'),
(85, '0197a7a9-b48c-7c8b-8b95-2e4b27b7c359'), (86, '0197a7a9-b48c-7c8c-9230-5a1b6a126d4e'), (87, '0197a7a9-b48c-7c8d-98e9-3622fe1418ae'), (88, '0197a7a9-b48c-7c8e-b262-e91dcf84f985'),
(89, '0197a7a9-b48c-7c8f-90c0-1036d19e438e'), (90, '0197a7a9-b48c-7c90-9fcb-f092518ae1e6'), (91, '0197a7a9-b48c-7c91-bf68-433e366f751d'), (92, '0197a7a9-b48c-7c92-95b6-82cb29498e5a'),
(93, '0197a7a9-b48c-7c93-9397-6ebbb9d4194d'), (94, '0197a7a9-b48c-7c94-8484-f47122e2dea3'), (95, '0197a7a9-b48c-7c95-a6e5-fe8d062f4e3c'), (96, '0197a7a9-b48c-7c96-914c-690b7930262f'),
(97, '0197a7a9-b48c-7c97-ac2b-473d61e0c396'), (98, '0197a7a9-b48c-7c98-93bd-ca093b30f6e8'), (99, '0197a7a9-b48c-7c99-b906-7fa2180536d3'), (100, '0197a7a9-b48c-7c9a-a090-fe01428ccefc');

SELECT
  $$
  select item from base_uuids order by rn
  $$ AS "QUERY"
\gset
\set TABLE_NAME base_uuids
\set TYPE uuid
\set COMPRESSION_CMD _timescaledb_internal.compress_uuid(item)
\set DECOMPRESS_FORWARD_CMD _timescaledb_internal.decompress_forward(c::_timescaledb_internal.compressed_data, NULL::uuid)
\set DECOMPRESS_REVERSE_CMD _timescaledb_internal.decompress_reverse(c::_timescaledb_internal.compressed_data, NULL::uuid)

SET timescaledb.enable_uuid_compression = on;

-- basic test, flipping values between UUID v4s, UUID v7s and NULLs
CREATE TABLE base_uuids AS SELECT row_number() OVER() as rn, NULL::uuid as item FROM (SELECT generate_series(1, 1000) item) sub;
UPDATE base_uuids SET item = gen_random_uuid() WHERE rn % 4 = 0;
UPDATE base_uuids SET item = (SELECT u FROM uuid_set WHERE i = rn % 100) WHERE rn % 4 = 1;
UPDATE base_uuids SET item = (SELECT u FROM uuid_set WHERE i = rn % 97) WHERE rn % 4 = 2;
\ir include/compression_test.sql
DROP TABLE base_uuids;

-- all NULLs and a single UUID v7
CREATE TABLE base_uuids AS SELECT row_number() OVER() as rn, NULL::uuid as item FROM (SELECT generate_series(1, 1000) item) sub;
UPDATE base_uuids SET item = '0197a7a9-b48b-7c74-a2a4-dcdbce635d11' WHERE rn = 1;
\ir include/compression_test.sql
DROP TABLE base_uuids;

-- no NULLs
CREATE TABLE base_uuids AS SELECT row_number() OVER() as rn, NULL::uuid as item FROM (SELECT generate_series(1, 1000) item) sub;
UPDATE base_uuids SET item = (SELECT u FROM uuid_set WHERE i = rn % 100);
\ir include/compression_test.sql
DROP TABLE base_uuids;

-- flipping values between two UUID v7s and NULLs
CREATE TABLE base_uuids AS SELECT row_number() OVER() as rn, NULL::uuid as item FROM (SELECT generate_series(1, 1000) item) sub;
UPDATE base_uuids SET item = '0197a7a9-b48b-7c74-a2a4-dcdbce635d11' WHERE rn % 3 = 0;
UPDATE base_uuids SET item = '0197a7a9-b48b-7c74-a2a4-dcdbce635d12' WHERE rn % 3 = 1;
\ir include/compression_test.sql
DROP TABLE base_uuids;

-- test with a set of UUID v7s
CREATE TABLE base_uuids AS SELECT row_number() OVER() as rn, u as item FROM uuid_set ORDER BY i;
\ir include/compression_test.sql
DROP TABLE base_uuids;

RESET timescaledb.enable_uuid_compression;
DROP table uuid_set;

-----------------------------------------------
-- Interesting corrupt data found by fuzzing --
-----------------------------------------------

\c :TEST_DBNAME :ROLE_SUPERUSER

create or replace function ts_read_compressed_data_file(cstring, regtype, cstring, bool = true) returns int
as :TSL_MODULE_PATHNAME, 'ts_read_compressed_data_file' language c;

\set ON_ERROR_STOP 0
select ts_read_compressed_data_file('gorilla', 'float8', '--nonexistent');
-- Just some random file that returns "compressed data is corrupt", for one-off testing.
select ts_read_compressed_data_file('gorilla', 'float8', (:'TEST_INPUT_DIR' || '/fuzzing/compression/gorilla-float8/1f09f12d930daae8e5fd34e11e7b2303e1705b2e')::cstring);
\set ON_ERROR_STOP 1

create or replace function ts_read_compressed_data_directory(cstring, regtype, cstring, bool)
returns table(path text, bytes int, rows int, sqlstate text, location text)
as :TSL_MODULE_PATHNAME, 'ts_read_compressed_data_directory' language c;

\set fn 'ts_read_compressed_data_directory(:''algo'', :''type'', format(''%s/fuzzing/compression/%s-%s'', :''TEST_INPUT_DIR'', :''algo'', :''type'')::cstring, '

\set algo gorilla
\set type float8
select count(*)
    , coalesce((bulk.rows >= 0)::text, bulk.sqlstate) bulk_result
    , coalesce((rowbyrow.rows >= 0)::text, rowbyrow.sqlstate) rowbyrow_result
from :fn true) bulk join :fn false) rowbyrow using (path)
group by 2, 3 order by 1 desc
;

\set algo deltadelta
\set type int8
select count(*)
    , coalesce((bulk.rows >= 0)::text, bulk.sqlstate) bulk_result
    , coalesce((rowbyrow.rows >= 0)::text, rowbyrow.sqlstate) rowbyrow_result
from :fn true) bulk join :fn false) rowbyrow using (path)
group by 2, 3 order by 1 desc
;

\set algo array
\set type text
select count(*)
    , coalesce((bulk.rows >= 0)::text, bulk.sqlstate) bulk_result
    , coalesce((rowbyrow.rows >= 0)::text, rowbyrow.sqlstate) rowbyrow_result
from :fn true) bulk join :fn false) rowbyrow using (path)
group by 2, 3 order by 1 desc
;

\set algo dictionary
\set type text
select count(*)
    , coalesce((bulk.rows >= 0)::text, bulk.sqlstate) bulk_result
    , coalesce((rowbyrow.rows >= 0)::text, rowbyrow.sqlstate) rowbyrow_result
from :fn true) bulk join :fn false) rowbyrow using (path)
group by 2, 3 order by 1 desc
;

