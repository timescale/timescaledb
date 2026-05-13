-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Testcase for issue #9578: the composite bloom filter naming
-- allowed to generate the same column name for ('a_b', 'c') and ('a', 'b_c').

CREATE TABLE t (
      ts timestamptz NOT NULL,
      a_b int,
      c   int,
      a   int,
      b_c int
);

SELECT create_hypertable('t', 'ts');

ALTER TABLE t SET (
      timescaledb.compress,
      timescaledb.compress_orderby = 'ts',
      timescaledb.compress_index = 'bloom(a_b, c), bloom(a, b_c)'
);

INSERT INTO t VALUES ('2024-01-01', 1, 2, 3, 4);

SELECT compress_chunk(c) FROM show_chunks('t') c;

-- If the below error doesn't appear, then the bugfix worked:
-- ERROR:  column "_ts_meta_v2_bloomh_a_b_c" specified more than once

-- Check the bloom column names in the compressed chunk
SELECT
      relname,
      attname
FROM
      pg_attribute a,
      pg_class c
WHERE
      c.oid=a.attrelid AND
      attname LIKE '%_ts_meta%bloom%a_b_c%' AND
      relname LIKE '%chunk'
ORDER BY
      relname::text COLLATE "C",
      attname::text COLLATE "C";

-- Verify the assumption that the zero byte separator used in the
-- bloom column names is not allowed in Postgres column names:
-- 'a' || chr(0) || 'b' = 'a\u0000b'

\set ON_ERROR_STOP 0
CREATE TABLE t2 (
      ts timestamptz NOT NULL,
      U&"a\0000b" int,
      c int,
      a int,
      U&"b\0000c" int);

CREATE TABLE t3 (
      ts timestamptz NOT NULL,
      E'a\000b' int,
      c int,
      a int,
      E'b\000c' int);

DO $$
BEGIN
  EXECUTE format('CREATE TABLE t4 (%I int)', E'a\000b');
END $$;

\set ON_ERROR_STOP 1
