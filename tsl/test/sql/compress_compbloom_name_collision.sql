-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Regression test for #9578: composite bloom metadata column names must not
-- collide when the joined column list produces the same string after
-- flattening with the '_' separator.

-- Suppress the "poor compression ratio" WARNING that fires on the tiny test
-- data so the expected output is stable.
SET client_min_messages TO ERROR;

-- Helper view: list the bloom metadata columns on each compressed chunk so
-- tests can assert on what the generator actually emits.  The runner's
-- output cleaner (test/runner_cleanup_output.sh) replaces the
-- "_ts_meta_v2_bloomh_..." prefix with "regress-test-bloom_..." in the
-- captured output, so the hash prefix stays visible in the stable form.
CREATE VIEW bloom_cols AS
SELECT c.relname, a.attname
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname LIKE 'compress_hyper_%'
  AND a.attname LIKE '\_ts\_meta\_v2\_bloomh\_%' ESCAPE '\'
  AND NOT a.attisdropped
ORDER BY c.relname COLLATE "C", a.attname COLLATE "C";

-- Case 1: reporter's exact repro. bloom(a_b, c) and bloom(a, b_c) both join
-- to "a_b_c". Before the fix, compress_chunk would fail with
-- "column ... specified more than once".
CREATE TABLE t_9578 (
    ts  timestamptz NOT NULL,
    a_b int,
    c   int,
    a   int,
    b_c int
);
SELECT create_hypertable('t_9578', 'ts');
ALTER TABLE t_9578 SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a_b, c), bloom(a, b_c)'
);
INSERT INTO t_9578 VALUES ('2024-01-01', 1, 2, 3, 4);
SELECT compress_chunk(c) FROM show_chunks('t_9578') c;

-- Both composite bloom columns must exist with distinct names.  Display
-- them so a reader can see the hash prefix disambiguates the two specs.
SELECT * FROM bloom_cols;

DROP TABLE t_9578;

-- Case 2: three-column composites that collide under '_' flattening.
-- [a_b, c, d] vs [a, b_c, d] both flatten to "a_b_c_d".
CREATE TABLE t_9578_3col (
    ts  timestamptz NOT NULL,
    a_b int,
    c   int,
    a   int,
    b_c int,
    d   int
);
SELECT create_hypertable('t_9578_3col', 'ts');
ALTER TABLE t_9578_3col SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a_b, c, d), bloom(a, b_c, d)'
);
INSERT INTO t_9578_3col VALUES ('2024-01-01', 1, 2, 3, 4, 5);
SELECT compress_chunk(c) FROM show_chunks('t_9578_3col') c;

SELECT * FROM bloom_cols;

DROP TABLE t_9578_3col;

-- Case 3: single-column bloom names are unchanged by the fix -- no hash
-- prefix needed because there is no collision risk with a single column.
CREATE TABLE t_9578_single (
    ts  timestamptz NOT NULL,
    x   int
);
SELECT create_hypertable('t_9578_single', 'ts');
ALTER TABLE t_9578_single SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(x)'
);
INSERT INTO t_9578_single VALUES ('2024-01-01', 42);
SELECT compress_chunk(c) FROM show_chunks('t_9578_single') c;

SELECT * FROM bloom_cols;

DROP TABLE t_9578_single;

-- Case 4: verify the _timescaledb_functions.compressed_column_metadata_name
-- helper that the upgrade script uses is accessible and deterministic.
-- The post-fix name for a 2-column bloom includes a 4-char hex hash prefix
-- between the metadata type and the joined column names.
SELECT _timescaledb_functions.compressed_column_metadata_name('bloomh', ARRAY['a_b', 'c']) AS new_name_case1,
       _timescaledb_functions.compressed_column_metadata_name('bloomh', ARRAY['a', 'b_c']) AS new_name_case1_alt;

-- Single-column is unchanged (no hash prefix).
SELECT _timescaledb_functions.compressed_column_metadata_name('bloomh', ARRAY['x']) AS single_name;

DROP VIEW bloom_cols;
