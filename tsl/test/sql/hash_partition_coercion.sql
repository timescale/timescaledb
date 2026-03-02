-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test hash partition chunk exclusion with cross-type comparisons

-- wrong result: text column + name literal
CREATE TABLE hash_text(time timestamptz NOT NULL, device text);
SELECT create_hypertable('hash_text', 'time');
SELECT add_dimension('hash_text', 'device', number_partitions => 3);
INSERT INTO hash_text VALUES ('2000-01-01', 'abc');
SELECT count(*) FROM hash_text WHERE device = 'abc'::name;
DROP TABLE hash_text;

-- x86_64 wrong result: int4 time column + large int8 literal + range query
-- 4294967196::int8 truncates to -100 as signed int4
-- Chunk exclusion uses time < -100, excluding all positive-time chunks
CREATE FUNCTION time_part_int4(val int4) RETURNS int4 AS $$ SELECT val $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE time_int4(time int4 NOT NULL, v int);
SELECT create_hypertable('time_int4', 'time', chunk_time_interval => 100, time_partitioning_func => 'time_part_int4');
INSERT INTO time_int4 VALUES (100, 1), (200, 2);
-- Both rows satisfy time < 4294967196, but bug truncates to time < -100
SELECT count(*) FROM time_int4 WHERE time < 4294967196::int8;
DROP TABLE time_int4;
DROP FUNCTION time_part_int4;

-- i386 crash: int8 time column + int4 literal + custom partitioning
-- On i386: SEGFAULT (DatumGetInt64 dereferences byval int4 as pointer)
-- On x86_64: works by coincidence (both int4 and int8 are byval)
CREATE FUNCTION time_part_int8(val int8) RETURNS int8 AS $$ SELECT val $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE time_int8(time int8 NOT NULL, v int);
SELECT create_hypertable('time_int8', 'time', chunk_time_interval => 10, time_partitioning_func => 'time_part_int8');
INSERT INTO time_int8 VALUES (1, 1), (11, 2), (21, 3);
SELECT count(*) FROM time_int8 WHERE time = 1::int4;
DROP TABLE time_int8;
DROP FUNCTION time_part_int8;

-- Exact type match: text column + text literal (no coercion needed)
CREATE TABLE hash_text_exact(time timestamptz NOT NULL, device text);
SELECT create_hypertable('hash_text_exact', 'time');
SELECT add_dimension('hash_text_exact', 'device', number_partitions => 3);
INSERT INTO hash_text_exact VALUES ('2000-01-01', 'abc');
SELECT count(*) FROM hash_text_exact WHERE device = 'abc'::text;
DROP TABLE hash_text_exact;

-- Binary compatible types: text column + varchar literal
-- PostgreSQL coerces varchar to text at parse time
CREATE TABLE hash_text_varchar(time timestamptz NOT NULL, device text);
SELECT create_hypertable('hash_text_varchar', 'time');
SELECT add_dimension('hash_text_varchar', 'device', number_partitions => 3);
INSERT INTO hash_text_varchar VALUES ('2000-01-01', 'abc');
SELECT count(*) FROM hash_text_varchar WHERE device = 'abc'::varchar;
DROP TABLE hash_text_varchar;

-- Array coercion: text column + name[] array (ScalarArrayOpExpr)
-- Test both ANY (OR) and ALL (AND) semantics
CREATE TABLE hash_text_array(time timestamptz NOT NULL, device text);
SELECT create_hypertable('hash_text_array', 'time');
SELECT add_dimension('hash_text_array', 'device', number_partitions => 3);
INSERT INTO hash_text_array VALUES ('2000-01-01', 'abc'), ('2000-01-01', 'def'), ('2000-01-01', 'ghi');
-- OR: match any element
SELECT count(*) FROM hash_text_array WHERE device = ANY(ARRAY['abc', 'def']::name[]);
-- AND: match all elements (logically empty for different values, but exercises code path)
SELECT count(*) FROM hash_text_array WHERE device = ALL(ARRAY['abc', 'def']::name[]);
-- AND: single element (equivalent to =)
SELECT count(*) FROM hash_text_array WHERE device = ALL(ARRAY['abc']::name[]);
DROP TABLE hash_text_array;

-- Time dimension with SAOP + type coercion (int4 column, int8 array)
-- Note: open (time/range) dimensions can't use SAOP with multiple OR values
-- for chunk exclusion, but AND with single effective bound works.
-- These tests verify correct results and that AND cases use chunk exclusion.
CREATE FUNCTION time_part_int4_saop(val int4) RETURNS int4 AS $$ SELECT val $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE time_int4_saop(time int4 NOT NULL, v int);
SELECT create_hypertable('time_int4_saop', 'time', chunk_time_interval => 100, time_partitioning_func => 'time_part_int4_saop');
INSERT INTO time_int4_saop VALUES (50, 1), (150, 2), (250, 3);
-- AND: time < ALL(array) means time < min(array) = 100
-- Single effective bound, chunk exclusion should work
SELECT count(*) FROM time_int4_saop WHERE time < ALL(ARRAY[100, 200]::int8[]);
-- AND: time > ALL(array) means time > max(array) = 200
SELECT count(*) FROM time_int4_saop WHERE time > ALL(ARRAY[100, 200]::int8[]);
-- OR cases: chunk exclusion not used (multiple OR values rejected),
-- but results must still be correct
SELECT count(*) FROM time_int4_saop WHERE time < ANY(ARRAY[100, 200]::int8[]);
SELECT count(*) FROM time_int4_saop WHERE time > ANY(ARRAY[100, 200]::int8[]);
DROP TABLE time_int4_saop;
DROP FUNCTION time_part_int4_saop;

-- Prepared statement with varchar parameter, text column
-- Custom plan: coercion at plan time, chunk exclusion works
-- Generic plan: no chunk exclusion (param unknown), but correct result
CREATE TABLE hash_prep(time timestamptz NOT NULL, device text);
SELECT create_hypertable('hash_prep', 'time');
SELECT add_dimension('hash_prep', 'device', number_partitions => 3);
INSERT INTO hash_prep VALUES ('2000-01-01', 'abc'), ('2000-01-01', 'def');
PREPARE hash_q(varchar) AS SELECT count(*) FROM hash_prep WHERE device = $1;
SET plan_cache_mode = force_custom_plan;
EXECUTE hash_q('abc');
EXECUTE hash_q('def');
SET plan_cache_mode = force_generic_plan;
EXECUTE hash_q('abc');
EXECUTE hash_q('def');
RESET plan_cache_mode;
DEALLOCATE hash_q;
DROP TABLE hash_prep;
