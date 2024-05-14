-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM ONLY PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM ONLY PUBLIC."Hypertable_1";

SELECT * FROM test.show_columns('PUBLIC."Hypertable_1"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');

\ir include/ddl_ops_2.sql

SELECT * FROM test.show_columns('PUBLIC."Hypertable_1"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');

SELECT * FROM PUBLIC."Hypertable_1";

-- alter column tests
CREATE TABLE alter_test(time timestamptz, temp float, color varchar(10));

-- create hypertable with two chunks
SELECT create_hypertable('alter_test', 'time', 'color', 2, chunk_time_interval => 2628000000000);

INSERT INTO alter_test VALUES ('2017-01-20T09:00:01', 17.5, 'blue'),
                              ('2017-01-21T09:00:01', 19.1, 'yellow'),
                              ('2017-04-20T09:00:01', 89.5, 'green'),
                              ('2017-04-21T09:00:01', 17.1, 'black');
SELECT * FROM test.show_columns('alter_test');
SELECT * FROM test.show_columnsp('_timescaledb_internal._hyper_9_%chunk');

-- show the column name and type of the partitioning dimension in the
-- metadata table
SELECT * FROM _timescaledb_catalog.dimension WHERE hypertable_id = 9;

EXPLAIN (costs off)
SELECT * FROM alter_test WHERE time > '2017-05-20T10:00:01';

-- rename column and change its type
ALTER TABLE alter_test RENAME COLUMN time TO time_us;
--converting timestamptz->timestamp should happen under UTC
SET timezone = 'UTC';
ALTER TABLE alter_test ALTER COLUMN time_us TYPE timestamp;
RESET timezone;
ALTER TABLE alter_test RENAME COLUMN color TO colorname;
\set ON_ERROR_STOP 0
-- Changing types on hash-partitioned columns is not safe for some
-- types and is therefore blocked.
ALTER TABLE alter_test ALTER COLUMN colorname TYPE text;
\set ON_ERROR_STOP 1

SELECT * FROM test.show_columns('alter_test');
SELECT * FROM test.show_columnsp('_timescaledb_internal._hyper_9_%chunk');

-- show that the metadata has been updated
SELECT * FROM _timescaledb_catalog.dimension WHERE hypertable_id = 9;

-- constraint exclusion should still work with updated column
EXPLAIN (costs off)
SELECT * FROM alter_test WHERE time_us > '2017-05-20T10:00:01';

\set ON_ERROR_STOP 0
-- verify that we cannot change the column type to something incompatible
ALTER TABLE alter_test ALTER COLUMN colorname TYPE varchar(3);
-- conversion that messes up partitioning fails
ALTER TABLE alter_test ALTER COLUMN time_us TYPE timestamptz USING time_us::timestamptz+INTERVAL '1 year';
-- dropping column that messes up partiitoning fails
ALTER TABLE alter_test DROP COLUMN colorname;
--ONLY blocked
ALTER TABLE ONLY alter_test RENAME COLUMN colorname TO colorname2;
ALTER TABLE ONLY alter_test ALTER COLUMN colorname TYPE varchar(10);
\set ON_ERROR_STOP 1

CREATE TABLE alter_test_bigint(time bigint, temp float);
SELECT create_hypertable('alter_test_bigint', 'time', chunk_time_interval => 2628000000000);

\set ON_ERROR_STOP 0
-- Changing type of time dimension to a non-supported type
-- shall not be allowed
ALTER TABLE alter_test_bigint
ALTER COLUMN time TYPE TEXT;
-- dropping open time dimension shall not be allowed.
ALTER TABLE alter_test_bigint
DROP COLUMN time;
\set ON_ERROR_STOP 1


-- test expression index creation where physical layout of chunks differs from hypertable
CREATE TABLE i2504(time timestamp NOT NULL, a int, b int, c int, d int);

select create_hypertable('i2504', 'time');

INSERT INTO i2504 VALUES (now(), 1, 2, 3, 4);
ALTER TABLE i2504 DROP COLUMN b;

INSERT INTO i2504(time, a, c, d) VALUES
(now() - interval '1 year', 1, 2, 3),
(now() - interval '2 years', 1, 2, 3);

CREATE INDEX idx2 ON i2504(a,d) WHERE c IS NOT NULL;
DROP INDEX idx2;
CREATE INDEX idx2 ON i2504(a,d) WITH (timescaledb.transaction_per_chunk) WHERE c IS NOT NULL;

-- Make sure custom composite types are supported as dimensions
CREATE TYPE TUPLE as (val1 int4, val2 int4);
CREATE TABLE part_custom_dim (time TIMESTAMPTZ, combo TUPLE, device TEXT);
\set ON_ERROR_STOP 0
-- should fail on PG < 14 because no partitioning function supplied and the given custom type
-- has no default hash function
-- on PG14 custom types are hashable
SELECT create_hypertable('part_custom_dim', 'time', 'combo', 4);
\set ON_ERROR_STOP 1

-- immutable functions with sub-transaction (issue #4489)
CREATE FUNCTION i4489(value TEXT DEFAULT '') RETURNS INTEGER
AS
$$
BEGIN
  RETURN value::INTEGER;
EXCEPTION WHEN invalid_text_representation THEN
  RETURN 0;
END;
$$
LANGUAGE PLPGSQL IMMUTABLE;

-- should return 1 (one) in both cases
SELECT i4489('1'), i4489('1');
-- should return 0 (zero) in all cases handled by the exception
SELECT i4489(), i4489();
SELECT i4489('a'), i4489('a');
