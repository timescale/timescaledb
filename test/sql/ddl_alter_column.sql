-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE alter_test(time timestamptz, temp float, color varchar(10));

-- create hypertable with two chunks
SELECT create_hypertable('alter_test', 'time', 'color', 2, chunk_time_interval => 2628000000000);

INSERT INTO alter_test VALUES ('2017-01-20T09:00:01', 17.5, 'blue'),
                              ('2017-01-21T09:00:01', 19.1, 'yellow'),
                              ('2017-04-20T09:00:01', 89.5, 'green'),
                              ('2017-04-21T09:00:01', 17.1, 'black');
SELECT * FROM test.show_columns('alter_test');
SELECT * FROM test.show_columnsp('_timescaledb_internal.%chunk');

-- show the column name and type of the partitioning dimension in the
-- metadata table
SELECT * FROM _timescaledb_catalog.dimension;

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
SELECT * FROM test.show_columnsp('_timescaledb_internal.%chunk');

-- show that the metadata has been updated
SELECT * FROM _timescaledb_catalog.dimension;

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
