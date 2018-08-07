CREATE TABLE part_legacy(time timestamptz, temp float, device int);
SELECT create_hypertable('part_legacy', 'time', 'device', 2, partitioning_func => '_timescaledb_internal.get_partition_for_key');

-- Show legacy partitioning function is used
SELECT * FROM _timescaledb_catalog.dimension;

INSERT INTO part_legacy VALUES ('2017-03-22T09:18:23', 23.4, 1);
INSERT INTO part_legacy VALUES ('2017-03-22T09:18:23', 23.4, 76);

VACUUM part_legacy;

-- Show two chunks and CHECK constraint with cast
SELECT * FROM test.show_constraintsp('_timescaledb_internal._hyper_1_%_chunk');

-- Make sure constraint exclusion works on device column
EXPLAIN (verbose, costs off)
SELECT * FROM part_legacy WHERE device = 1;

CREATE TABLE part_new(time timestamptz, temp float, device int);
SELECT create_hypertable('part_new', 'time', 'device', 2);

SELECT * FROM _timescaledb_catalog.dimension;

INSERT INTO part_new VALUES ('2017-03-22T09:18:23', 23.4, 1);
INSERT INTO part_new VALUES ('2017-03-22T09:18:23', 23.4, 2);

VACUUM part_new;

-- Show two chunks and CHECK constraint without cast
SELECT * FROM test.show_constraintsp('_timescaledb_internal._hyper_2_%_chunk');

-- Make sure constraint exclusion works on device column
EXPLAIN (verbose, costs off)
SELECT * FROM part_new WHERE device = 1;

CREATE TABLE part_new_convert1(time timestamptz, temp float8, device int);
SELECT create_hypertable('part_new_convert1', 'time', 'temp', 2);

INSERT INTO part_new_convert1 VALUES ('2017-03-22T09:18:23', 1.0, 2);
\set ON_ERROR_STOP 0
-- Changing the type of a hash-partitioned column should not be supported
ALTER TABLE part_new_convert1 ALTER COLUMN temp TYPE numeric;
\set ON_ERROR_STOP 1

-- Should be able to change if not hash partitioned though
ALTER TABLE part_new_convert1 ALTER COLUMN time TYPE timestamp;

SELECT * FROM test.show_columnsp('_timescaledb_internal._hyper_3_%_chunk');

CREATE TABLE part_add_dim(time timestamptz, temp float8, device int, location int);
SELECT create_hypertable('part_add_dim', 'time', 'temp', 2);

\set ON_ERROR_STOP 0
SELECT add_dimension('part_add_dim', 'location', 2, partitioning_func => 'bad_func');
\set ON_ERROR_STOP 1

SELECT add_dimension('part_add_dim', 'location', 2, partitioning_func => '_timescaledb_internal.get_partition_for_key');
SELECT * FROM _timescaledb_catalog.dimension;

-- Test that we support custom SQL-based partitioning functions and
-- that our native partitioning function handles function expressions
-- as argument
CREATE OR REPLACE FUNCTION custom_partfunc(source anyelement)
    RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
DECLARE
    retval INTEGER;
BEGIN
    retval = _timescaledb_internal.get_partition_hash(substring(source::text FROM '[A-za-z0-9 ]+'));
    RAISE NOTICE 'hash value for % is %', source, retval;
    RETURN retval;
END
$BODY$;

CREATE TABLE part_custom_func(time timestamptz, temp float8, device text);
SELECT create_hypertable('part_custom_func', 'time', 'device', 2, partitioning_func => 'custom_partfunc');

SELECT _timescaledb_internal.get_partition_hash(substring('dev1' FROM '[A-za-z0-9 ]+'));
SELECT _timescaledb_internal.get_partition_hash('dev1'::text);
SELECT _timescaledb_internal.get_partition_hash('dev7'::text);

INSERT INTO part_custom_func VALUES ('2017-03-22T09:18:23', 23.4, 'dev1'),
                                    ('2017-03-22T09:18:23', 23.4, 'dev7');

SELECT * FROM test.show_subtables('part_custom_func');

-- This first test is slightly trivial, but segfaulted in old versions
CREATE TYPE simpl AS (val1 int4);

CREATE OR REPLACE FUNCTION simpl_type_hash(ANYELEMENT) RETURNS int4 AS $$
    SELECT $1.val1;
$$ LANGUAGE SQL IMMUTABLE;

CREATE TABLE simpl_partition ("timestamp" TIMESTAMPTZ, object simpl);

SELECT create_hypertable(
    'simpl_partition',
    'timestamp',
    'object',
    1000,
    chunk_time_interval => interval '1 day',
    partitioning_func=>'simpl_type_hash');

INSERT INTO simpl_partition VALUES ('2017-03-22T09:18:23', ROW(1)::simpl);

SELECT * from simpl_partition;

-- Also test that the fix works when we have more chunks than allowed at once
SET timescaledb.max_open_chunks_per_insert=1;

INSERT INTO simpl_partition VALUES
    ('2017-03-22T10:18:23', ROW(0)::simpl),
    ('2017-03-22T10:18:23', ROW(1)::simpl),
    ('2017-03-22T10:18:23', ROW(2)::simpl),
    ('2017-03-22T10:18:23', ROW(3)::simpl),
    ('2017-03-22T10:18:23', ROW(4)::simpl),
    ('2017-03-22T10:18:23', ROW(5)::simpl);

SET timescaledb.max_open_chunks_per_insert=default;

SELECT * from simpl_partition;

-- Test that index creation is handled correctly.
CREATE TABLE hyper_with_index(time timestamptz, temp float, device int);
CREATE UNIQUE INDEX temp_index ON hyper_with_index(temp);

\set ON_ERROR_STOP 0
SELECT create_hypertable('hyper_with_index', 'time');
SELECT create_hypertable('hyper_with_index', 'time', 'device', 2);
SELECT create_hypertable('hyper_with_index', 'time', 'temp', 2);
\set ON_ERROR_STOP 1

DROP INDEX temp_index;
CREATE UNIQUE INDEX time_index ON hyper_with_index(time);

\set ON_ERROR_STOP 0
-- should error because device not in index
SELECT create_hypertable('hyper_with_index', 'time', 'device', 4);
\set ON_ERROR_STOP 1
SELECT create_hypertable('hyper_with_index', 'time');
-- make sure user created index is used.
-- not using \d or \d+ because output syntax differs
-- between postgres 9 and postgres 10.
SELECT indexname FROM pg_indexes WHERE tablename = 'hyper_with_index';
\set ON_ERROR_STOP 0
SELECT add_dimension('hyper_with_index', 'device', 4);
\set ON_ERROR_STOP 1

DROP INDEX time_index;
CREATE UNIQUE INDEX time_space_index ON hyper_with_index(time, device);
SELECT add_dimension('hyper_with_index', 'device', 4);


CREATE TABLE hyper_with_primary(time TIMESTAMPTZ PRIMARY KEY, temp float, device int);
\set ON_ERROR_STOP 0
SELECT create_hypertable('hyper_with_primary', 'time', 'device', 4);
\set ON_ERROR_STOP 1

SELECT create_hypertable('hyper_with_primary', 'time');
\set ON_ERROR_STOP 0
SELECT add_dimension('hyper_with_primary', 'device', 4);
\set ON_ERROR_STOP 1

-- NON-unique indexes can still be created
CREATE INDEX temp_index ON hyper_with_index(temp);

-- Make sure custom composite types are supported as dimensions
CREATE TYPE TUPLE as (val1 int4, val2 int4);
CREATE FUNCTION tuple_hash(value ANYELEMENT) RETURNS INT4
LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'custom hash value is: %', value.val1+value.val2;
    RETURN value.val1+value.val2;
END
$BODY$;

CREATE TABLE part_custom_dim (time TIMESTAMPTZ, combo TUPLE, device TEXT);

\set ON_ERROR_STOP 0
-- should fail because no partitioning function supplied and the given custom type
-- has no default hash function
SELECT create_hypertable('part_custom_dim', 'time', 'combo', 4);
\set ON_ERROR_STOP 1
SELECT create_hypertable('part_custom_dim', 'time', 'combo', 4, partitioning_func=>'tuple_hash');

INSERT INTO part_custom_dim(time, combo) VALUES (now(), (1,2));
