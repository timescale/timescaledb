-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
create schema test_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
create schema chunk_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER_2;

SET ROLE :ROLE_DEFAULT_PERM_USER;
create table test_schema.test_table(time BIGINT, temp float8, device_id text, device_type text, location text, id int, id2 int);

\set ON_ERROR_STOP 0
-- get_create_command should fail since hypertable isn't made yet
SELECT * FROM _timescaledb_functions.get_create_command('test_table');
\set ON_ERROR_STOP 1

\dt "test_schema".*

create table test_schema.test_table_no_not_null(time BIGINT, device_id text);

\set ON_ERROR_STOP 0
-- Permission denied with unprivileged role
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'));

-- CREATE on schema is not enough
SET ROLE :ROLE_DEFAULT_PERM_USER;
GRANT ALL ON SCHEMA test_schema TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

-- Should work with when granted table owner role
RESET ROLE;
GRANT :ROLE_DEFAULT_PERM_USER TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
insert into test_schema.test_table_no_not_null (device_id) VALUES('foo');
\set ON_ERROR_STOP 1
insert into test_schema.test_table_no_not_null (time, device_id) VALUES(1, 'foo');

RESET ROLE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- No permissions on associated schema should fail
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'), associated_schema_name => 'chunk_schema');
\set ON_ERROR_STOP 1

-- Granting permissions on chunk_schema should make things work
RESET ROLE;
GRANT CREATE ON SCHEMA chunk_schema TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'), associated_schema_name => 'chunk_schema');

-- Check that the insert block trigger exists
SELECT * FROM test.show_triggers('test_schema.test_table');

SELECT * FROM _timescaledb_functions.get_create_command('test_table');

--test adding one more closed dimension
select add_dimension('test_schema.test_table', 'location', 4);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;

--test that we can change the number of partitions and that 1 is allowed
SELECT set_number_partitions('test_schema.test_table', 1, 'location');
select * from _timescaledb_catalog.dimension WHERE column_name = 'location';
SELECT set_number_partitions('test_schema.test_table', 2, 'location');
select * from _timescaledb_catalog.dimension WHERE column_name = 'location';

\set ON_ERROR_STOP 0
--must give an explicit dimension when there are multiple space dimensions
SELECT set_number_partitions('test_schema.test_table', 3);
--too few
SELECT set_number_partitions('test_schema.test_table', 0, 'location');
-- Too many
SELECT set_number_partitions('test_schema.test_table', 32768, 'location');
-- get_create_command only works on tables w/ 1 or 2 dimensions
SELECT * FROM _timescaledb_functions.get_create_command('test_table');
\set ON_ERROR_STOP 1

--test adding one more open dimension
select add_dimension('test_schema.test_table', 'id', chunk_time_interval => 1000);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;

-- Test add_dimension: can use interval types for TIMESTAMPTZ columns
CREATE TABLE dim_test_time(time TIMESTAMPTZ, time2 TIMESTAMPTZ, time3 BIGINT, temp float8, device int, location int);
SELECT create_hypertable('dim_test_time', 'time');
SELECT add_dimension('dim_test_time', 'time2', chunk_time_interval => INTERVAL '1 day');

-- Test add_dimension: only integral should work on BIGINT columns
\set ON_ERROR_STOP 0
SELECT add_dimension('dim_test_time', 'time3', chunk_time_interval => INTERVAL '1 day');

-- string is not a valid type
SELECT add_dimension('dim_test_time', 'time3', chunk_time_interval => 'foo'::TEXT);
\set ON_ERROR_STOP 1
SELECT add_dimension('dim_test_time', 'time3', chunk_time_interval => 500);

-- Test add_dimension: integrals should work on TIMESTAMPTZ columns
CREATE TABLE dim_test_time2(time TIMESTAMPTZ, time2 TIMESTAMPTZ, temp float8, device int, location int);
SELECT create_hypertable('dim_test_time2', 'time');
SELECT add_dimension('dim_test_time2', 'time2', chunk_time_interval => 500);

--adding a dimension twice should not fail with 'if_not_exists'
SELECT add_dimension('dim_test_time2', 'time2', chunk_time_interval => 500, if_not_exists => true);

\set ON_ERROR_STOP 0
--adding on a non-hypertable
CREATE TABLE not_hypertable(time TIMESTAMPTZ, temp float8, device int, location int);
SELECT add_dimension('not_hypertable', 'time', chunk_time_interval => 500);

--adding a non-exist column
SELECT add_dimension('test_schema.test_table', 'nope', 2);

--adding the same dimension twice should fail
select add_dimension('test_schema.test_table', 'location', 2);

--adding dimension with both number_partitions and chunk_time_interval should fail
select add_dimension('test_schema.test_table', 'id2', number_partitions => 2, chunk_time_interval => 1000);

\set ON_ERROR_STOP 1

-- test adding a new dimension on a non-empty table
CREATE TABLE dim_test(time TIMESTAMPTZ, device int);
SELECT create_hypertable('dim_test', 'time', chunk_time_interval => INTERVAL '1 day');

CREATE VIEW dim_test_slices AS
SELECT c.id AS chunk_id, c.hypertable_id, ds.dimension_id, cc.dimension_slice_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN _timescaledb_catalog.dimension td ON (h.id = td.hypertable_id)
INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = td.id)
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.table_name = 'dim_test'
ORDER BY c.id, ds.dimension_id;

INSERT INTO dim_test VALUES ('2004-10-10 00:00:00+00', 1);
INSERT INTO dim_test VALUES ('2004-10-20 00:00:00+00', 2);

SELECT * FROM dim_test_slices;
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_5_2_chunk');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_5_3_chunk');

-- add dimension to the existing chunks by adding -inf/inf dimension slices
SELECT add_dimension('dim_test', 'device', 2);

SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_5_2_chunk');
SELECT * FROM test.show_constraints('_timescaledb_internal._hyper_5_3_chunk');
SELECT * FROM dim_test_slices;

-- newer chunks have proper dimension slices range
INSERT INTO dim_test VALUES ('2004-10-30 00:00:00+00', 3);
SELECT * FROM dim_test_slices;

SELECT * FROM dim_test ORDER BY time;

DROP VIEW dim_test_slices;
DROP TABLE dim_test;

-- test add_dimension() with existing data on table with space partitioning
CREATE TABLE dim_test(time TIMESTAMPTZ, device int, data int);
SELECT create_hypertable('dim_test', 'time', 'device', 2, chunk_time_interval => INTERVAL '1 day');

CREATE VIEW dim_test_slices AS
SELECT c.id AS chunk_id, c.hypertable_id, ds.dimension_id, cc.dimension_slice_id, c.schema_name AS chunk_schema, c.table_name AS chunk_table, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c
INNER JOIN _timescaledb_catalog.hypertable h ON (c.hypertable_id = h.id)
INNER JOIN _timescaledb_catalog.dimension td ON (h.id = td.hypertable_id)
INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = td.id)
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
WHERE h.table_name = 'dim_test'
ORDER BY c.id, ds.dimension_id;

INSERT INTO dim_test VALUES ('2004-10-10 00:00:00+00', 1, 3);
INSERT INTO dim_test VALUES ('2004-10-20 00:00:00+00', 2, 2);
SELECT * FROM dim_test_slices;

-- new dimension slice will cover full range on existing chunks
SELECT add_dimension('dim_test', 'data', 1);
SELECT * FROM dim_test_slices;

INSERT INTO dim_test VALUES ('2004-10-30 00:00:00+00', 3, 1);
SELECT * FROM dim_test_slices;
SELECT * FROM dim_test ORDER BY time;

DROP VIEW dim_test_slices;
DROP TABLE dim_test;

-- should not fail on non-empty table with 'if_not_exists' in case the dimension exists
select add_dimension('test_schema.test_table', 'location', 2, if_not_exists => true);

--test partitioning in only time dimension
create table test_schema.test_1dim(time timestamp, temp float);
select create_hypertable('test_schema.test_1dim', 'time');
SELECT * FROM _timescaledb_functions.get_create_command('test_1dim');

\dt "test_schema".*

select create_hypertable('test_schema.test_1dim', 'time', if_not_exists => true);

-- Should error when creating again without if_not_exists set to true
\set ON_ERROR_STOP 0
select create_hypertable('test_schema.test_1dim', 'time');
\set ON_ERROR_STOP 1

-- if_not_exist should also work with data in the hypertable
insert into test_schema.test_1dim VALUES ('2004-10-19 10:23:54+02', 1.0);
select create_hypertable('test_schema.test_1dim', 'time', if_not_exists => true);

-- Should error when creating again without if_not_exists set to true
\set ON_ERROR_STOP 0
select create_hypertable('test_schema.test_1dim', 'time');
\set ON_ERROR_STOP 1

-- Test partitioning functions
CREATE OR REPLACE FUNCTION invalid_partfunc(source integer)
    RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN NULL;
END
$BODY$;

CREATE OR REPLACE FUNCTION time_partfunc(source text)
    RETURNS TIMESTAMPTZ LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN timezone('UTC', to_timestamp(source));
END
$BODY$;

CREATE TABLE test_schema.test_invalid_func(time timestamptz, temp float8, device text);

\set ON_ERROR_STOP 0
-- should fail due to invalid signature
SELECT create_hypertable('test_schema.test_invalid_func', 'time', 'device', 2, partitioning_func => 'invalid_partfunc');

SELECT create_hypertable('test_schema.test_invalid_func', 'time');
-- should also fail due to invalid signature
SELECT add_dimension('test_schema.test_invalid_func', 'device', 2, partitioning_func => 'invalid_partfunc');
\set ON_ERROR_STOP 1


-- Test open-dimension function
CREATE TABLE test_schema.open_dim_part_func(time text, temp float8, device text, event_time text);

\set ON_ERROR_STOP 0
-- should fail due to invalid signature
SELECT create_hypertable('test_schema.open_dim_part_func', 'time', time_partitioning_func => 'invalid_partfunc');
\set ON_ERROR_STOP 1

SELECT create_hypertable('test_schema.open_dim_part_func', 'time', time_partitioning_func => 'time_partfunc');

\set ON_ERROR_STOP 0
-- should fail due to invalid signature
SELECT add_dimension('test_schema.open_dim_part_func', 'event_time', chunk_time_interval => interval '1 day', partitioning_func => 'invalid_partfunc');
\set ON_ERROR_STOP 1

SELECT add_dimension('test_schema.open_dim_part_func', 'event_time', chunk_time_interval => interval '1 day', partitioning_func => 'time_partfunc');

--test data migration
create table test_schema.test_migrate(time timestamp, temp float);
insert into test_schema.test_migrate VALUES ('2004-10-19 10:23:54+02', 1.0), ('2004-12-19 10:23:54+02', 2.0);
select * from only test_schema.test_migrate;
\set ON_ERROR_STOP 0
--should fail without migrate_data => true
select create_hypertable('test_schema.test_migrate', 'time');
\set ON_ERROR_STOP 1
select create_hypertable('test_schema.test_migrate', 'time', migrate_data => true);

--there should be two new chunks
select * from _timescaledb_catalog.hypertable where table_name = 'test_migrate';
select * from _timescaledb_catalog.chunk;
select * from test_schema.test_migrate;
--main table should now be empty
select * from only test_schema.test_migrate;
select * from only _timescaledb_internal._hyper_10_9_chunk;
select * from only _timescaledb_internal._hyper_10_10_chunk;

create table test_schema.test_migrate_empty(time timestamp, temp float);
select create_hypertable('test_schema.test_migrate_empty', 'time', migrate_data => true);

CREATE TYPE test_type AS (time timestamp, temp float);
CREATE TABLE test_table_of_type OF test_type;
SELECT create_hypertable('test_table_of_type', 'time');
INSERT INTO test_table_of_type VALUES ('2004-10-19 10:23:54+02', 1.0), ('2004-12-19 10:23:54+02', 2.0);

\set ON_ERROR_STOP 0
DROP TYPE test_type;
\set ON_ERROR_STOP 1
DROP TYPE test_type CASCADE;

CREATE TABLE test_table_of_type (time timestamp, temp float);
SELECT create_hypertable('test_table_of_type', 'time');
INSERT INTO test_table_of_type VALUES ('2004-10-19 10:23:54+02', 1.0), ('2004-12-19 10:23:54+02', 2.0);
CREATE TYPE test_type AS (time timestamp, temp float);
ALTER TABLE test_table_of_type OF test_type;

\set ON_ERROR_STOP 0
DROP TYPE test_type;
\set ON_ERROR_STOP 1
BEGIN;
DROP TYPE test_type CASCADE;
ROLLBACK;

ALTER TABLE test_table_of_type NOT OF;
DROP TYPE test_type;


-- Reset GRANTS
\c :TEST_DBNAME :ROLE_SUPERUSER
REVOKE :ROLE_DEFAULT_PERM_USER FROM :ROLE_DEFAULT_PERM_USER_2;

-- Test custom partitioning functions
CREATE OR REPLACE FUNCTION partfunc_not_immutable(source anyelement)
    RETURNS INTEGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.get_partition_hash(source);
END
$BODY$;


CREATE OR REPLACE FUNCTION partfunc_bad_return_type(source anyelement)
    RETURNS BIGINT LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.get_partition_hash(source);
END
$BODY$;


CREATE OR REPLACE FUNCTION partfunc_bad_arg_type(source text)
    RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.get_partition_hash(source);
END
$BODY$;


CREATE OR REPLACE FUNCTION partfunc_bad_multi_arg(source anyelement, extra_arg integer)
    RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.get_partition_hash(source);
END
$BODY$;

CREATE OR REPLACE FUNCTION partfunc_valid(source anyelement)
    RETURNS INTEGER LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RETURN _timescaledb_internal.get_partition_hash(source);
END
$BODY$;

create table test_schema.test_partfunc(time timestamptz, temp float, device int);

-- Test that create_hypertable fails due to invalid partitioning function
\set ON_ERROR_STOP 0
select create_hypertable('test_schema.test_partfunc', 'time', 'device', 2, partitioning_func => 'partfunc_not_immutable');
select create_hypertable('test_schema.test_partfunc', 'time', 'device', 2, partitioning_func => 'partfunc_bad_return_type');
select create_hypertable('test_schema.test_partfunc', 'time', 'device', 2, partitioning_func => 'partfunc_bad_arg_type');
select create_hypertable('test_schema.test_partfunc', 'time', 'device', 2, partitioning_func => 'partfunc_bad_multi_arg');
\set ON_ERROR_STOP 1

-- Test that add_dimension fails due to invalid partitioning function
select create_hypertable('test_schema.test_partfunc', 'time');

\set ON_ERROR_STOP 0
select add_dimension('test_schema.test_partfunc', 'device', 2, partitioning_func => 'partfunc_not_immutable');
select add_dimension('test_schema.test_partfunc', 'device', 2, partitioning_func => 'partfunc_bad_return_type');
select add_dimension('test_schema.test_partfunc', 'device', 2, partitioning_func => 'partfunc_bad_arg_type');
select add_dimension('test_schema.test_partfunc', 'device', 2, partitioning_func => 'partfunc_bad_multi_arg');
\set ON_ERROR_STOP 1

-- A valid function should work:
select add_dimension('test_schema.test_partfunc', 'device', 2, partitioning_func => 'partfunc_valid');

-- check get_create_command produces valid command
CREATE TABLE test_schema.test_sql_cmd(time TIMESTAMPTZ, temp FLOAT8, device_id TEXT, device_type TEXT, location TEXT, id INT, id2 INT);
SELECT create_hypertable('test_schema.test_sql_cmd','time');
SELECT * FROM _timescaledb_functions.get_create_command('test_sql_cmd');
SELECT _timescaledb_functions.get_create_command('test_sql_cmd') AS create_cmd; \gset
DROP TABLE test_schema.test_sql_cmd CASCADE;
CREATE TABLE test_schema.test_sql_cmd(time TIMESTAMPTZ, temp FLOAT8, device_id TEXT, device_type TEXT, location TEXT, id INT, id2 INT);
SELECT test.execute_sql(:'create_cmd');


\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE test_table_int(time bigint, junk int);
SELECT hypertable_id AS "TEST_TABLE_INT_HYPERTABLE_ID" FROM create_hypertable('test_table_int', 'time', chunk_time_interval => 1) \gset

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA IF NOT EXISTS my_schema;
create or replace function my_schema.dummy_now2() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
grant execute on ALL FUNCTIONS IN SCHEMA my_schema to public;
create or replace function dummy_now3() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
grant execute on ALL FUNCTIONS IN SCHEMA my_schema to public;
REVOKE execute ON function dummy_now3() FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS my_user_schema;
GRANT ALL ON SCHEMA my_user_schema to PUBLIC;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER


create or replace function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
create or replace function my_user_schema.dummy_now4() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';


select set_integer_now_func('test_table_int', 'dummy_now');
select * from _timescaledb_catalog.dimension WHERE hypertable_id = :TEST_TABLE_INT_HYPERTABLE_ID;
\set ON_ERROR_STOP 0
select set_integer_now_func('test_table_int', 'dummy_now');
select set_integer_now_func('test_table_int', 'my_schema.dummy_now2', replace_if_exists => TRUE);
select set_integer_now_func('test_table_int', 'dummy_now3', replace_if_exists => TRUE);
\set ON_ERROR_STOP

select set_integer_now_func('test_table_int', 'my_user_schema.dummy_now4', replace_if_exists => TRUE);
\c :TEST_DBNAME :ROLE_SUPERUSER
ALTER SCHEMA my_user_schema RENAME TO my_new_schema;
select * from _timescaledb_catalog.dimension WHERE hypertable_id = :TEST_TABLE_INT_HYPERTABLE_ID;

-- github issue #4650
CREATE TABLE sample_table (
       cpu double precision null,
       time TIMESTAMP WITH TIME ZONE NOT NULL,
       sensor_id INTEGER NOT NULL,
       name varchar(100) default 'this is a default string value',
       UNIQUE(sensor_id, time)
);

ALTER TABLE sample_table DROP COLUMN name;

-- below creation should not report any warnings.
SELECT * FROM create_hypertable('sample_table', 'time');

-- cleanup
DROP TABLE sample_table CASCADE;

-- github issue 4684
-- test PARTITION BY HASH
CREATE TABLE regular(
   id INT NOT NULL,
   dev INT NOT NULL,
   value INT,
   CONSTRAINT cstr_regular_pky PRIMARY KEY (id)
) PARTITION BY HASH (id);

DO $$
BEGIN
   FOR i IN 1..2
   LOOP
      EXECUTE format('
         CREATE TABLE %I
         PARTITION OF regular
         FOR VALUES WITH (MODULUS 2, REMAINDER %s)',
         'regular_' || i, i - 1
      );
   END LOOP;
END;
$$;

INSERT INTO regular SELECT generate_series(1,1000), 44,55;

CREATE TABLE timescale (
   ts TIMESTAMP WITH TIME ZONE NOT NULL,
   id INT NOT NULL,
   dev INT NOT NULL,
   FOREIGN KEY (id)  REFERENCES regular(id) ON DELETE CASCADE
);

SELECT create_hypertable(
   relation => 'timescale',
   time_column_name => 'ts'
);

-- creates chunk1
INSERT INTO timescale SELECT now(), generate_series(1,200), 43;
-- creates chunk2
INSERT INTO timescale SELECT now() + interval '20' day, generate_series(1,200), 43;
-- creates chunk3
INSERT INTO timescale SELECT now() + interval '40' day, generate_series(1,200), 43;

-- show chunks
SELECT SHOW_CHUNKS('timescale');

\set ON_ERROR_STOP 0
-- record goes into chunk1 violating FK constraint as value 1001 is not present in regular table
INSERT INTO timescale SELECT now(), 1001, 43;
-- record goes into chunk2 violating FK constraint as value 1002 is not present in regular table
INSERT INTO timescale SELECT now() + interval '20' day, 1002, 43;
-- record goes into chunk3 violating FK constraint as value 1003 is not present in regular table
INSERT INTO timescale SELECT now() + interval '40' day, 1003, 43;
\set ON_ERROR_STOP 1

-- cleanup
DROP TABLE regular cascade;
DROP TABLE timescale cascade;

-- test PARTITION BY RANGE
CREATE TABLE regular(
   id INT NOT NULL,
   dev INT NOT NULL,
   value INT,
   CONSTRAINT cstr_regular_pky PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE TABLE regular_1_500 PARTITION OF regular
    FOR VALUES FROM (1) TO (500);

CREATE TABLE regular_500_1000 PARTITION OF regular
    FOR VALUES FROM (500) TO (801);

INSERT INTO regular SELECT generate_series(1,800), 44,55;

CREATE TABLE timescale (
   ts TIMESTAMP WITH TIME ZONE NOT NULL,
   id INT NOT NULL,
   dev INT NOT NULL,
   FOREIGN KEY (id)  REFERENCES regular(id) ON DELETE CASCADE
);

SELECT create_hypertable(
   relation => 'timescale',
   time_column_name => 'ts'
);

-- creates chunk1
INSERT INTO timescale SELECT now(), generate_series(1,200), 43;
-- creates chunk2
INSERT INTO timescale SELECT now() + interval '20' day, generate_series(200,400), 43;
-- creates chunk3
INSERT INTO timescale SELECT now() + interval '40' day, generate_series(400,600), 43;

-- show chunks
SELECT SHOW_CHUNKS('timescale');

\set ON_ERROR_STOP 0
-- FK constraint violation as value 801 is not present in regular table
INSERT INTO timescale SELECT now(), 801, 43;
-- FK constraint violation as value 902 is not present in regular table
INSERT INTO timescale SELECT now() + interval '20' day, 902, 43;
-- FK constraint violation as value 1003 is not present in regular table
INSERT INTO timescale SELECT now() + interval '40' day, 1003, 43;
\set ON_ERROR_STOP 1

-- cleanup
DROP TABLE regular cascade;
DROP TABLE timescale cascade;

-- test PARTITION BY LIST
CREATE TABLE regular(
   id INT NOT NULL,
   dev INT NOT NULL,
   value INT,
   CONSTRAINT cstr_regular_pky PRIMARY KEY (id)
) PARTITION BY LIST (id);

CREATE TABLE regular_1_2_3_4 PARTITION OF regular FOR VALUES IN (1,2,3,4);
CREATE TABLE regular_5_6_7_8 PARTITION OF regular FOR VALUES IN (5,6,7,8);

INSERT INTO regular SELECT generate_series(1,8), 44,55;

CREATE TABLE timescale (
   ts TIMESTAMP WITH TIME ZONE NOT NULL,
   id INT NOT NULL,
   dev INT NOT NULL,
   FOREIGN KEY (id)  REFERENCES regular(id) ON DELETE CASCADE
);

SELECT create_hypertable(
   relation => 'timescale',
   time_column_name => 'ts'
);

insert into timescale values (now(), 1,2);
insert into timescale values (now(), 2,2);
insert into timescale values (now(), 3,2);
insert into timescale values (now(), 4,2);
insert into timescale values (now(), 5,2);
insert into timescale values (now(), 6,2);
insert into timescale values (now(), 7,2);
insert into timescale values (now(), 8,2);

\set ON_ERROR_STOP 0
-- FK constraint violation as value 9 is not present in regular table
insert into timescale values (now(), 9,2);
-- FK constraint violation as value 10 is not present in regular table
insert into timescale values (now(), 10,2);
-- FK constraint violation as value 111 is not present in regular table
insert into timescale values (now(), 111,2);
\set ON_ERROR_STOP 1

-- cleanup
DROP TABLE regular cascade;
DROP TABLE timescale cascade;

-- github issue 4872
-- If subplan of ChunkAppend is TidRangeScan, then SELECT on
-- hypertable fails with error "invalid child of chunk append: Node (26)"
create table tidrangescan_test (
  time timestamp with time zone,
  some_column bigint
);

select create_hypertable('tidrangescan_test', 'time');

insert into tidrangescan_test (time, some_column) values ('2023-02-12 00:00:00+02:40', 1);
insert into tidrangescan_test (time, some_column) values ('2023-02-12 00:00:10+02:40', 2);
insert into tidrangescan_test (time, some_column) values ('2023-02-12 00:00:20+02:40', 3);

-- Below query will generate plan as
-- Custom Scan (ChunkAppend)
--   ->  Tid Range Scan
-- However when traversing ChunkAppend node, Tid Range Scan node is not
-- recognised as a valid child node of ChunkAppend which causes error
-- "invalid child of chunk append: Node (26)" when below query is executed
select * from tidrangescan_test where time > '2023-02-12 00:00:00+02:40'::timestamp with time zone - interval '5 years' and ctid < '(1,1)'::tid ORDER BY time;

drop table tidrangescan_test;
