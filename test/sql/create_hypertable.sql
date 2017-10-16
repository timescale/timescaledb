\c single :ROLE_SUPERUSER
create schema test_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER;

\c single :ROLE_DEFAULT_PERM_USER
create table test_schema.test_table(time BIGINT, temp float8, device_id text, device_type text, location text, id int, id2 int);


\set ON_ERROR_STOP 0
-- get_create_command should fail since hypertable isn't made yet
SELECT * FROM _timescaledb_internal.get_create_command('test_table');
\set ON_ERROR_STOP 1

\dt "test_schema".*

create table test_schema.test_table_no_not_null(time BIGINT, device_id text);
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
insert into test_schema.test_table_no_not_null (device_id) VALUES('foo');
\set ON_ERROR_STOP 1
insert into test_schema.test_table_no_not_null (time, device_id) VALUES(1, 'foo');

\d test_schema.test_table_no_not_null
ALTER TABLE test_schema.test_table_no_not_null ALTER time DROP NOT NULL;
\d test_schema.test_table_no_not_null
SELECT _timescaledb_internal.set_time_columns_not_null();
\d test_schema.test_table_no_not_null

select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
SELECT * FROM _timescaledb_internal.get_create_command('test_table');

--test adding one more closed dimension
select add_dimension('test_schema.test_table', 'location', 2);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;
\set ON_ERROR_STOP 0
-- get_create_command only works on tables w/ 1 or 2 dimensions
SELECT * FROM _timescaledb_internal.get_create_command('test_table');
\set ON_ERROR_STOP 1

--test adding one more open dimension
select add_dimension('test_schema.test_table', 'id', interval_length => 1000);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;

\set ON_ERROR_STOP 0
--adding the same dimension twice should fail
select add_dimension('test_schema.test_table', 'location', 2);

--adding dimension with both number_partitions and interval_length should fail
select add_dimension('test_schema.test_table', 'id2', number_partitions => 2, interval_length => 1000);

--adding a new dimension on a non-empty table should also fail
insert into test_schema.test_table values (123456789, 23.8, 'blue', 'type1', 'nyc', 1, 1);
select add_dimension('test_schema.test_table', 'device_type', 2);
\set ON_ERROR_STOP 1

--test partitioning in only time dimension
create table test_schema.test_1dim(time timestamp, temp float);
select create_hypertable('test_schema.test_1dim', 'time');
SELECT * FROM _timescaledb_internal.get_create_command('test_1dim');

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
