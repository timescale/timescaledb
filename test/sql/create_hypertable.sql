\ir include/create_single_db.sql

create schema test_schema;
create table test_schema.test_table(time BIGINT, temp float8, device_id text, device_type text, location text, id int);

\dt "test_schema".*
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

--test adding one more closed dimension
select add_dimension('test_schema.test_table', 'location', 2);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;

--test adding one more open dimension
select add_dimension('test_schema.test_table', 'id', interval_length => 1000);
select * from _timescaledb_catalog.hypertable where table_name = 'test_table';
select * from _timescaledb_catalog.dimension;

\set ON_ERROR_STOP 0
--adding the same dimension twice should afail
select add_dimension('test_schema.test_table', 'location', 2);

--adding a new dimension on a non-empty table should also fail
insert into test_schema.test_table values (123456789, 23.8, 'blue', 'type1', 'nyc', 1);
select add_dimension('test_schema.test_table', 'device_type', 2);
\set ON_ERROR_STOP 1

--test partitioning in only time dimension
create table test_schema.test_1dim(time timestamp, temp float);
select create_hypertable('test_schema.test_1dim', 'time');

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
