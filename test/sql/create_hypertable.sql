\c single :ROLE_SUPERUSER
create schema test_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
create schema chunk_schema AUTHORIZATION :ROLE_DEFAULT_PERM_USER_2;

SET ROLE :ROLE_DEFAULT_PERM_USER;
create table test_schema.test_table(time BIGINT, temp float8, device_id text, device_type text, location text, id int, id2 int);


\set ON_ERROR_STOP 0
-- get_create_command should fail since hypertable isn't made yet
SELECT * FROM _timescaledb_internal.get_create_command('test_table');
\set ON_ERROR_STOP 1

\dt "test_schema".*

create table test_schema.test_table_no_not_null(time BIGINT, device_id text);

\set ON_ERROR_STOP 0
-- Permission denied with unprivileged role
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

-- CREATE on schema is not enough
SET ROLE :ROLE_DEFAULT_PERM_USER;
GRANT ALL ON SCHEMA test_schema TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));
\set ON_ERROR_STOP 1

-- Should work with when granted table owner role
RESET ROLE;
GRANT :ROLE_DEFAULT_PERM_USER TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
select * from create_hypertable('test_schema.test_table_no_not_null', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));

\set ON_ERROR_STOP 0
insert into test_schema.test_table_no_not_null (device_id) VALUES('foo');
\set ON_ERROR_STOP 1
insert into test_schema.test_table_no_not_null (time, device_id) VALUES(1, 'foo');

RESET ROLE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- No permissions on associated schema should fail
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'), associated_schema_name => 'chunk_schema');
\set ON_ERROR_STOP 1

-- Granting permissions on chunk_schema should make things work
RESET ROLE;
GRANT CREATE ON SCHEMA chunk_schema TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'), associated_schema_name => 'chunk_schema');

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

-- Test add_dimension: can use interval types for TIMESTAMPTZ columns
CREATE TABLE dim_test_time(time TIMESTAMPTZ, time2 TIMESTAMPTZ, time3 BIGINT, temp float8, device int, location int);
SELECT create_hypertable('dim_test_time', 'time');
SELECT add_dimension('dim_test_time', 'time2', interval_length => INTERVAL '1 day');

-- Test add_dimension: only integral should work on BIGINT columns
\set ON_ERROR_STOP 0
SELECT add_dimension('dim_test_time', 'time3', interval_length => INTERVAL '1 day');

-- string is not a valid type
SELECT add_dimension('dim_test_time', 'time3', interval_length => 'foo'::TEXT);
\set ON_ERROR_STOP 1
SELECT add_dimension('dim_test_time', 'time3', interval_length => 500);

-- Test add_dimension: integrals should work on TIMESTAMPTZ columns
CREATE TABLE dim_test_time2(time TIMESTAMPTZ, time2 TIMESTAMPTZ, temp float8, device int, location int);
SELECT create_hypertable('dim_test_time2', 'time');
SELECT add_dimension('dim_test_time2', 'time2', interval_length => 500);


\set ON_ERROR_STOP 0
--adding on a non-hypertable
CREATE TABLE not_hypertable(time TIMESTAMPTZ, temp float8, device int, location int);
SELECT add_dimension('not_hypertable', 'time', interval_length => 500);

--adding a non-exist column
SELECT add_dimension('test_schema.test_table', 'nope', 2);

--adding the same dimension twice should fail
select add_dimension('test_schema.test_table', 'location', 2);

--adding dimension with both number_partitions and interval_length should fail
select add_dimension('test_schema.test_table', 'id2', number_partitions => 2, interval_length => 1000);

--adding a new dimension on a non-empty table should also fail
insert into test_schema.test_table values (123456789, 23.8, 'blue', 'type1', 'nyc', 1, 1);
select add_dimension('test_schema.test_table', 'device_type', 2);
\set ON_ERROR_STOP 1

--show chunks in the associated schema
\dt "chunk_schema".*

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

-- Reset GRANTS
\c single :ROLE_SUPERUSER
REVOKE :ROLE_DEFAULT_PERM_USER FROM :ROLE_DEFAULT_PERM_USER_2;
