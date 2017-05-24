\ir include/create_single_db.sql

create schema test_schema;
create table test_schema.test_table(time bigint, temp float8, device_id text);
\dt "test_schema".*
select * from create_hypertable('test_schema.test_table', 'time', 'device_id', 2);

--test partitioning in only time dimension
create table test_schema.test_1dim(time timestamp, temp float);
select create_hypertable('test_schema.test_1dim', 'time');

\dt "test_schema".*

select create_hypertable('test_schema.test_1dim', 'time', if_not_exists => true);

-- Should error when creating again without if_not_exists set to true
\set ON_ERROR_STOP 0
select create_hypertable('test_schema.test_1dim', 'time');
\set ON_ERROR_STOP 1

