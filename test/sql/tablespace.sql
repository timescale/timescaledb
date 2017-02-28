
\set ON_ERROR_STOP 1
\set SHOW_CONTEXT never

\ir include/create_single_db.sql

\set ECHO ALL
\c single

\set ON_ERROR_STOP 0

SET client_min_messages = ERROR;
drop tablespace if exists tspace1;
SET client_min_messages = NOTICE;

\set VERBOSITY verbose

--test hypertable with tables space
create tablespace tspace1 location :TEST_TABLESPACE_PATH;
create table test_tspace(time timestamp, temp float, device_id text) tablespace tspace1;
select create_hypertable('test_tspace', 'time', 'device_id');
select * from _timescaledb_catalog.partition p INNER JOIN _timescaledb_catalog.partition_replica pr ON (pr.partition_id = p.id);
insert into test_tspace values ('2017-01-20T09:00:01', 24.3, 'dev1');
insert into test_tspace values ('2017-01-20T09:00:02', 22.3, 'dev7');
\dt test_tspace

--verify that the table chunk has the correct tablespace
\d+ _timescaledb_internal.*

--cleanup
\set VERBOSITY default
drop table test_tspace;
drop tablespace tspace1;
