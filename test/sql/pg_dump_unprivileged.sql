-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c template1 :ROLE_SUPERUSER

SET client_min_messages TO ERROR;
CREATE EXTENSION IF NOT EXISTS timescaledb;
RESET client_min_messages;

CREATE USER dump_unprivileged CREATEDB;

\c template1 dump_unprivileged
CREATE database dump_unprivileged;

\! utils/pg_dump_unprivileged.sh

\c template1 :ROLE_SUPERUSER
-- DROP EXTENSION timescaledb;
DROP DATABASE dump_unprivileged;
DROP USER dump_unprivileged;

-- run this test only if predefined role pg_database_owner exists
SELECT application_name FROM pg_stat_activity WHERE application_name LIKE 'User-Defined Action%';
SELECT exists (SELECT rolname FROM pg_authid where rolname like 'pg_database_owner') AS owner_role_exists \gset

\if :owner_role_exists
CREATE USER dump_owner CREATEDB;
-- create a database owned by user dump_owner
DROP DATABASE IF EXISTS dump_owner;
CREATE database dump_owner owner dump_owner;
-- turn on restoring, restore the database from the dump and turn off restoring 
\c dump_owner :ROLE_SUPERUSER
CREATE EXTENSION IF NOT EXISTS timescaledb; 
\c dump_owner dump_owner;
-- create a table, insert a few rows, just to check 
-- that restore worked ok
create table mytable(time timestamptz not null, a int);
select create_hypertable('mytable', 'time');

insert into mytable values ('2022-01-01'::timestamptz, 1), ('2022-01-02'::timestamptz, 2);

select * from mytable;

\! utils/pg_dump_owner.sh dump/pg_dump_owner.sql

\c template1 :ROLE_SUPERUSER
DROP EXTENSION timescaledb;
drop database dump_owner;

CREATE database dump_owner_restored owner dump_owner;
\c dump_owner_restored :ROLE_SUPERUSER
CREATE EXTENSION IF NOT EXISTS timescaledb;


\c dump_owner_restored dump_owner;
SELECT timescaledb_pre_restore();

show timescaledb.restoring;

\! utils/pg_restore_owner.sh dump/pg_dump_owner.sql

SELECT timescaledb_post_restore();

show timescaledb.restoring;

select * from mytable;

\endif