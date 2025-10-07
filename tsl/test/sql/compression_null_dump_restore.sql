-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This test is to verify fixing the behaviour reported in: SDC #2912

-- Create destination database
\set TEST_DBNAME_3 :TEST_DBNAME _3
\c postgres :ROLE_SUPERUSER
CREATE DATABASE :TEST_DBNAME_3;
\c :TEST_DBNAME_3 :ROLE_SUPERUSER
create extension timescaledb CASCADE;

-- Create source database
\set TEST_DBNAME_2 :TEST_DBNAME _2
\c postgres :ROLE_SUPERUSER
CREATE DATABASE :TEST_DBNAME_2;
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
create extension timescaledb CASCADE;

-- Create compressed table with one column having all NULL
-- values, so it will be compressed with the NULL algorithm
--
create table null_dump (ts int primary key, n int);
select create_hypertable('null_dump', 'ts');
insert into null_dump values (1), (2), (3), (4);
alter table null_dump set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
select compress_chunk(show_chunks('null_dump'));

-- Dump the content of the source database
\setenv TEST_DBNAME :TEST_DBNAME_2
\setenv DUMP_OPTIONS '--quote-all-identifiers --no-tablespaces --no-owner --no-privileges --exclude-schema=test'
\c postgres :ROLE_SUPERUSER
\! utils/pg_dump_aux_plain_dump.sh dump/null-compress-dump.sql

-- Restore the data into the destination database
\c :TEST_DBNAME_3 :ROLE_SUPERUSER
select public.timescaledb_pre_restore();
\set ECHO none
\o /dev/null
SET client_min_messages = 'error';
\i dump/null-compress-dump.sql
\o
\set ECHO queries
select public.timescaledb_post_restore();

select * from public.null_dump order by 1;

\c postgres :ROLE_SUPERUSER
drop database :TEST_DBNAME_2 WITH (FORCE);
drop database :TEST_DBNAME_3 WITH (FORCE);
