-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP EXTENSION timescaledb;

-- test that installation script errors when any of our internal schemas already exists
\set ON_ERROR_STOP 0
CREATE SCHEMA _timescaledb_catalog;
CREATE EXTENSION timescaledb;
DROP SCHEMA _timescaledb_catalog;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA _timescaledb_internal;
CREATE EXTENSION timescaledb;
DROP SCHEMA _timescaledb_internal;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA _timescaledb_cache;
CREATE EXTENSION timescaledb;
DROP SCHEMA _timescaledb_cache;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA _timescaledb_config;
CREATE EXTENSION timescaledb;
DROP SCHEMA _timescaledb_config;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA timescaledb_experimental;
CREATE EXTENSION timescaledb;
DROP SCHEMA timescaledb_experimental;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA timescaledb_information;
CREATE EXTENSION timescaledb;
DROP SCHEMA timescaledb_information;

-- test that installation script errors when any of the function in public schema already exists
-- we don't test every public function but just a few common ones
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE FUNCTION time_bucket(int,int) RETURNS int LANGUAGE SQL AS $$ SELECT 1::int; $$;
CREATE EXTENSION timescaledb;
DROP FUNCTION time_bucket;

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION show_chunks(relation regclass, older_than "any" DEFAULT NULL, newer_than "any" DEFAULT NULL) RETURNS SETOF regclass language internal as 'pg_partition_ancestors';
CREATE EXTENSION timescaledb;
DROP FUNCTION show_chunks;

\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;

