-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE DATABASE trusted_test;
GRANT CREATE ON DATABASE trusted_test TO :ROLE_1;

\c trusted_test :ROLE_READ_ONLY
\set ON_ERROR_STOP 0
CREATE EXTENSION timescaledb;
\set ON_ERROR_STOP 1

\c trusted_test :ROLE_1
-- user shouldn't have superuser privilege
SELECT rolsuper FROM pg_roles WHERE rolname=user;

SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
RESET client_min_messages;

CREATE TABLE t(time timestamptz);
SELECT create_hypertable('t','time');

INSERT INTO t VALUES ('2000-01-01'), ('2001-01-01');

SELECT * FROM t ORDER BY 1;
SELECT * FROM timescaledb_information.hypertables;
\dt+ _timescaledb_internal._hyper_*

DROP EXTENSION timescaledb CASCADE;

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE trusted_test;
