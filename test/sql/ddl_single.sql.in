-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";

\ir include/ddl_ops_2.sql

SELECT * FROM test.show_columns('PUBLIC."Hypertable_1"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');

SELECT * FROM PUBLIC."Hypertable_1";
