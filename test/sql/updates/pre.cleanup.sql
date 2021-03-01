-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Clean up objects that are created by the setup files. Ideally, we
-- should clean them up in each post.*.sql file after generating the
-- output, but now we do it here.

SELECT :'TEST_VERSION' >= '2.0.0' AS has_create_mat_view \gset

SET client_min_messages TO WARNING;

\if :has_create_mat_view
DROP MATERIALIZED VIEW IF EXISTS mat_inval CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_drop CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_before CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_conflict CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_inttime CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_inttime2 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mat_ignoreinval CASCADE;
DROP MATERIALIZED VIEW IF EXISTS cagg.realtime_mat CASCADE;
\else
DROP VIEW IF EXISTS mat_inval CASCADE;
DROP VIEW IF EXISTS mat_drop CASCADE;
DROP VIEW IF EXISTS mat_before CASCADE;
DROP VIEW IF EXISTS mat_conflict CASCADE;
DROP VIEW IF EXISTS mat_inttime CASCADE;
DROP VIEW IF EXISTS mat_inttime2 CASCADE;
DROP VIEW IF EXISTS mat_ignoreinval CASCADE;
DROP VIEW IF EXISTS cagg.realtime_mat CASCADE;
\endif

DROP TABLE IF EXISTS public.hyper_timestamp;
DROP TABLE IF EXISTS public."two_Partitions";
DROP TABLE IF EXISTS conditions_before;
DROP TABLE IF EXISTS inval_test;
DROP TABLE IF EXISTS int_time_test;
DROP TABLE IF EXISTS conflict_test;
DROP TABLE IF EXISTS drop_test;
DROP TABLE IF EXISTS repair_test_timestamptz;
DROP TABLE IF EXISTS repair_test_int;
DROP TABLE IF EXISTS repair_test_extra;
DROP TABLE IF EXISTS repair_test_timestamp;
DROP TABLE IF EXISTS repair_test_date;
DROP TABLE IF EXISTS compress;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS disthyper;
DROP TABLE IF EXISTS policy_test_timestamptz;

DROP TYPE IF EXISTS custom_type;
DROP TYPE IF EXISTS custom_type_for_compression;

DROP PROCEDURE IF EXISTS _timescaledb_testing.restart_dimension_slice_id;
DROP PROCEDURE IF EXISTS _timescaledb_testing.stop_workers;

DROP FUNCTION IF EXISTS timescaledb_integrity_test;
DROP FUNCTION IF EXISTS timescaledb_catalog_has_no_missing_columns;
DROP FUNCTION IF EXISTS integer_now_test;

DROP SCHEMA IF EXISTS cagg;
DROP SCHEMA IF EXISTS _timescaledb_testing;


RESET client_min_messages;
