-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- These functions are used when running normal update tests.

CREATE SCHEMA IF NOT EXISTS _timescaledb_testing;

CREATE OR REPLACE PROCEDURE _timescaledb_testing.restart_dimension_slice_id()
LANGUAGE SQL
AS $$
   ALTER SEQUENCE _timescaledb_catalog.dimension_slice_id_seq RESTART WITH 100;
$$;

CREATE OR REPLACE PROCEDURE _timescaledb_testing.stop_workers()
LANGUAGE PLPGSQL
AS $$
BEGIN
   IF EXISTS (SELECT FROM pg_proc WHERE proname='stop_background_workers' AND pronamespace='_timescaledb_internal'::regnamespace)
   THEN PERFORM _timescaledb_internal.stop_background_workers();
   ELSE PERFORM _timescaledb_functions.stop_background_workers();
   END IF;
END
$$;

