-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- These functions are used when running smoke tests. For smoke tests
-- we assume that we do not have SUPER privileges.

CREATE SCHEMA IF NOT EXISTS _timescaledb_testing;

CREATE PROCEDURE _timescaledb_testing.restart_dimension_slice_id() LANGUAGE SQL AS '';

CREATE PROCEDURE _timescaledb_testing.stop_workers() LANGUAGE SQL AS '';
