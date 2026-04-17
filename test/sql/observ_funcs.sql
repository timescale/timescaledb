-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test the observability functions.

-- Smoke tests, the results should be empty
SELECT * FROM _timescaledb_functions.observ_reset();
SELECT * FROM _timescaledb_functions.observ_keys();
SELECT * FROM _timescaledb_functions.observ_log();
SELECT * FROM _timescaledb_functions.observ_key_values('event_type');
SELECT * FROM _timescaledb_functions.observ_log_events();

