-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We drop the telemetry function here since we have a non-telemetry
-- build and the function might exist.
DROP FUNCTION IF EXISTS @extschema@.get_telemetry_report;

-- We delete the telemetry job since this is a non-telemetry build.
DELETE FROM _timescaledb_config.bgw_job WHERE id = 1;


