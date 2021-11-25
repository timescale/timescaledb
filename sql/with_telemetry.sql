-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.get_telemetry_report() RETURNS jsonb
    AS '@MODULE_PATHNAME@', 'ts_telemetry_get_report_jsonb'
	LANGUAGE C STABLE PARALLEL SAFE;

INSERT INTO _timescaledb_config.bgw_job (id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled) VALUES
(1, 'Telemetry Reporter [1]', INTERVAL '24h', INTERVAL '100s', -1, INTERVAL '1h', '_timescaledb_internal', 'policy_telemetry', CURRENT_ROLE, true)
ON CONFLICT (id) DO NOTHING;
