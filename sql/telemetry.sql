-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.get_telemetry_report() RETURNS jsonb
    AS '@MODULE_PATHNAME@', 'ts_telemetry_get_report_jsonb'
	LANGUAGE C STABLE PARALLEL SAFE;
