-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE OR REPLACE FUNCTION test.tsl_override_current_timestamptz(new_value TIMESTAMPTZ)
RETURNS VOID AS :TSL_MODULE_PATHNAME, 'ts_test_override_current_timestamptz' LANGUAGE C VOLATILE;
