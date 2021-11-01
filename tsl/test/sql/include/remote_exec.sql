-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS test;
GRANT USAGE ON SCHEMA test TO PUBLIC;

CREATE OR REPLACE FUNCTION test.remote_exec(srv_name name[], command text)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_remote_exec'
LANGUAGE C;

CREATE OR REPLACE FUNCTION test.remote_exec_get_result_strings(srv_name name[], command text)
RETURNS TABLE("table_record" CSTRING[])
AS :TSL_MODULE_PATHNAME, 'ts_remote_exec_get_result_strings'
LANGUAGE C;
