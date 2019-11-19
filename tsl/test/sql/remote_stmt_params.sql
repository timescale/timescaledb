-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION _timescaledb_internal.test_stmt_params_format(binary BOOL)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_stmt_params_format'
LANGUAGE C STRICT;

-- by default we use binary format
SELECT _timescaledb_internal.test_stmt_params_format(true);

SET timescaledb.enable_connection_binary_data = false;

SELECT _timescaledb_internal.test_stmt_params_format(false);
