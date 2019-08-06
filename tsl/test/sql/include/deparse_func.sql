-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_tabledef(tbl REGCLASS) RETURNS TEXT
AS :TSL_MODULE_PATHNAME, 'tsl_test_get_tabledef' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION tsl_test_deparse_drop_chunks(older_than "any" = NULL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    newer_than "any" = NULL,
    verbose BOOLEAN = FALSE,
    cascade_to_materializations BOOLEAN = NULL) RETURNS TEXT
AS :TSL_MODULE_PATHNAME, 'tsl_test_deparse_drop_chunks' LANGUAGE C VOLATILE;

SET ROLE :ROLE_DEFAULT_PERM_USER;