/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include "export.h"
#include "deparse.h"

TS_FUNCTION_INFO_V1(tsl_test_get_tabledef);
TS_FUNCTION_INFO_V1(tsl_test_deparse_drop_chunks);

Datum
tsl_test_get_tabledef(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	const char *cmd = deparse_get_tabledef_commands_concat(relid);
	PG_RETURN_TEXT_P(cstring_to_text(cmd));
}

Datum
tsl_test_deparse_drop_chunks(PG_FUNCTION_ARGS)
{
	Name table_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1);
	Name schema_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	Datum older_than_datum = PG_GETARG_DATUM(0);
	Datum newer_than_datum = PG_GETARG_DATUM(4);
	Oid older_than_type = PG_ARGISNULL(0) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 0);
	Oid newer_than_type = PG_ARGISNULL(4) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 4);
	bool cascade = PG_GETARG_BOOL(3);
	bool verbose = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5);
	bool cascades_to_materializations = PG_ARGISNULL(6) ? false : PG_GETARG_BOOL(6);
	const char *sql_cmd;

	sql_cmd = deparse_drop_chunks_func(table_name,
									   schema_name,
									   older_than_datum,
									   newer_than_datum,
									   older_than_type,
									   newer_than_type,
									   cascade,
									   cascades_to_materializations,
									   verbose);

	PG_RETURN_TEXT_P(cstring_to_text(sql_cmd));
}
