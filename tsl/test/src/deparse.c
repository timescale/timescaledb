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

Datum
tsl_test_get_tabledef(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	const char *cmd = deparse_get_tabledef_commands_concat(relid);
	PG_RETURN_TEXT_P(cstring_to_text(cmd));
}
