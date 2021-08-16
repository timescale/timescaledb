/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <funcapi.h>

#include <utils.h>
#include <compat/compat.h>
#include <extension.h>
#include "export.h"
#include "deparse.h"

TS_FUNCTION_INFO_V1(ts_test_get_tabledef);

Datum
ts_test_get_tabledef(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	const char *cmd = deparse_get_tabledef_commands_concat(relid);
	PG_RETURN_TEXT_P(cstring_to_text(cmd));
}

TS_FUNCTION_INFO_V1(ts_test_deparse_drop_chunks);

Datum
ts_test_deparse_drop_chunks(PG_FUNCTION_ARGS)
{
	FmgrInfo flinfo;
	FunctionCallInfo fcinfo2 = palloc(SizeForFunctionCallInfo(fcinfo->nargs));
	Oid argtypes[] = { REGCLASSOID, ANYOID, ANYOID, BOOLOID };
	Oid funcid = ts_get_function_oid("drop_chunks",
									 ts_extension_schema_name(),
									 sizeof(argtypes) / sizeof(*argtypes),
									 argtypes);
	const char *sql_cmd;
	int i;

	fmgr_info(funcid, &flinfo);
	InitFunctionCallInfoData(*fcinfo2,
							 &flinfo,
							 fcinfo->nargs,
							 fcinfo->fncollation,
							 fcinfo->context,
							 fcinfo->resultinfo);

	/* Copy over the arguments into the new function call data */
	for (i = 0; i < fcinfo->nargs; i++)
	{
		FC_ARG(fcinfo2, i) = FC_ARG(fcinfo, i);
		FC_NULL(fcinfo2, i) = FC_NULL(fcinfo, i);
	}

	/* Use the expression from this function so that the deparse function can
	 * result the data types of args with ANY type */
	fcinfo2->flinfo->fn_expr = fcinfo->flinfo->fn_expr;
	sql_cmd = deparse_func_call(fcinfo2);

	PG_RETURN_TEXT_P(cstring_to_text(sql_cmd));
}

TS_FUNCTION_INFO_V1(ts_test_deparse_func);

Datum
ts_test_deparse_func(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	Oid resulttypeid;
	const char *deparsed = deparse_func_call(fcinfo);
	Datum retval = 0;

	elog(NOTICE, "Deparsed: %s", deparsed);

	switch (get_call_result_type(fcinfo, &resulttypeid, &tupdesc))
	{
		case TYPEFUNC_SCALAR:
			retval = BoolGetDatum(true);
			break;
		case TYPEFUNC_COMPOSITE:
		{
			Datum *values = palloc(tupdesc->natts * sizeof(Datum));
			bool *nulls = palloc(tupdesc->natts * sizeof(bool));
			HeapTuple tup;
			int i;

			for (i = 0; i < tupdesc->natts; i++)
				nulls[i] = true;

			tup = heap_form_tuple(tupdesc, values, nulls);
			pfree(values);
			pfree(nulls);
			retval = HeapTupleGetDatum(tup);
			break;
		}
		case TYPEFUNC_RECORD:
			/* indeterminate rowtype result */
		case TYPEFUNC_COMPOSITE_DOMAIN:
			/* domain over determinable rowtype result */
		case TYPEFUNC_OTHER:
			elog(ERROR, "unsupported result type for deparsing");
			break;
	}

	PG_RETURN_DATUM(retval);
}
