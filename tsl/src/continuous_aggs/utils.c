/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <commands/view.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/regproc.h>
#include <utils/timestamp.h>

#include "extension.h"
#include "guc.h"
#include "time_bucket.h"
#include "utils.h"

enum
{
	Anum_cagg_validate_query_valid = 1,
	Anum_cagg_validate_query_error_level,
	Anum_cagg_validate_query_error_code,
	Anum_cagg_validate_query_error_message,
	Anum_cagg_validate_query_error_detail,
	Anum_cagg_validate_query_error_hint,
	_Anum_cagg_validate_query_max
};

#define Natts_cagg_validate_query (_Anum_cagg_validate_query_max - 1)
#define ORIGIN_PARAMETER_NAME "origin"

static Datum
create_cagg_validate_query_datum(TupleDesc tupdesc, const bool is_valid_query,
								 const ErrorData *edata)
{
	NullableDatum datums[Natts_cagg_validate_query] = { { 0 } };
	HeapTuple tuple;

	tupdesc = BlessTupleDesc(tupdesc);

	ts_datum_set_bool(Anum_cagg_validate_query_valid, datums, is_valid_query);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_level,
								   datums,
								   edata->elevel > 0 ? error_severity(edata->elevel) : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_code,
								   datums,
								   edata->sqlerrcode > 0 ? unpack_sql_state(edata->sqlerrcode) :
														   NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_message,
								   datums,
								   edata->message ? edata->message : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_detail,
								   datums,
								   edata->detail ? edata->detail : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_hint,
								   datums,
								   edata->hint ? edata->hint : NULL);

	Assert(tupdesc->natts == Natts_cagg_validate_query);
	tuple = ts_heap_form_tuple(tupdesc, datums);

	return HeapTupleGetDatum(tuple);
}

Datum
continuous_agg_validate_query(PG_FUNCTION_ARGS)
{
	text *query_text = PG_GETARG_TEXT_P(0);
	char *sql;
	bool is_valid_query = false;
	Datum datum_sql;
	TupleDesc tupdesc;
	ErrorData *edata;
	MemoryContext oldcontext = CurrentMemoryContext;

	/* Change $1, $2 ... placeholders to NULL constant. This is necessary to make parser happy */
	sql = text_to_cstring(query_text);
	elog(DEBUG1, "sql: %s", sql);

	datum_sql = CStringGetTextDatum(sql);
	datum_sql = DirectFunctionCall4Coll(textregexreplace,
										C_COLLATION_OID,
										datum_sql,
										CStringGetTextDatum("\\$[0-9]+"),
										CStringGetTextDatum("NULL"),
										CStringGetTextDatum("g"));
	sql = text_to_cstring(DatumGetTextP(datum_sql));
	elog(DEBUG1, "sql: %s", sql);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	PG_TRY();
	{
		List *tree;
		Node *node;
		RawStmt *rawstmt;
		ParseState *pstate;
		Query *query;

		edata = (ErrorData *) palloc0(sizeof(ErrorData));
		edata->message = NULL;
		edata->detail = NULL;
		edata->hint = NULL;

		tree = pg_parse_query(sql);

		if (tree == NIL)
		{
			edata->elevel = ERROR;
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			edata->message = "failed to parse query";
		}
		else if (list_length(tree) > 1)
		{
			edata->elevel = WARNING;
			edata->sqlerrcode = ERRCODE_FEATURE_NOT_SUPPORTED;
			edata->message = "multiple statements are not supported";
		}
		else
		{
			node = linitial(tree);
			rawstmt = (RawStmt *) node;
			pstate = make_parsestate(NULL);

			Assert(IsA(node, RawStmt));

			if (!IsA(rawstmt->stmt, SelectStmt))
			{
				edata->elevel = WARNING;
				edata->sqlerrcode = ERRCODE_FEATURE_NOT_SUPPORTED;
				edata->message = "only select statements are supported";
			}
			else
			{
				pstate->p_sourcetext = sql;
				query = transformTopLevelStmt(pstate, rawstmt);
				free_parsestate(pstate);

				(void) cagg_validate_query(query, true, "public", "cagg_validate", false);
				is_valid_query = true;
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	PG_RETURN_DATUM(create_cagg_validate_query_datum(tupdesc, is_valid_query, edata));
}
