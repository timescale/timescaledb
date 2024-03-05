/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "utils.h"

static void
fill_values_text(int index, Datum *values, bool *nulls, char *value)
{
	if (value)
	{
		values[AttrNumberGetAttrOffset(index)] = PointerGetDatum(cstring_to_text(value));
		nulls[AttrNumberGetAttrOffset(index)] = false;
	}
	else
		nulls[AttrNumberGetAttrOffset(index)] = true;
}

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

static Datum
create_cagg_validate_query_datum(TupleDesc tupdesc, const bool is_valid_query,
								 const ErrorData *edata)
{
	Datum values[_Anum_cagg_validate_query_max] = { 0 };
	bool nulls[_Anum_cagg_validate_query_max] = { false };
	HeapTuple tuple;

	tupdesc = BlessTupleDesc(tupdesc);

	values[AttrNumberGetAttrOffset(Anum_cagg_validate_query_valid)] = BoolGetDatum(is_valid_query);

	fill_values_text(Anum_cagg_validate_query_error_level,
					 values,
					 nulls,
					 edata->elevel > 0 ? (char *) error_severity(edata->elevel) : NULL);
	fill_values_text(Anum_cagg_validate_query_error_code,
					 values,
					 nulls,
					 edata->sqlerrcode > 0 ? unpack_sql_state(edata->sqlerrcode) : NULL);
	fill_values_text(Anum_cagg_validate_query_error_message, values, nulls, edata->message);
	fill_values_text(Anum_cagg_validate_query_error_detail, values, nulls, edata->detail);
	fill_values_text(Anum_cagg_validate_query_error_hint, values, nulls, edata->hint);

	tuple = heap_form_tuple(tupdesc, values, nulls);

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
