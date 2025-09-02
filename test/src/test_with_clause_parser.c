/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <fmgr.h>
#include <funcapi.h>
#include <postgres_ext.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/regproc.h>

#include "annotations.h"
#include "export.h"
#include "test_utils.h"
#include "with_clause/with_clause_parser.h"

TS_FUNCTION_INFO_V1(ts_sqlstate_raise_in);
TS_FUNCTION_INFO_V1(ts_sqlstate_raise_out);

/*
 * Input function that will raise the error code give. This means that you can
 * trigger an error when reading and converting a string to this type.
 */
Datum
ts_sqlstate_raise_in(PG_FUNCTION_ARGS)
{
	char *code = PG_GETARG_CSTRING(0);
	if (strlen(code) != 5)
		ereport(ERROR,
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("error code \"%s\" was not of length 5", code));
	int sqlstate = MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4]);
	ereport(ERROR, errcode(sqlstate), errmsg("raised requested error code \"%s\"", code));
	return 0;
}

/*
 * Dummy function, we do not store values of this type anywhere.
 */
Datum
ts_sqlstate_raise_out(PG_FUNCTION_ARGS)
{
	PG_RETURN_CSTRING("uninteresting");
}

static DefElem *
def_elem_from_texts(Datum *texts, int nelems)
{
	DefElem *elem = palloc0(sizeof(*elem));
	switch (nelems)
	{
		case 1:
			elem->defname = text_to_cstring(DatumGetTextP(texts[0]));
			break;
		case 3:
			elem->arg = (Node *) makeString(text_to_cstring(DatumGetTextP(texts[2])));
			TS_FALLTHROUGH;
		case 2:
			elem->defname = text_to_cstring(DatumGetTextP(texts[1]));
			elem->defnamespace = text_to_cstring(DatumGetTextP(texts[0]));
			break;
		default:
			elog(ERROR, "%d elements invalid for defelem", nelems);
	}
	return elem;
}

static List *
def_elems_from_array(ArrayType *with_clause_array)
{
	ArrayMetaState with_clause_meta = { .element_type = TEXTOID };
	ArrayIterator with_clause_iter;
	Datum with_clause_datum;
	bool with_clause_null;
	List *def_elems = NIL;

	get_typlenbyvalalign(with_clause_meta.element_type,
						 &with_clause_meta.typlen,
						 &with_clause_meta.typbyval,
						 &with_clause_meta.typalign);
	with_clause_iter = array_create_iterator(with_clause_array, 1, &with_clause_meta);

	while (array_iterate(with_clause_iter, &with_clause_datum, &with_clause_null))
	{
		Datum *with_clause_fields;
		int with_clause_elems;
		ArrayType *with_clause = DatumGetArrayTypeP(with_clause_datum);
		TestAssertTrue(!with_clause_null);
		deconstruct_array(with_clause,
						  TEXTOID,
						  with_clause_meta.typlen,
						  with_clause_meta.typbyval,
						  with_clause_meta.typalign,
						  &with_clause_fields,
						  NULL,
						  &with_clause_elems);
		def_elems = lappend(def_elems, def_elem_from_texts(with_clause_fields, with_clause_elems));
	}
	return def_elems;
}

typedef struct FilteredWithClauses
{
	List *within;
	List *tigerlake;
	List *without;
} FilteredWithClauses;

static HeapTuple
create_filter_tuple(TupleDesc tuple_desc, DefElem *d, bool within)
{
	Datum *values = palloc0(sizeof(*values) * tuple_desc->natts);
	bool *nulls = palloc0(sizeof(*nulls) * tuple_desc->natts);

	TestAssertTrue(tuple_desc->natts >= 4);

	if (d->defnamespace != NULL)
		values[0] = CStringGetTextDatum(d->defnamespace);
	else
		nulls[0] = true;

	if (d->defname != NULL)
		values[1] = CStringGetTextDatum(d->defname);
	else
		nulls[1] = true;

	if (d->arg != NULL)
		values[2] = CStringGetTextDatum(defGetString(d));
	else
		nulls[2] = true;

	values[3] = BoolGetDatum(within);
	return heap_form_tuple(tuple_desc, values, nulls);
}

TS_TEST_FN(ts_test_with_clause_filter)
{
	FuncCallContext *funcctx;
	FilteredWithClauses *filtered;
	MemoryContext oldcontext;

	if (SRF_IS_FIRSTCALL())
	{
		ArrayType *with_clause_array;
		TupleDesc tupdesc;
		List *def_elems;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		with_clause_array = DatumGetArrayTypeP(PG_GETARG_DATUM(0));

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		def_elems = def_elems_from_array(with_clause_array);

		filtered = palloc(sizeof(*filtered));
		filtered->within = NIL;
		filtered->tigerlake = NIL;
		filtered->without = NIL;

		ts_with_clause_filter(def_elems,
							  &filtered->within,
							  &filtered->tigerlake,
							  &filtered->without);

		funcctx->user_fctx = filtered;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	filtered = funcctx->user_fctx;
	if (filtered->within != NIL)
	{
		HeapTuple tuple;
		DefElem *d = linitial(filtered->within);
		tuple = create_filter_tuple(funcctx->tuple_desc, d, true);
		filtered->within = list_delete_first(filtered->within);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else if (filtered->without != NIL)
	{
		HeapTuple tuple;
		DefElem *d = linitial(filtered->without);
		tuple = create_filter_tuple(funcctx->tuple_desc, d, false);
		filtered->without = list_delete_first(filtered->without);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
		SRF_RETURN_DONE(funcctx);
}

typedef enum TestArgs
{
	TestArgUnimpl = 0,
	TestArgBool,
	TestArgInt32,
	TestArgDefault,
	TestArgName,
	TestArgRegclass,
	TestArgRaise,
} TestArgs;

static WithClauseDefinition test_args[] = {
	[TestArgUnimpl] = { .arg_names = {"unimplemented", NULL},
						.type_id = InvalidOid, },
	[TestArgBool] = { .arg_names = {"bool", NULL}, .type_id = BOOLOID, },
	[TestArgInt32] = { .arg_names = {"int32", NULL}, .type_id = INT4OID, },
	[TestArgDefault] = { .arg_names = {"default", NULL},
						 .type_id = INT4OID,
						 .default_val = (Datum)-100 },
	[TestArgName] = { .arg_names = {"name", NULL}, .type_id = NAMEOID, },
	[TestArgRegclass] = {
		.arg_names = {"regclass", NULL},
		.type_id = REGCLASSOID,
	},
	[TestArgRaise] = {
		.arg_names = {"sqlstate_raise", NULL},
		.type_id = InvalidOid,
	},
};

typedef struct WithClauseValue
{
	WithClauseResult *parsed;
	int i;
} WithClauseValue;

TS_TEST_FN(ts_test_with_clause_parse)
{
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	Datum *values;
	bool *nulls;
	HeapTuple tuple;
	WithClauseValue *result;

	/*
	 * Look up any missing type ids before using it below to allow
	 * user-defined types.
	 *
	 * Note that this will not look up types we have found in previous calls
	 * of this function.
	 *
	 * We use the slightly more complicated way of calling to_regtype since
	 * that exists on all versions of PostgreSQL. We cannot use regtypein
	 * since that can generate errors and we do not want to deal with that.
	 */
	for (unsigned int i = 0; i < TS_ARRAY_LEN(test_args); ++i)
	{
		LOCAL_FCINFO(fcinfo_in, 1);
		Datum result;
		if (!OidIsValid(test_args[i].type_id))
		{
			InitFunctionCallInfoData(*fcinfo_in, NULL, 1, InvalidOid, NULL, NULL);
			fcinfo_in->args[0].value = CStringGetTextDatum(test_args[i].arg_names[0]);
			fcinfo_in->args[0].isnull = false;
			result = to_regtype(fcinfo_in);
			if (!fcinfo_in->isnull)
				test_args[i].type_id = DatumGetObjectId(result);
		}
	}

	if (SRF_IS_FIRSTCALL())
	{
		ArrayType *with_clause_array;
		List *def_elems;
		TupleDesc tupdesc;
		WithClauseResult *parsed;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		with_clause_array = DatumGetArrayTypeP(PG_GETARG_DATUM(0));
		def_elems = def_elems_from_array(with_clause_array);

		parsed = ts_with_clauses_parse(def_elems, test_args, TS_ARRAY_LEN(test_args));
		result = palloc(sizeof(*result));
		result->parsed = parsed;
		result->i = 0;

		funcctx->user_fctx = result;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	result = funcctx->user_fctx;
	if (result == NULL || (size_t) result->i >= TS_ARRAY_LEN(test_args))
		SRF_RETURN_DONE(funcctx);

	values = palloc0(sizeof(*values) * funcctx->tuple_desc->natts);
	nulls = palloc(sizeof(*nulls) * funcctx->tuple_desc->natts);
	memset(nulls, true, sizeof(*nulls) * funcctx->tuple_desc->natts);

	values[0] = CStringGetTextDatum(test_args[result->i].arg_names[0]);
	nulls[0] = false;
	if (!result->parsed[result->i].is_default || result->i == TestArgDefault)
	{
		values[result->i + 1] = result->parsed[result->i].parsed;
		nulls[result->i + 1] = false;
	}

	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

	result->i += 1;
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}
