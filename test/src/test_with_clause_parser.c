/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <access/htup_details.h>
#include <commands/defrem.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>

#include "export.h"
#include "test_utils.h"
#include "with_clause_parser.h"
#include "annotations.h"

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
		filtered->without = NIL;

		ts_with_clause_filter(def_elems, &filtered->within, &filtered->without);

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
} TestArgs;

static WithClauseDefinition test_args[] = {
	[TestArgUnimpl] = { .arg_name = "unimplemented",
						.type_id = InvalidOid, },
	[TestArgBool] = { .arg_name = "bool", .type_id = BOOLOID, },
	[TestArgInt32] = { .arg_name = "int32", .type_id = INT4OID, },
	[TestArgDefault] = { .arg_name = "default",
						 .type_id = INT4OID,
						 .default_val = (Datum)-100 },
	[TestArgName] = { .arg_name = "name", .type_id = NAMEOID, },
	[TestArgRegclass] = { .arg_name = "regclass",
						  .type_id = REGCLASSOID },
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

	values[0] = CStringGetTextDatum(test_args[result->i].arg_name);
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
