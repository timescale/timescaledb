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
#include <utils/snapmgr.h>
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

/*
 * Find proper time_bucket replacement for time_bucket_ng function
 *
 * need_order_flip indicates if the origin and timezone parameters needs to be changed
 * (see inline comment for more details).
 *
 */
static Oid
get_replacement_timebucket_function(const ContinuousAgg *cagg, bool *need_parameter_order_change)
{
	Oid bucket_function = cagg->bucket_function->bucket_function;
	Assert(OidIsValid(bucket_function));

	/* Return type of the current bucket function */
	Oid bucket_function_rettype = get_func_rettype(bucket_function);
	Assert(OidIsValid(bucket_function_rettype));

	FuncInfo *func_info = ts_func_cache_get(bucket_function);
	Ensure(func_info != NULL, "unable to get function info for Oid %d", bucket_function);

	/* The input and output parameters of the function has to be the same
	 * (parameter 0 is the bucket size, parameter 1 the input attribute).
	 */
	Assert(bucket_function_rettype == func_info->arg_types[1]);

	/* Check if the CAgg is actually using a deprecated time_bucket_ng function */
	if (!IS_DEPRECATED_TIME_BUCKET_NG_FUNC(func_info))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("CAgg %s does not use a deprecated bucket function",
						get_rel_name(cagg->relid))));
	}

	/* Should never happen since time_bucket_ng does not support these configuration. But just to be
	 * sure about the configured parameter.
	 */
	Ensure(cagg->bucket_function->bucket_time_based,
		   "CAgg seems to be integer based, however time_bucket_ng does not support this");

	Ensure(cagg->bucket_function->bucket_time_origin,
		   "CAgg seems to have an origin, however time_bucket_ng does not support this");

	/* Return values of func_get_detail */
	Oid funcid;
	Oid rettype;
	bool retset;
	int nvargs;
	Oid vatype;
	Oid *declared_arg_types;
	List *argdefaults;

	List *arg_names = NIL;
	int func_nargs = func_info->nargs;
	Oid *func_arg_types = func_info->arg_types;
	*need_parameter_order_change = false;

	/*
	 * Create a new private copy of the arguments with space for n+1 Oids. We will add one entry or
	 * re-order entries and we don't want to modify the entry in the catalog cache.
	 */
	Assert(!OidIsValid(func_arg_types[func_nargs]));
	func_arg_types = palloc0((func_nargs + 1) * sizeof(Oid));
	memcpy(func_arg_types, func_info->arg_types, func_nargs * sizeof(Oid));
	Assert(!OidIsValid(func_arg_types[func_nargs]));

	/* Add argument for new origin value */
	if (cagg->bucket_function->bucket_time_based &&
		TIMESTAMP_NOT_FINITE(cagg->bucket_function->bucket_time_origin))
	{
		/* Add new origin parameter. The origin parameter has the same type as the return type of
		 * the function. For example, a time_bucket function processing dates, takes also a date a
		 * origin parameter. */
		func_arg_types[func_nargs] = bucket_function_rettype;
		arg_names = list_make1(ORIGIN_PARAMETER_NAME);
		func_nargs++;
	}
	else if (func_nargs == 4)
	{
		/*
		 * timebucket_ng and time_bucket take the timezone at different positions,
		 *   change parameter order to help PostgreSQL to find the right function:
		 *
		 * time_bucket_ng
		 *  bucket_width interval, ts timestamp with time zone,
		 *        origin timestamp with time zone, timezone text
		 *
		 * time_bucket
		 * bucket_width interval, ts timestamp with time zone, timezone text,
		 *     origin timestamp with time zone DEFAULT NULL,
		 *     offset interval DEFAULT NULL
		 */

		if (func_arg_types[2] == TIMESTAMPTZOID && func_arg_types[3] == TEXTOID)
		{
			func_arg_types[2] = TEXTOID;
			func_arg_types[3] = TIMESTAMPTZOID;
			*need_parameter_order_change = true;
		}
	}

	FuncDetailCode fdresult = func_get_detail(list_make1(makeString("time_bucket")),
											  NIL,
											  arg_names,
											  func_nargs,
											  func_arg_types,
											  true /* expand_variadic */,
											  true /* expand_defaults */,
											  false /* include_out_arguments */,
											  &funcid,
											  &rettype,
											  &retset,
											  &nvargs,
											  &vatype,
											  &declared_arg_types,
											  &argdefaults);

	if (fdresult == FUNCDETAIL_NOTFOUND)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unable to find replacement for function %s", func_info->funcname)));
	}

	Assert(fdresult == FUNCDETAIL_NORMAL);

	Ensure(rettype == bucket_function_rettype,
		   "unable to find a bucket replacement function with the same return type");

	FuncInfo *func_info_new = ts_func_cache_get(funcid);
	Ensure(func_info_new != NULL, "unable to get function info for Oid %d", funcid);
	Ensure(func_info_new->allowed_in_cagg_definition,
		   "new time_bucket function is not allowed in CAggs");

	return funcid;
}

/*
 * Update the cagg bucket function catalog table. During the migration, we set a new bucket
 * function and a origin if the bucket function is time based.
 */
static ScanTupleResult
cagg_time_bucket_update(TupleInfo *ti, void *data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	TupleDesc tupleDesc = ts_scanner_get_tupledesc(ti);
	const ContinuousAgg *cagg = (ContinuousAgg *) data;

	Datum values[Natts_continuous_aggs_bucket_function] = { 0 };
	bool isnull[Natts_continuous_aggs_bucket_function] = { 0 };
	bool doReplace[Natts_continuous_aggs_bucket_function] = { 0 };

	/* Update the bucket function */
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_function)] =
		CStringGetTextDatum(format_procedure_qualified(cagg->bucket_function->bucket_function));
	doReplace[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_function)] = true;

	/* Set new origin if not already present. Time_bucket and time_bucket_ng use different
	 * origin values for time based values.
	 */
	if (cagg->bucket_function->bucket_time_based)
	{
		char *origin_value = DatumGetCString(
			DirectFunctionCall1(timestamptz_out,
								TimestampTzGetDatum(cagg->bucket_function->bucket_time_origin)));

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)] =
			CStringGetTextDatum(origin_value);

		doReplace[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)] =
			true;
	}

	HeapTuple new_tuple = heap_modify_tuple(tuple, tupleDesc, values, isnull, doReplace);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

/*
 * Search for the bucket function entry in the catalog and update the values.
 */
static int
replace_time_bucket_function_in_catalog(ContinuousAgg *cagg)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_bucket_function_pkey_mat_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(cagg->data.mat_hypertable_id));

	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGGS_BUCKET_FUNCTION),
		.index = catalog_get_index(catalog,
								   CONTINUOUS_AGGS_BUCKET_FUNCTION,
								   CONTINUOUS_AGGS_BUCKET_FUNCTION_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.data = cagg,
		.limit = 1,
		.tuple_found = cagg_time_bucket_update,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan(&scanctx);
}

typedef struct TimeBucketInfoContext
{
	/* The updated cagg definition */
	const ContinuousAgg *cagg;

	/* The Oid of the old bucket function that should be replaced */
	Oid function_to_replace;

	/* Was the defined origin added during the migration and needs
	 * to be added to the function parameters during rewrite? */
	bool origin_added_during_migration;

	/* Do we need to flip the timezone and the origin parameter during migration? */
	bool need_parameter_order_change;
} TimeBucketInfoContext;

/*
 * Build a new const value for the origin parameter of the given type
 */
static Const *
build_const_value_for_origin(TimeBucketInfoContext *context, Oid origin_type)
{
	Datum const_datum;

	switch (origin_type)
	{
		case TIMESTAMPTZOID:
			const_datum = TimestampTzGetDatum(context->cagg->bucket_function->bucket_time_origin);
			break;
		case TIMESTAMPOID:
			const_datum =
				DirectFunctionCall1(timestamptz_timestamp,
									TimestampTzGetDatum(
										context->cagg->bucket_function->bucket_time_origin));
			break;
		case DATEOID:
			const_datum =
				DirectFunctionCall1(timestamptz_date,
									TimestampTzGetDatum(
										context->cagg->bucket_function->bucket_time_origin));
			break;
		default:
			elog(ERROR,
				 "unable to build const value for bucket function with unsupported return type: %s",
				 format_type_extended(origin_type, -1, 0));
			pg_unreachable();
	}

	TypeCacheEntry *tce = lookup_type_cache(origin_type, 0);

	/* The new origin value for the function */
	Const *const_value = makeConst(origin_type /* consttype */,
								   -1 /* consttypmod */,
								   InvalidOid /* constcollid */,
								   tce->typlen /* constlen */,
								   const_datum /* constvalue */,
								   false /* constisnull */,
								   tce->typbyval /* constbyval */
	);

	return const_value;
}

/*
 * Replace the old time_bucket_ng function with the time_bucket function. Add a custom origin if
 * needed.
 */
static Node *
cagg_user_query_mutator(Node *node, TimeBucketInfoContext *context)
{
	Assert(context != NULL);

	if (node == NULL)
		return NULL;

	if (IsA(node, FuncExpr))
	{
		FuncExpr *funcExpr = castNode(FuncExpr, node);

		if (context->function_to_replace == funcExpr->funcid)
		{
			FuncExpr *new_func = (FuncExpr *) copyObject(node);
			new_func->funcid = context->cagg->bucket_function->bucket_function;

			/* Origin can be added during migration OR we have an existing origin and need to shift
			 * parameters */
			Assert(context->origin_added_during_migration == false ||
				   context->need_parameter_order_change == false);

			/* Ensure the new bucket function produces buckets with the same origin as the old one
			 */
			if (context->origin_added_during_migration)
			{
				/* The origin parameter of the bucket function has the same type as the function
				 * result. Build a new const value for the origin parameter of the needed type. */
				Const *origin = build_const_value_for_origin(context, funcExpr->funcresulttype);

				/* Build wrapping named arg node */
				NamedArgExpr *origin_arg = makeNode(NamedArgExpr);
				origin_arg->argnumber = list_length(funcExpr->args);
				origin_arg->location = -1;
				origin_arg->name = ORIGIN_PARAMETER_NAME;
				origin_arg->arg = (Expr *) origin;

				new_func->args = lappend(new_func->args, origin_arg);
			}

			if (context->need_parameter_order_change)
			{
				/* The existing CAgg should to have an origin before migration */
				Assert(context->origin_added_during_migration == false);

				/* We need 4 elements (see comment in get_replacement_timebucket_function())*/
				Assert(list_length(new_func->args) == 4);

				/* Change parameter order of origin and timezone */
				ListCell lc = new_func->args->elements[2];
				new_func->args->elements[2] = new_func->args->elements[3];
				new_func->args->elements[3] = lc;
			}

			return (Node *) new_func;
		}
	}
	else if (IsA(node, Query))
	{
		/* Recurse into subselects */
		Query *query = castNode(Query, node);
		return (Node *) query_tree_mutator(query, cagg_user_query_mutator, context, 0);
	}

	return expression_tree_mutator(node, cagg_user_query_mutator, context);
}

/*
 * Rewrite the given CAgg view and replace the bucket function
 */
static void
continuous_agg_rewrite_view(Oid view_oid, const ContinuousAgg *cagg, TimeBucketInfoContext *context)
{
	int sec_ctx;
	Oid uid, saved_uid;

	Relation direct_view_rel = relation_open(view_oid, AccessShareLock);
	Query *direct_query = copyObject(get_view_query(direct_view_rel));

	/* Keep lock until end of transaction. */
	relation_close(direct_view_rel, NoLock);

	RemoveRangeTableEntries(direct_query);

	/* Update bucket function in CAgg query */
	Query *updated_direct_query = (Query *) cagg_user_query_mutator((Node *) direct_query, context);

	/* Store updated CAgg query */
	SWITCH_TO_TS_USER(NameStr(cagg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(view_oid, updated_direct_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Replace the bucket function in the CAgg view definition
 */
static void
continuous_agg_replace_function(const ContinuousAgg *cagg, Oid function_to_replace,
								bool origin_added_during_migration,
								bool need_parameter_order_change)
{
	TimeBucketInfoContext context = { 0 };
	context.cagg = cagg;
	context.function_to_replace = function_to_replace;
	context.origin_added_during_migration = origin_added_during_migration;
	context.need_parameter_order_change = need_parameter_order_change;

	/* Rewrite the direct_view */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);

	continuous_agg_rewrite_view(direct_view_oid, cagg, &context);

	/* Rewrite the partial_view */
	Oid partial_view_oid = ts_get_relation_relid(NameStr(cagg->data.partial_view_schema),
												 NameStr(cagg->data.partial_view_name),
												 false);

	continuous_agg_rewrite_view(partial_view_oid, cagg, &context);

	/* Rewrite the user facing view if needed */
	if (!cagg->data.materialized_only)
	{
		Oid user_view_oid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
												  NameStr(cagg->data.user_view_name),
												  false);

		continuous_agg_rewrite_view(user_view_oid, cagg, &context);
	}
}

/*
 * Get the default origin value for time_bucket to be compatible with
 * the default origin of time_bucket_ng.
 */
static TimestampTz
continuous_agg_get_default_origin(Oid new_bucket_function)
{
	Assert(OidIsValid(new_bucket_function));

	Oid bucket_function_rettype = get_func_rettype(new_bucket_function);
	Assert(OidIsValid(bucket_function_rettype));

	Datum origin;

	/* Get default origin for the given datatype */
	if (bucket_function_rettype == TIMESTAMPTZOID)
	{
		origin =
			DirectFunctionCall3(timestamptz_in,
								CStringGetDatum(TIME_BUCKET_NG_DEFAULT_ORIGIN_TIMESTAMPSTAMPTZ),
								ObjectIdGetDatum(InvalidOid),
								Int32GetDatum(-1));
	}
	else if (bucket_function_rettype == TIMESTAMPOID)
	{
		origin =
			DirectFunctionCall1(timestamp_timestamptz,
								DirectFunctionCall3(timestamp_in,
													CStringGetDatum(
														TIME_BUCKET_NG_DEFAULT_ORIGIN_TIMESTAMP),
													ObjectIdGetDatum(InvalidOid),
													Int32GetDatum(-1)));
	}
	else if (bucket_function_rettype == DATEOID)
	{
		origin = DirectFunctionCall1(date_timestamptz,
									 DirectFunctionCall1(date_in,
														 CStringGetDatum(
															 TIME_BUCKET_NG_DEFAULT_ORIGIN_DATE)));
	}
	else
	{
		elog(ERROR,
			 "unable to determine default origin for time_bucket of type %s",
			 format_type_extended(bucket_function_rettype, -1, 0));
		pg_unreachable();
	}

	return DatumGetTimestampTz(origin);
}

/*
 * Migrate a CAgg which is using time_bucket_ng as a bucket function into
 * a CAgg which is using the regular time_bucket function.
 */
Datum
continuous_agg_migrate_to_time_bucket(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);

	ts_feature_flag_check(FEATURE_CAGG);

	ContinuousAgg *cagg = cagg_get_by_relid_or_fail(cagg_relid);
	Assert(cagg_relid == cagg->relid);

	/* Allow migration only if owner */
	if (!object_ownercheck(RelationRelationId, cagg->relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(cagg->relid)),
					   get_rel_name(cagg->relid));

	PreventCommandIfReadOnly("continuous_agg_migrate_to_time_bucket");

	/* Allow migration only on finalized CAggs */
	if (!ContinuousAggIsFinalized(cagg))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on continuous aggregates that are not "
						"finalized"),
				 errhint("Run \"CALL cagg_migrate('%s.%s');\" to migrate to the new "
						 "format.",
						 NameStr(cagg->data.user_view_schema),
						 NameStr(cagg->data.user_view_name))));
	}

	/* Ensure CAgg is not updated/deleted by somebody else concurrently. Should be moved
	 * into the scanner since the CAgg can be deleted after we found it in the catalog. */
	Assert(OidIsValid(cagg_relid));
	LockRelationOid(cagg_relid, ShareUpdateExclusiveLock);

	/* Get the new time_bucket replacement function */
	bool need_parameter_order_change;
	Oid new_bucket_function =
		get_replacement_timebucket_function(cagg, &need_parameter_order_change);
	Oid old_bucket_function = cagg->bucket_function->bucket_function;

	Assert(OidIsValid(new_bucket_function));
	Assert(new_bucket_function != cagg->bucket_function->bucket_function);

	/* Update the time_bucket_fuction */
	cagg->bucket_function->bucket_function = new_bucket_function;
	bool origin_added_during_migration = false;

	/* Set new origin if not already present in the function definition. This is needed since
	 * time_bucket and time_bucket_ng use different origin default values.
	 */
	if (cagg->bucket_function->bucket_time_based &&
		TIMESTAMP_NOT_FINITE(cagg->bucket_function->bucket_time_origin))
	{
		cagg->bucket_function->bucket_time_origin =
			continuous_agg_get_default_origin(new_bucket_function);
		origin_added_during_migration = true;
	}

	/* Update the catalog */
	replace_time_bucket_function_in_catalog(cagg);

	/* Fetch new CAgg definition from catalog */
	ContinuousAgg PG_USED_FOR_ASSERTS_ONLY *new_cagg_definition =
		cagg_get_by_relid_or_fail(cagg_relid);
	Assert(new_cagg_definition->bucket_function->bucket_function == new_bucket_function);
	Assert(cagg->bucket_function->bucket_time_origin ==
		   new_cagg_definition->bucket_function->bucket_time_origin);

	/* Modify the CAgg view definition */
	continuous_agg_replace_function(cagg,
									old_bucket_function,
									origin_added_during_migration,
									need_parameter_order_change);

	/* The migration is a procedure, no return value is expected */
	PG_RETURN_VOID();
}

/* Get the Oid of the direct view of the CAgg. We cannot use the TimescaleDB internal
 * functions such as ts_continuous_agg_find_by_mat_hypertable_id() at this point since this
 * function can be called during an extension upgrade and ts_catalog_get() does not work.
 */
static Oid
get_direct_view_oid(int32 mat_hypertable_id)
{
	RangeVar *ts_cagg = makeRangeVar(CATALOG_SCHEMA_NAME,
									 CONTINUOUS_AGG_TABLE_NAME,
									 -1 /* taken location unknown */);
	Relation cagg_rel = relation_openrv_extended(ts_cagg, AccessShareLock, /* missing ok */ true);

	RangeVar *ts_cagg_idx =
		makeRangeVar(CATALOG_SCHEMA_NAME, TS_CAGG_CATALOG_IDX, -1 /* taken location unknown */);
	Relation cagg_idx_rel =
		relation_openrv_extended(ts_cagg_idx, AccessShareLock, /* missing ok */ true);

	/* Prepare relation scan */
	TupleTableSlot *slot = table_slot_create(cagg_rel, NULL);
	ScanKeyData scankeys[1];
	ScanKeyEntryInitialize(&scankeys[0],
						   0,
						   1,
						   BTEqualStrategyNumber,
						   InvalidOid,
						   InvalidOid,
						   F_INT4EQ,
						   Int32GetDatum(mat_hypertable_id));

	/* Prepare index scan */
	IndexScanDesc indexscan =
		index_beginscan(cagg_rel, cagg_idx_rel, GetTransactionSnapshot(), 1, 0);
	index_rescan(indexscan, scankeys, 1, NULL, 0);

	/* Read tuple from relation */
	bool got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	if (!got_next_slot)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid materialized hypertable ID: %d", mat_hypertable_id)));
	bool is_null = false;

	/* We use the view_schema and view_name to get data from the system catalog. Even char pointers
	 * are passed to the catalog, it calls namehashfast() internally, which assumes that a char of
	 * NAMEDATALEN is allocated. */
	NameData view_schema_name;
	NameData view_name_name;

	/* We need to get the attribute names dynamically since this function can be called during an
	 * upgrade and the fixed attribution numbers (i.e., Anum_continuous_agg_direct_view_schema) can
	 * be different. */
	AttrNumber direct_view_schema_attr = get_attnum(cagg_rel->rd_id, "direct_view_schema");
	Ensure(direct_view_schema_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_schema");

	AttrNumber direct_view_name_attr = get_attnum(cagg_rel->rd_id, "direct_view_name");
	Ensure(direct_view_name_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_name");

	char *view_schema = DatumGetCString(slot_getattr(slot, direct_view_schema_attr, &is_null));
	Ensure(!is_null, "unable to get view schema for oid %d", mat_hypertable_id);
	Assert(view_schema != NULL);
	namestrcpy(&view_schema_name, view_schema);

	char *view_name = DatumGetCString(slot_getattr(slot, direct_view_name_attr, &is_null));
	Ensure(!is_null, "unable to get view name for oid %d", mat_hypertable_id);
	Assert(view_name != NULL);
	namestrcpy(&view_name_name, view_name);

	got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	Ensure(!got_next_slot, "found duplicate definitions for CAgg mat_ht %d", mat_hypertable_id);

	/* End relation scan */
	index_endscan(indexscan);
	ExecDropSingleTupleTableSlot(slot);
	relation_close(cagg_rel, AccessShareLock);
	relation_close(cagg_idx_rel, AccessShareLock);

	/* Get Oid of user view */
	Oid direct_view_oid =
		ts_get_relation_relid(NameStr(view_schema_name), NameStr(view_name_name), false);
	Assert(OidIsValid(direct_view_oid));
	return direct_view_oid;
}

enum
{
	Anum_cagg_bucket_function_oid = 1,
	Anum_cagg_bucket_function_width,
	Anum_cagg_bucket_function_origin,
	Anum_cagg_bucket_function_offset,
	Anum_cagg_bucket_function_timezone,
	Anum_cagg_bucket_function_fixed_width,
	_Anum_cagg_bucket_function_max
};

#define Natts_cagg_bucket_function (_Anum_cagg_bucket_function_max - 1)

static Datum
create_cagg_get_bucket_function_datum(TupleDesc tupdesc, ContinuousAggsBucketFunction *bf)
{
	NullableDatum datums[Natts_cagg_bucket_function] = { { 0 } };
	HeapTuple tuple;

	char *bucket_origin = NULL;
	char *bucket_offset = NULL;
	char *bucket_width = NULL;

	if (IS_TIME_BUCKET_INFO_TIME_BASED(bf))
	{
		/* Bucketing on time */
		Assert(bf->bucket_time_width != NULL);
		bucket_width = DatumGetCString(
			DirectFunctionCall1(interval_out, IntervalPGetDatum(bf->bucket_time_width)));

		if (!TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
		{
			bucket_origin = DatumGetCString(
				DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(bf->bucket_time_origin)));
		}

		if (bf->bucket_time_offset != NULL)
		{
			bucket_offset = DatumGetCString(
				DirectFunctionCall1(interval_out, IntervalPGetDatum(bf->bucket_time_offset)));
		}
	}
	else
	{
		/* Bucketing on integers */
		bucket_width = palloc0((MAXINT8LEN + 1) * sizeof(char));
		pg_lltoa(bf->bucket_integer_width, bucket_width);

		/* Integer buckets with origin are not supported, so nothing to do. */
		Assert(bucket_origin == NULL);

		if (bf->bucket_integer_offset != 0)
		{
			bucket_offset = palloc0((MAXINT8LEN + 1) * sizeof(char));
			pg_lltoa(bf->bucket_integer_offset, bucket_offset);
		}
	}

	tupdesc = BlessTupleDesc(tupdesc);

	ts_datum_set_objectid(Anum_cagg_bucket_function_oid, datums, bf->bucket_function);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_width, datums, bucket_width);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_origin, datums, bucket_origin);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_offset, datums, bucket_offset);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_timezone,
								   datums,
								   bf->bucket_time_timezone);
	ts_datum_set_bool(Anum_cagg_bucket_function_fixed_width, datums, bf->bucket_fixed_interval);

	Assert(tupdesc->natts == Natts_cagg_validate_query);
	tuple = ts_heap_form_tuple(tupdesc, datums);

	return HeapTupleGetDatum(tuple);
}

/*
 * Get the bucket function information for the given materialized hypertable id.
 *
 * When running `cagg_get_bucket_function_info` the function returns the following fields:
 * - oid: The Oid of the bucket function
 * - width: The width of the bucket function
 * - origin: The origin of the bucket function
 * - offset: The offset of the bucket function
 * - timezone: The timezone of the bucket function
 * - fixed_width: Is the bucket width fixed
 *
 * When running `cagg_get_bucket_function` the function returns the following fields:
 * - oid: The Oid of the bucket function
 */
static Datum
cagg_get_bucket_function_datum(int32 mat_hypertable_id, FunctionCallInfo fcinfo)
{
	Oid direct_view_oid = get_direct_view_oid(mat_hypertable_id);
	TupleDesc tupdesc;

	if (fcinfo != NULL && get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	ContinuousAggsBucketFunction *bf = ts_cagg_get_bucket_function_info(direct_view_oid);

	if (!OidIsValid(bf->bucket_function))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("time_bucket function not found in CAgg definition for "
						"mat_ht_id: %d",
						mat_hypertable_id)));
		pg_unreachable();
	}

	if (fcinfo != NULL)
		return create_cagg_get_bucket_function_datum(tupdesc, bf);

	return ObjectIdGetDatum(bf->bucket_function);
}

/*
 * This function returns the `time_bucket` function Oid in the user view definition
 * of a given materialization hupertable.
 */
Datum
continuous_agg_get_bucket_function(PG_FUNCTION_ARGS)
{
	/* Return the oid of the bucket function */
	PG_RETURN_DATUM(cagg_get_bucket_function_datum(PG_GETARG_INT32(0), NULL));
}

/*
 * This function returns all information about the `time_bucket` function of a given
 * materialization hypertable using the user view definition stored in Postgres catalog.
 */
Datum
continuous_agg_get_bucket_function_info(PG_FUNCTION_ARGS)
{
	/* Return all bucket function info */
	PG_RETURN_DATUM(cagg_get_bucket_function_datum(PG_GETARG_INT32(0), fcinfo));
}
