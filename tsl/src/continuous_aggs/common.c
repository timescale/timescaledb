/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "common.h"

#include <utils/acl.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "guc.h"

static void caggtimebucket_validate(ContinuousAggTimeBucketInfo *tbinfo, List *groupClause,
									List *targetList, List *rtable, bool is_cagg_create);
static Datum get_bucket_width_datum(ContinuousAggTimeBucketInfo bucket_info);
static int64 get_bucket_width(ContinuousAggTimeBucketInfo bucket_info);
static FuncExpr *build_conversion_call(Oid type, FuncExpr *boundary);
static FuncExpr *build_boundary_call(int32 ht_id, Oid type);
static Const *cagg_boundary_make_lower_bound(Oid type);
static Node *build_union_query_quals(int32 ht_id, Oid partcoltype, Oid opno, int varno,
									 AttrNumber attno);
static RangeTblEntry *makeRangeTblEntry(Query *subquery, const char *aliasname);

#define INTERNAL_TO_DATE_FUNCTION "to_date"
#define INTERNAL_TO_TSTZ_FUNCTION "to_timestamp"
#define INTERNAL_TO_TS_FUNCTION "to_timestamp_without_timezone"
#define BOUNDARY_FUNCTION "cagg_watermark"

/*
 * Initialize MaterializationHypertableColumnInfo.
 */
void
mattablecolumninfo_init(MaterializationHypertableColumnInfo *matcolinfo, List *grouplist)
{
	matcolinfo->matcollist = NIL;
	matcolinfo->partial_seltlist = NIL;
	matcolinfo->partial_grouplist = grouplist;
	matcolinfo->mat_groupcolname_list = NIL;
	matcolinfo->matpartcolno = -1;
	matcolinfo->matpartcolname = NULL;
}

/*
 * When a view is created (StoreViewQuery), 2 dummy rtable entries corresponding to "old" and
 * "new" are prepended to the rtable list. We remove these and adjust the varnos to recreate
 * the user or direct view query.
 */
void
RemoveRangeTableEntries(Query *query)
{
#if PG16_LT
	List *rtable = query->rtable;
	Assert(list_length(rtable) >= 3);
	rtable = list_delete_first(rtable);
	query->rtable = list_delete_first(rtable);
	OffsetVarNodes((Node *) query, -2, 0);
	Assert(list_length(query->rtable) >= 1);
#endif
}

/*
 * Extract the final view from the UNION ALL query.
 *
 * q1 is the query on the materialization hypertable with the finalize call
 * q2 is the query on the raw hypertable which was supplied in the initial CREATE VIEW statement
 * returns q1 from:
 * SELECT * from (  SELECT * from q1 where <coale_qual>
 *                  UNION ALL
 *                  SELECT * from q2 where existing_qual and <coale_qual>
 * where coale_qual is: time < ----> (or >= )
 * COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark( <htid>)),
 * '-infinity'::timestamp with time zone)
 * The WHERE clause of the final view is removed.
 */
Query *
destroy_union_query(Query *q)
{
	Assert(q->commandType == CMD_SELECT &&
		   ((SetOperationStmt *) q->setOperations)->op == SETOP_UNION &&
		   ((SetOperationStmt *) q->setOperations)->all == true);

	/* Get RTE of the left-hand side of UNION ALL. */
	RangeTblEntry *rte = linitial(q->rtable);
	Assert(rte->rtekind == RTE_SUBQUERY);

	Query *query = copyObject(rte->subquery);

	/* Delete the WHERE clause from the final view. */
	query->jointree->quals = NULL;

	return query;
}

/*
 * Check if the group-by clauses has exactly 1 time_bucket(.., <col>) where
 * <col> is the hypertable's partitioning column and other invariants. Then fill
 * the `bucket_width` and other fields of `tbinfo`.
 */
static void
caggtimebucket_validate(ContinuousAggTimeBucketInfo *tbinfo, List *groupClause, List *targetList,
						List *rtable, bool is_cagg_create)
{
	bool for_rewrite = false;
	ts_caggtimebucket_validate(tbinfo->bf,
							   groupClause,
							   targetList,
							   rtable,
							   tbinfo->htpartcolno,
							   NULL,
							   is_cagg_create,
							   for_rewrite);
}

static Datum
get_bucket_width_datum(ContinuousAggTimeBucketInfo bucket_info)
{
	Datum width = UnassignedDatum;

	switch (bucket_info.bf->bucket_width_type)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			width = ts_internal_to_interval_value(bucket_info.bf->bucket_integer_width,
												  bucket_info.bf->bucket_width_type);
			break;
		case INTERVALOID:
			width = IntervalPGetDatum(bucket_info.bf->bucket_time_width);
			break;
		default:
			Assert(false);
	}

	return width;
}

static int64
get_bucket_width(ContinuousAggTimeBucketInfo bucket_info)
{
	int64 width = 0;

	/* Calculate the width. */
	switch (bucket_info.bf->bucket_width_type)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			width = bucket_info.bf->bucket_integer_width;
			break;
		case INTERVALOID:
		{
			/*
			 * Original interval should not be changed, hence create a local copy
			 * for this check.
			 */
			Interval interval = *bucket_info.bf->bucket_time_width;

			/*
			 * epoch will treat year as 365.25 days. This leads to the unexpected
			 * result that year is not multiple of day or month, which is perceived
			 * as a bug. For that reason, we treat all months as 30 days regardless of year
			 */
			if (interval.month && !interval.day && !interval.time)
			{
				interval.day = interval.month * DAYS_PER_MONTH;
				interval.month = 0;
			}
			/* Convert Interval to int64 */
			width = ts_interval_value_to_internal(IntervalPGetDatum(&interval), INTERVALOID);
			break;
		}
		default:
			Assert(false);
	}

	return width;
}

ContinuousAggTimeBucketInfo
cagg_validate_query(const Query *query, const char *cagg_schema, const char *cagg_name,
					const bool is_cagg_create)
{
	ContinuousAggTimeBucketInfo bucket_info = { 0 };
	ContinuousAggTimeBucketInfo bucket_info_parent = { 0 };
	Hypertable *ht = NULL, *ht_parent = NULL;
	RangeTblEntry *ht_rte = NULL;
	StringInfoData msg;
	StringInfoData hint;
	StringInfoData detail;
	bool is_hierarchical = false;
	Query *prev_query = NULL;
	ContinuousAgg *cagg_parent = NULL;
	bool for_rewrites = false;

	initStringInfo(&msg);
	initStringInfo(&hint);
	initStringInfo(&detail);
	if (!ts_cagg_query_supported(query, &hint, &detail, for_rewrites))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid continuous aggregate query"),
				 hint.len > 0 ? errhint("%s", hint.data) : 0,
				 detail.len > 0 ? errdetail("%s", detail.data) : 0));
	}

	ListCell *lc;
	foreach (lc, query->rtable)
	{
		RangeTblEntry *inner_rte = lfirst_node(RangeTblEntry, lc);

		/* Check whether RTEs are valid in a Cagg, should have 1 hypertable among them */
		if (!ts_cagg_query_rtes_supported(inner_rte, &ht_rte, &detail, false))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view"),
					 detail.len > 0 ? errdetail("%s", detail.data) : 0));
		}
	}

	if (ht_rte == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid continuous aggregate view"),
				 errdetail("At least one hypertable should be used in the view definition.")));
	}
	else
		ht_rte = copyObject(ht_rte);

	const Dimension *part_dimension = NULL;
	int32 parent_mat_hypertable_id = INVALID_HYPERTABLE_ID;
	Cache *hcache = ts_hypertable_cache_pin();

	if (ht_rte->relkind == RELKIND_RELATION)
	{
		ht = ts_hypertable_cache_get_entry(hcache, ht_rte->relid, CACHE_FLAG_MISSING_OK);

		if (!ht)
		{
			ts_cache_release(&hcache);
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
					 errmsg("table \"%s\" is not a hypertable", get_rel_name(ht_rte->relid))));
		}
	}
	else
	{
		cagg_parent = ts_continuous_agg_find_by_relid(ht_rte->relid);

		if (!cagg_parent)
		{
			ts_cache_release(&hcache);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate query"),
					 errhint("Continuous aggregate needs to query hypertable or another "
							 "continuous aggregate.")));
		}

		parent_mat_hypertable_id = cagg_parent->data.mat_hypertable_id;
		ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg_parent->data.mat_hypertable_id);

		/* If parent cagg is hierarchical then we should get the matht otherwise the rawht. */
		if (ContinuousAggIsHierarchical(cagg_parent))
			ht_parent =
				ts_hypertable_cache_get_entry_by_id(hcache, cagg_parent->data.mat_hypertable_id);
		else
			ht_parent =
				ts_hypertable_cache_get_entry_by_id(hcache, cagg_parent->data.raw_hypertable_id);

		/* Get the querydef for the source cagg. */
		is_hierarchical = true;
		prev_query = ts_continuous_agg_get_query(cagg_parent);
	}

	/*
	 * Check if user can refresh continuous aggregate
	 * We only check for SELECT on the hypertable here but there
	 * could be other permissions needed depending on the query.
	 * For WITH DATA this is not a problem since we try a refresh
	 * immediately but for WITH NO DATA the refresh might still
	 * fail due to other permissions being needed.
	 */
	AclResult aclresult = pg_class_aclcheck(ht->main_table_relid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
	{
		/* User doesn't have permission */
		aclcheck_error(aclresult,
					   get_relkind_objtype(get_rel_relkind(ht->main_table_relid)),
					   get_rel_name(ht->main_table_relid));
	}

	part_dimension =
		ts_cagg_hypertable_dim_supported(ht_rte, ht, &msg, &detail, &hint, for_rewrites);
	if (!part_dimension)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 msg.len > 0 ? errmsg("%s", msg.data) : errmsg("Feature not supported"),
				 detail.len > 0 ? errdetail("%s", detail.data) : 0,
				 hint.len > 0 ? errhint("%s", hint.data) : 0));
	}

	ts_caggtimebucketinfo_init(&bucket_info,
							   ht->fd.id,
							   ht->main_table_relid,
							   part_dimension->column_attno,
							   part_dimension->fd.column_type,
							   part_dimension->fd.interval_length,
							   parent_mat_hypertable_id);

	if (is_hierarchical)
	{
		const Dimension *part_dimension_parent = hyperspace_get_open_dimension(ht_parent->space, 0);

		ts_caggtimebucketinfo_init(&bucket_info_parent,
								   ht_parent->fd.id,
								   ht_parent->main_table_relid,
								   part_dimension_parent->column_attno,
								   part_dimension_parent->fd.column_type,
								   part_dimension_parent->fd.interval_length,
								   INVALID_HYPERTABLE_ID);
	}

	ts_cache_release(&hcache);

	/*
	 * We need a GROUP By clause with time_bucket on the partitioning
	 * column of the hypertable
	 */
	Assert(query->groupClause);
	caggtimebucket_validate(&bucket_info,
							query->groupClause,
							query->targetList,
							query->rtable,
							is_cagg_create);

	/* Check row security settings for the table. */
	if (ts_has_row_security(ht_rte->relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create continuous aggregate on hypertable with row security")));

	/* At this point, we should have a valid bucket function. Otherwise, we have errored out before.
	 */
	Ensure(OidIsValid(bucket_info.bf->bucket_function), "unable to find valid bucket function");

	/* Ignore time_bucket_ng in this check, since offset and origin were allowed in the past */
	FuncInfo *func_info = ts_func_cache_get_bucketing_func(bucket_info.bf->bucket_function);
	Ensure(func_info != NULL, "bucket function is not found in function cache");

	/* hierarchical cagg validations */
	if (is_hierarchical)
	{
		int64 bucket_width = 0, bucket_width_parent = 0;
		bool is_greater_or_equal_than_parent = true, is_multiple_of_parent = true;

		Assert(prev_query->groupClause);
		caggtimebucket_validate(&bucket_info_parent,
								prev_query->groupClause,
								prev_query->targetList,
								prev_query->rtable,
								is_cagg_create);

		/* Cannot create cagg with fixed bucket on top of variable bucket. */
		if (ts_time_bucket_info_has_fixed_width(bucket_info_parent.bf) == false &&
			ts_time_bucket_info_has_fixed_width(bucket_info.bf) == true)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot create continuous aggregate with fixed-width bucket on top of "
							"one using variable-width bucket"),
					 errdetail("Continuous aggregate with a fixed time bucket width (e.g. 61 days) "
							   "cannot be created on top of one using variable time bucket width "
							   "(e.g. 1 month).\n"
							   "The variance can lead to the fixed width one not being a multiple "
							   "of the variable width one.")));
		}

		/* Get bucket widths for validation. */
		bucket_width = get_bucket_width(bucket_info);
		bucket_width_parent = get_bucket_width(bucket_info_parent);

		Assert(bucket_width != 0);
		Assert(bucket_width_parent != 0);

		/* Check if the current bucket is greater or equal than the parent. */
		is_greater_or_equal_than_parent = (bucket_width >= bucket_width_parent);

		/* Check if buckets are multiple. */
		if (bucket_width_parent != 0)
		{
			if (bucket_width_parent > bucket_width && bucket_width != 0)
				is_multiple_of_parent = ((bucket_width_parent % bucket_width) == 0);
			else
				is_multiple_of_parent = ((bucket_width % bucket_width_parent) == 0);
		}

		/* Proceed with validation errors. */
		if (!is_greater_or_equal_than_parent || !is_multiple_of_parent)
		{
			char *message = NULL;

			/* New bucket should be multiple of the parent. */
			if (!is_multiple_of_parent)
				message = "multiple of";

			/* New bucket should be greater than the parent. */
			if (!is_greater_or_equal_than_parent)
				message = "greater or equal than";

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot create continuous aggregate with incompatible bucket width"),
					 errdetail("Time bucket width of \"%s.%s\" [%s] should be %s the time "
							   "bucket width of \"%s.%s\" [%s].",
							   cagg_schema,
							   cagg_name,
							   ts_datum_to_string(get_bucket_width_datum(bucket_info),
												  bucket_info.bf->bucket_width_type),
							   message,
							   NameStr(cagg_parent->data.user_view_schema),
							   NameStr(cagg_parent->data.user_view_name),
							   ts_datum_to_string(get_bucket_width_datum(bucket_info_parent),
												  bucket_info_parent.bf->bucket_width_type))));
		}

		/* Test compatible time origin values */
		if (bucket_info.bf->bucket_time_origin != bucket_info_parent.bf->bucket_time_origin)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "cannot create continuous aggregate with different bucket origin values"),
					 errdetail("Time origin of \"%s.%s\" [%s] and \"%s.%s\" [%s] should be the "
							   "same.",
							   cagg_schema,
							   cagg_name,
							   ts_datum_to_string(TimestampTzGetDatum(
													  bucket_info.bf->bucket_time_origin),
												  TIMESTAMPTZOID),
							   NameStr(cagg_parent->data.user_view_schema),
							   NameStr(cagg_parent->data.user_view_name),
							   ts_datum_to_string(TimestampTzGetDatum(
													  bucket_info_parent.bf->bucket_time_origin),
												  TIMESTAMPTZOID))));
		}

		/* Test compatible time offset values */
		if (bucket_info.bf->bucket_time_offset != NULL ||
			bucket_info_parent.bf->bucket_time_offset != NULL)
		{
			bool bucket_offset_isnull = bucket_info.bf->bucket_time_offset == NULL;
			bool bucket_offset_parent_isnull = bucket_info_parent.bf->bucket_time_offset == NULL;

			Datum offset_datum = IntervalPGetDatum(bucket_info.bf->bucket_time_offset);
			Datum offset_datum_parent =
				IntervalPGetDatum(bucket_info_parent.bf->bucket_time_offset);

			bool both_buckets_are_equal = false;
			bool both_buckets_have_offset = !bucket_offset_isnull && !bucket_offset_parent_isnull;

			if (both_buckets_have_offset)
			{
				both_buckets_are_equal = DatumGetBool(
					DirectFunctionCall2(interval_eq, offset_datum, offset_datum_parent));
			}

			if (!both_buckets_are_equal)
			{
				char *offset =
					!bucket_offset_isnull ?
						DatumGetCString(DirectFunctionCall1(interval_out, offset_datum)) :
						"NULL";
				char *offset_parent =
					!bucket_offset_parent_isnull ?
						DatumGetCString(DirectFunctionCall1(interval_out, offset_datum_parent)) :
						"NULL";

				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot create continuous aggregate with different bucket offset "
								"values"),
						 errdetail("Time origin of \"%s.%s\" [%s] and \"%s.%s\" [%s] should be the "
								   "same.",
								   cagg_schema,
								   cagg_name,
								   offset,
								   NameStr(cagg_parent->data.user_view_schema),
								   NameStr(cagg_parent->data.user_view_name),
								   offset_parent)));
			}
		}

		/* Test compatible integer offset values */
		if (bucket_info.bf->bucket_integer_offset != bucket_info_parent.bf->bucket_integer_offset)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "cannot create continuous aggregate with different bucket offset values"),
					 errdetail("Integer offset of \"%s.%s\" [" INT64_FORMAT
							   "] and \"%s.%s\" [" INT64_FORMAT "] should be the same.",
							   cagg_schema,
							   cagg_name,
							   bucket_info.bf->bucket_integer_offset,
							   NameStr(cagg_parent->data.user_view_schema),
							   NameStr(cagg_parent->data.user_view_name),
							   bucket_info_parent.bf->bucket_integer_offset)));
		}
	}

	if (is_hierarchical)
		bucket_info.htoidparent = cagg_parent->relid;

	return bucket_info;
}

/*
 * Get oid of function to convert from our internal representation
 * to postgres representation.
 */
Oid
cagg_get_boundary_converter_funcoid(Oid typoid)
{
	char *function_name;
	Oid argtyp[] = { INT8OID };

	switch (typoid)
	{
		case DATEOID:
			function_name = INTERNAL_TO_DATE_FUNCTION;
			break;
		case TIMESTAMPOID:
			function_name = INTERNAL_TO_TS_FUNCTION;
			break;
		case TIMESTAMPTZOID:
			function_name = INTERNAL_TO_TSTZ_FUNCTION;
			break;
		default:
			/*
			 * This should never be reached and unsupported datatypes
			 * should be caught at much earlier stages.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no converter function defined for datatype: %s",
							format_type_be(typoid))));
			pg_unreachable();
	}

	List *func_name = list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(function_name));
	Oid converter_oid = LookupFuncName(func_name, lengthof(argtyp), argtyp, false);

	Assert(OidIsValid(converter_oid));

	return converter_oid;
}

static FuncExpr *
build_conversion_call(Oid type, FuncExpr *boundary)
{
	/*
	 * If the partitioning column type is not integer we need to convert
	 * to proper representation.
	 */
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		{
			/* Since the boundary function returns int8 we need to cast to proper type here. */
			Oid cast_oid = ts_get_cast_func(INT8OID, type);

			return makeFuncExpr(cast_oid,
								type,
								list_make1(boundary),
								InvalidOid,
								InvalidOid,
								COERCE_IMPLICIT_CAST);
		}
		case INT8OID:
			/* Nothing to do for int8. */
			return boundary;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/*
			 * date/timestamp/timestamptz need to be converted since
			 * we store them differently from postgres format.
			 */
			Oid converter_oid = cagg_get_boundary_converter_funcoid(type);
			return makeFuncExpr(converter_oid,
								type,
								list_make1(boundary),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);
		}

		default:
			/*
			 * All valid types should be handled above, this should
			 * never be reached and error handling at earlier stages
			 * should catch this.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(type))));
			pg_unreachable();
	}
}

/*
 * Return the Oid of the cagg_watermark function
 */
Oid
get_watermark_function_oid(void)
{
	Oid argtyp[] = { INT4OID };

	Oid boundary_func_oid =
		LookupFuncName(list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(BOUNDARY_FUNCTION)),
					   lengthof(argtyp),
					   argtyp,
					   false);

	return boundary_func_oid;
}

/*
 * Build function call that returns boundary for a hypertable
 * wrapped in type conversion calls when required.
 */
static FuncExpr *
build_boundary_call(int32 ht_id, Oid type)
{
	FuncExpr *boundary;
	Oid boundary_func_oid = get_watermark_function_oid();
	List *func_args =
		list_make1(makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(ht_id), false, true));

	boundary = makeFuncExpr(boundary_func_oid,
							INT8OID,
							func_args,
							InvalidOid,
							InvalidOid,
							COERCE_EXPLICIT_CALL);

	return build_conversion_call(type, boundary);
}

/*
 * Create Const of proper type for lower bound of watermark when
 * watermark has not been set yet.
 */
static Const *
cagg_boundary_make_lower_bound(Oid type)
{
	Datum value;
	int16 typlen;
	bool typbyval;

	get_typlenbyval(type, &typlen, &typbyval);
	value = ts_time_datum_get_nobegin_or_min(type);

	return makeConst(type, -1, InvalidOid, typlen, value, false, typbyval);
}

static Node *
build_union_query_quals(int32 ht_id, Oid partcoltype, Oid opno, int varno, AttrNumber attno)
{
	Var *var = makeVar(varno, attno, partcoltype, -1, InvalidOid, InvalidOid);
	FuncExpr *boundary = build_boundary_call(ht_id, partcoltype);

	CoalesceExpr *coalesce = makeNode(CoalesceExpr);
	coalesce->coalescetype = partcoltype;
	coalesce->coalescecollid = InvalidOid;
	coalesce->args = list_make2(boundary, cagg_boundary_make_lower_bound(partcoltype));

	return (Node *) make_opclause(opno,
								  BOOLOID,
								  false,
								  (Expr *) var,
								  (Expr *) coalesce,
								  InvalidOid,
								  InvalidOid);
}

static RangeTblEntry *
makeRangeTblEntry(Query *query, const char *aliasname)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	ListCell *lc;

	rte->rtekind = RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = query;
	rte->alias = makeAlias(aliasname, NIL);
	rte->eref = copyObject(rte->alias);

	foreach (lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (!tle->resjunk)
			rte->eref->colnames = lappend(rte->eref->colnames, makeString(pstrdup(tle->resname)));
	}

	rte->lateral = false;
	rte->inh = false; /* never true for subqueries */
	rte->inFromCl = false;

	return rte;
}

/*
 * Build union query combining the materialized data with data from the raw data hypertable.
 *
 * q1 is the query on the materialization hypertable with the finalize call
 * q2 is the query on the raw hypertable which was supplied in the initial CREATE VIEW statement
 * returns a query as
 * SELECT * from (  SELECT * from q1 where <coale_qual>
 *                  UNION ALL
 *                  SELECT * from q2 where existing_qual and <coale_qual>
 * where coale_qual is: time < ----> (or >= )
 *
 * COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(<htid>)),
 * '-infinity'::timestamp with time zone)
 *
 * See build_union_query_quals for COALESCE clauses.
 */
Query *
build_union_query(ContinuousAggTimeBucketInfo *tbinfo, int matpartcolno, Query *q1, Query *q2,
				  int materialize_htid)
{
	ListCell *lc1, *lc2;
	List *col_types = NIL;
	List *col_typmods = NIL;
	List *col_collations = NIL;
	List *tlist = NIL;
	List *sortClause = NIL;
	int varno;
	Node *q2_quals = NULL;

	Assert(list_length(q1->targetList) <= list_length(q2->targetList));

	q1 = copyObject(q1);
	q2 = copyObject(q2);

	if (q1->sortClause)
		sortClause = copyObject(q1->sortClause);

	TypeCacheEntry *tce = lookup_type_cache(tbinfo->htpartcoltype, TYPECACHE_LT_OPR);

	varno = list_length(q1->rtable);
	q1->jointree->quals = build_union_query_quals(materialize_htid,
												  tbinfo->htpartcoltype,
												  tce->lt_opr,
												  varno,
												  matpartcolno);
	/*
	 * If there is join in CAgg definition then adjust varno
	 * to get time column from the hypertable in the join.
	 */
	varno = list_length(q2->rtable);

	if (list_length(q2->rtable) > 1)
	{
		int nvarno = 1;
		foreach (lc2, q2->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc2);
			if (rte->rtekind == RTE_RELATION)
			{
				/* look for hypertable or parent hypertable in RangeTableEntry list */
				if (rte->relid == tbinfo->htoid || rte->relid == tbinfo->htoidparent)
				{
					varno = nvarno;
					break;
				}
			}
			nvarno++;
		}
	}

	q2_quals = build_union_query_quals(materialize_htid,
									   tbinfo->htpartcoltype,
									   get_negator(tce->lt_opr),
									   varno,
									   tbinfo->htpartcolno);

	q2->jointree->quals = make_and_qual(q2->jointree->quals, q2_quals);

	Query *query = makeNode(Query);
	SetOperationStmt *setop = makeNode(SetOperationStmt);
	RangeTblEntry *rte_q1 = makeRangeTblEntry(q1, "*SELECT* 1");
	RangeTblEntry *rte_q2 = makeRangeTblEntry(q2, "*SELECT* 2");
	RangeTblRef *ref_q1 = makeNode(RangeTblRef);
	RangeTblRef *ref_q2 = makeNode(RangeTblRef);

	query->commandType = CMD_SELECT;
	query->rtable = list_make2(rte_q1, rte_q2);
	query->setOperations = (Node *) setop;

	setop->op = SETOP_UNION;
	setop->all = true;
	ref_q1->rtindex = 1;
	ref_q2->rtindex = 2;
	setop->larg = (Node *) ref_q1;
	setop->rarg = (Node *) ref_q2;

	forboth (lc1, q1->targetList, lc2, q2->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc1);
		TargetEntry *tle2 = lfirst_node(TargetEntry, lc2);
		TargetEntry *tle_union;
		Var *expr;

		if (!tle->resjunk)
		{
			col_types = lappend_int(col_types, exprType((Node *) tle->expr));
			col_typmods = lappend_int(col_typmods, exprTypmod((Node *) tle->expr));
			col_collations = lappend_int(col_collations, exprCollation((Node *) tle->expr));

			expr = makeVarFromTargetEntry(1, tle);
			/*
			 * We need to use resname from q2 because that is the query from the
			 * initial CREATE VIEW statement so the VIEW can be updated in place.
			 */
			tle_union = makeTargetEntry((Expr *) copyObject(expr),
										list_length(tlist) + 1,
										tle2->resname,
										false);
			tle_union->resorigtbl = expr->varno;
			tle_union->resorigcol = expr->varattno;
			tle_union->ressortgroupref = tle->ressortgroupref;

			tlist = lappend(tlist, tle_union);
		}
	}

	query->targetList = tlist;
	query->jointree = makeFromExpr(NIL, NULL);

	if (sortClause)
	{
		query->sortClause = sortClause;
	}

	setop->colTypes = col_types;
	setop->colTypmods = col_typmods;
	setop->colCollations = col_collations;

	return query;
}

ContinuousAgg *
cagg_get_by_relid_or_fail(const Oid cagg_relid)
{
	ContinuousAgg *cagg;

	if (!OidIsValid(cagg_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid continuous aggregate")));

	cagg = ts_continuous_agg_find_by_relid(cagg_relid);

	if (NULL == cagg)
	{
		const char *relname = get_rel_name(cagg_relid);

		if (relname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 (errmsg("continuous aggregate does not exist"))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("relation \"%s\" is not a continuous aggregate", relname))));
	}

	return cagg;
}

/*
 * This function is responsible to return a list of column names used in
 * GROUP BY clause of the cagg query. It behaves a bit different depending
 * of the type of the Continuous Aggregate.
 *
 * Retrieve the "direct view query" and find the GROUP BY clause and
 * "time_bucket" clause. We use the "direct view query" because in the
 * "user view query" we removed the re-aggregation in the part that query
 * the materialization hypertable so we don't have a GROUP BY clause
 * anymore.
 *
 * Get the column name from the GROUP BY clause because all the column
 * names are the same in all underlying objects (user view, direct view,
 * partial view and materialization hypertable).
 */
List *
cagg_find_groupingcols(ContinuousAgg *agg, Hypertable *mat_ht)
{
	List *retlist = NIL;
	ListCell *lc;
	Query *cagg_view_query = ts_continuous_agg_get_query(agg);
	Oid mat_relid = mat_ht->main_table_relid;
	Query *finalize_query;

#if PG16_LT
	/* The view rule has dummy old and new range table entries as the 1st and 2nd entries */
	Assert(list_length(cagg_view_query->rtable) >= 2);
#endif

	if (cagg_view_query->setOperations)
	{
		/*
		 * This corresponds to the union view.
		 *   PG16_LT the 3rd RTE entry has the SELECT 1 query from the union view.
		 *   PG16_GE the 1st RTE entry has the SELECT 1 query from the union view
		 */
#if PG16_LT
		RangeTblEntry *finalize_query_rte = lthird(cagg_view_query->rtable);
#else
		RangeTblEntry *finalize_query_rte = linitial(cagg_view_query->rtable);
#endif
		if (finalize_query_rte->rtekind != RTE_SUBQUERY)
			ereport(ERROR,
					(errcode(ERRCODE_TS_UNEXPECTED),
					 errmsg("unexpected rte type for view %d", finalize_query_rte->rtekind)));

		finalize_query = finalize_query_rte->subquery;
	}
	else
	{
		finalize_query = cagg_view_query;
	}

	foreach (lc, finalize_query->groupClause)
	{
		SortGroupClause *cagg_gc = (SortGroupClause *) lfirst(lc);
		TargetEntry *cagg_tle = get_sortgroupclause_tle(cagg_gc, finalize_query->targetList);

		/* "resname" is the same as "mat column names" */
		if (!cagg_tle->resjunk && cagg_tle->resname)
			retlist = lappend(retlist, get_attname(mat_relid, cagg_tle->resno, false));
	}
	return retlist;
}
