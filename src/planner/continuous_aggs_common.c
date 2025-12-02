/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "planner/continuous_aggs_common.h"

#include <utils/acl.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "guc.h"

/*
 * Check query for Cagg support, extract error details and error hints.
 *
 * Returns:
 *   True if the query is supported
 *   (either for Cagg view or for query rewrite with Cagg, see for_rewrites),
 *   false otherwise with hints and errors added
 *   if hint and error string buffers are provided.
 */
bool
ts_cagg_query_supported(const Query *query, StringInfo hint, StringInfo detail,
						const bool finalized, const bool for_rewrites)
{
	if (!finalized)
	{
		/* continuous aggregates with old format will not be allowed */
		appendStringInfoString(detail,
							   "Continuous Aggregates with partials is not supported anymore.");
		appendStringInfoString(hint,
							   "Define the Continuous Aggregate with \"finalized\" parameter set "
							   "to true.");
		return false;
	}
	if (!query->jointree->fromlist)
	{
		if (!for_rewrites)
			appendStringInfoString(hint, "FROM clause missing in the query");
		else if (hint)
			appendStringInfoString(hint, "FROM clause missing in the query");
		return false;
	}
	if (query->commandType != CMD_SELECT)
	{
		if (!for_rewrites)
			appendStringInfoString(hint, "Use a SELECT query in the continuous aggregate view.");
		else if (hint)
			appendStringInfoString(hint, "not a SELECT query");
		return false;
	}

	if (query->hasWindowFuncs)
	{
		if (ts_guc_enable_cagg_window_functions)
		{
			if (!for_rewrites)
				elog(WARNING,
					 "window function support is experimental and may result in unexpected results "
					 "depending on the functions used.");
		}
		else
		{
			if (!for_rewrites)
			{
				appendStringInfoString(detail, "Window function support not enabled.");
				appendStringInfoString(hint,
									   "Enable experimental window function support by setting "
									   "timescaledb.enable_cagg_window_functions.");
			}
			else if (hint)
				appendStringInfoString(hint,
									   "window function support not enabled for continuous "
									   "aggregates");
			return false;
		}
	}

	if (query->hasDistinctOn || query->distinctClause)
	{
		if (!for_rewrites)
			appendStringInfoString(detail,
								   "DISTINCT / DISTINCT ON queries are not supported by continuous "
								   "aggregates.");
		else if (hint)
			appendStringInfoString(hint,
								   "DISTINCT / DISTINCT ON queries are not supported by continuous "
								   "aggregates.");
		return false;
	}

	/* Can apply LIMIT to queries rewritten with Caggs */
	if (!for_rewrites && (query->limitOffset || query->limitCount))
	{
		appendStringInfoString(detail,
							   "LIMIT and LIMIT OFFSET are not supported in queries defining "
							   "continuous aggregates.");
		appendStringInfoString(hint,
							   "Use LIMIT and LIMIT OFFSET in SELECTS from the continuous "
							   "aggregate view instead.");
		return false;
	}

	if (query->hasRecursive || query->hasSubLinks || query->cteList)
	{
		if (!for_rewrites)
			appendStringInfoString(detail,
								   "CTEs and subqueries are not supported by "
								   "continuous aggregates.");
		else if (hint)
			appendStringInfoString(hint,
								   "CTEs and subqueries are not supported by "
								   "continuous aggregates.");
		return false;
	}

	if (query->hasForUpdate || query->hasModifyingCTE)
	{
		if (!for_rewrites)
			appendStringInfoString(detail,
								   "Data modification is not allowed in continuous aggregate view "
								   "definitions.");
		else if (hint)
			appendStringInfoString(hint,
								   "Data modification is not allowed in continuous aggregates");
		return false;
	}

	if (query->hasRowSecurity)
	{
		if (!for_rewrites)
			appendStringInfoString(detail,
								   "Row level security is not supported by continuous aggregate "
								   "views.");
		else if (hint)
			appendStringInfoString(hint,
								   "Row level security is not supported by continuous aggregates");
		return false;
	}

	if (query->groupingSets)
	{
		if (!for_rewrites)
		{
			appendStringInfoString(detail,
								   "GROUP BY GROUPING SETS, ROLLUP and CUBE are not supported by "
								   "continuous aggregates");
			appendStringInfoString(hint,
								   "Define multiple continuous aggregates with different grouping "
								   "levels.");
		}
		else if (hint)
			appendStringInfoString(hint,
								   "GROUP BY GROUPING SETS, ROLLUP and CUBE are not supported by "
								   "continuous aggregates");
		return false;
	}

	if (query->setOperations)
	{
		if (!for_rewrites)
			appendStringInfoString(detail,
								   "UNION, EXCEPT & INTERSECT are not supported by continuous "
								   "aggregates");
		else if (hint)
			appendStringInfoString(hint,
								   "UNION, EXCEPT & INTERSECT are not supported by continuous "
								   "aggregates");
		return false;
	}

	if (!query->groupClause)
	{
		/*
		 * Query can have aggregate without group by, so look
		 * for groupClause.
		 */
		if (!for_rewrites)
			appendStringInfoString(hint,
								   "Include at least one aggregate function"
								   " and a GROUP BY clause with time bucket.");
		else if (hint)
			appendStringInfoString(hint, "no GROUP BY clause in the query");
		return false;
	}

	return true; /* Query was OK and is supported. */
}

bool
ts_cagg_query_rtes_supported(RangeTblEntry *rte, RangeTblEntry **ht_rte, StringInfo detail,
							 const bool for_rewrites)
{
	if (rte->rtekind == RTE_RELATION
		/* Allow for processed views in Caggs used in a query */
		|| (for_rewrites && rte->relkind == RELKIND_VIEW))
	{
		bool is_hypertable =
			ts_is_hypertable(rte->relid) || ts_continuous_agg_find_by_relid(rte->relid);

		if (is_hypertable && !(*ht_rte))
		{
			*ht_rte = rte;
		}
		else if (is_hypertable && (*ht_rte))
		{
			if (!for_rewrites)
				appendStringInfoString(detail,
									   "Only one hypertable is allowed in continuous aggregate "
									   "view.");
			else if (detail)
				appendStringInfo(detail,
								 "More than one hypertable in the query: \"%s\" and \"%s\"",
								 rte->eref->aliasname,
								 (*ht_rte)->eref->aliasname);
			return false;
		}

		if (is_hypertable && rte->inh == false && !(for_rewrites && rte->relkind == RELKIND_VIEW))
		{
			if (!for_rewrites)
				appendStringInfoString(detail,
									   "FROM ONLY on hypertables is not allowed in continuous "
									   "aggregate.");
			else if (detail)
				appendStringInfo(detail,
								 "FROM ONLY on hypertable \"%s\" is not allowed in continuous "
								 "aggregate.",
								 rte->eref->aliasname);
			return false;
		}
	}
	/* Only inner joins are allowed. */
	if (rte->jointype != JOIN_INNER && rte->jointype != JOIN_LEFT)
	{
		if (detail)
			appendStringInfoString(detail,
								   "only INNER or LEFT joins are supported in continuous "
								   "aggregates");
		return false;
	}

	/* Subquery only using LATERAL */
	if (rte->subquery && !rte->lateral && !(for_rewrites && rte->relkind == RELKIND_VIEW))
	{
		if (!for_rewrites)
			appendStringInfoString(detail, "Sub-queries are not supported in FROM clause.");
		else if (detail)
			appendStringInfoString(detail,
								   "only LATERAL subqueries in FROM clause are supported in "
								   "continuous aggregates.");
		return false;
	}

	/* TABLESAMPLE not allowed */
	if (rte->tablesample)
	{
		if (detail)
			appendStringInfoString(detail, "TABLESAMPLE is not supported in continuous aggregate.");
		return false;
	}
	return true;
}

const Dimension *
ts_cagg_hypertable_dim_supported(RangeTblEntry *ht_rte, Hypertable *ht, StringInfo msg,
								 StringInfo detail, StringInfo hint, const bool for_rewrites)
{
	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		if (!for_rewrites)
			appendStringInfoString(msg, "hypertable is an internal compressed hypertable");
		else if (msg)
			appendStringInfo(msg,
							 "hypertable \"%s.%s\" is an internal compressed hypertable",
							 NameStr(ht->fd.schema_name),
							 NameStr(ht->fd.table_name));
		return NULL;
	}

	if (ht_rte->relkind == RELKIND_RELATION)
	{
		ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);

		/* Prevent create a CAGG over an existing materialization hypertable. */
		if (status == HypertableIsMaterialization || status == HypertableIsMaterializationAndRaw)
		{
			const ContinuousAgg *cagg =
				ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id, false);
			Assert(cagg != NULL);
			if (!for_rewrites)
			{
				appendStringInfoString(msg,
									   "hypertable is a continuous aggregate materialization "
									   "table");
				appendStringInfo(detail,
								 "Materialization hypertable \"%s.%s\".",
								 NameStr(ht->fd.schema_name),
								 NameStr(ht->fd.table_name));
				appendStringInfo(hint,
								 "Do you want to use continuous aggregate \"%s.%s\" instead?",
								 NameStr(cagg->data.user_view_schema),
								 NameStr(cagg->data.user_view_name));
			}
			else if (msg)
			{
				appendStringInfo(msg,
								 "hypertable \"%s.%s\" is a continuous aggregate materialization "
								 "table",
								 NameStr(ht->fd.schema_name),
								 NameStr(ht->fd.table_name));
			}
			return NULL;
		}
	}

	/* Get primary partitioning column information. */
	const Dimension *part_dimension = hyperspace_get_open_dimension(ht->space, 0);
	/*
	 * NOTE: if we ever allow custom partitioning functions we'll need to
	 *       change part_dimension->fd.column_type to partitioning_type
	 *       below, along with any other fallout.
	 */
	if (part_dimension == NULL || part_dimension->partitioning != NULL)
	{
		if (msg)
			appendStringInfoString(msg,
								   "custom partitioning functions not supported with continuous "
								   "aggregates");
		return NULL;
	}

	if (IS_INTEGER_TYPE(ts_dimension_get_partition_type(part_dimension)) &&
		ht_rte->relkind == RELKIND_RELATION)
	{
		const char *funcschema = NameStr(part_dimension->fd.integer_now_func_schema);
		const char *funcname = NameStr(part_dimension->fd.integer_now_func);

		if (strlen(funcschema) == 0 || strlen(funcname) == 0)
		{
			if (!for_rewrites)
			{
				appendStringInfo(msg,
								 "custom time function required on hypertable \"%s\"",
								 get_rel_name(ht->main_table_relid));
				appendStringInfoString(detail,
									   "An integer-based hypertable requires a custom time "
									   "function to support continuous aggregates.");
				appendStringInfoString(hint, "Set a custom time function on the hypertable.");
			}
			else if (msg)
				appendStringInfo(msg,
								 "custom time function required on integer-based hypertable \"%s\"",
								 get_rel_name(ht->main_table_relid));
			return NULL;
		}
	}
	return part_dimension;
}

/*
 * Returns true if the time bucket size is fixed
 */
bool
ts_time_bucket_info_has_fixed_width(const ContinuousAggBucketFunction *bf)
{
	if (!IS_TIME_BUCKET_INFO_TIME_BASED(bf))
	{
		return true;
	}
	else
	{
		/* Historically, we treat all buckets with timezones as variable. Buckets with only days are
		 * treated as fixed. */
		return bf->bucket_time_width->month == 0 && bf->bucket_time_timezone == NULL;
	}
}

/*
 * Check if the supplied OID belongs to a valid bucket function
 * for continuous aggregates.
 */
bool
ts_function_allowed_in_cagg_definition(Oid funcid)
{
	FuncInfo *finfo = ts_func_cache_get_bucketing_func(funcid);
	if (finfo == NULL)
		return false;

	if (finfo->allowed_in_cagg_definition)
		return true;

	/* Allow creation of CAggs with deprecated bucket function in debug builds for testing purposes
	 */
	if (ts_guc_debug_allow_cagg_with_deprecated_funcs && IS_DEPRECATED_TIME_BUCKET_NG_FUNC(finfo))
		return true;

	return false;
}

bool
caggtimebucket_equal(ContinuousAggBucketFunction *bf1, ContinuousAggBucketFunction *bf2)
{
	if (bf1->bucket_fixed_interval != bf2->bucket_fixed_interval)
		return false;

	if (bf1->bucket_time_based != bf2->bucket_time_based)
		return false;

	if (bf1->bucket_time_based)
	{
		if (bf1->bucket_time_origin != bf2->bucket_time_origin)
			return false;
		if (bf1->bucket_time_offset != bf2->bucket_time_offset)
			return false;
		if (bf1->bucket_time_timezone != bf2->bucket_time_timezone)
			return false;
	}

	int64 bucket_width1 = ts_continuous_agg_bucket_width(bf1);
	int64 bucket_width2 = ts_continuous_agg_bucket_width(bf2);

	if (bucket_width1 != bucket_width2)
		return false;

	return true;
}

/*
 * Initialize caggtimebucket.
 */
void
ts_caggtimebucketinfo_init(ContinuousAggTimeBucketInfo *src, int32 hypertable_id,
						   Oid hypertable_oid, AttrNumber hypertable_partition_colno,
						   Oid hypertable_partition_coltype,
						   int64 hypertable_partition_col_interval, int32 parent_mat_hypertable_id)
{
	src->htid = hypertable_id;
	src->parent_mat_hypertable_id = parent_mat_hypertable_id;
	src->htoid = hypertable_oid;
	src->htoidparent = InvalidOid;
	src->htpartcolno = hypertable_partition_colno;
	src->htpartcoltype = hypertable_partition_coltype;
	src->htpartcol_interval_len = hypertable_partition_col_interval;

	/* Initialize bucket function data structure */
	src->bf = palloc0(sizeof(ContinuousAggBucketFunction));
	src->bf->bucket_function = InvalidOid;
	src->bf->bucket_width_type = InvalidOid;

	/* Time based buckets */
	src->bf->bucket_time_width = NULL;				/* not specified by default */
	src->bf->bucket_time_timezone = NULL;			/* not specified by default */
	src->bf->bucket_time_offset = NULL;				/* not specified by default */
	TIMESTAMP_NOBEGIN(src->bf->bucket_time_origin); /* origin is not specified by default */

	/* Integer based buckets */
	src->bf->bucket_integer_width = 0;	/* invalid value */
	src->bf->bucket_integer_offset = 0; /* invalid value */
}

static Const *
check_time_bucket_argument(Node *arg, char *position, bool process_checks, StringInfo msg,
						   bool for_rewrites)
{
	if (IsA(arg, NamedArgExpr))
		arg = (Node *) castNode(NamedArgExpr, arg)->arg;

	Node *expr = eval_const_expressions(NULL, arg);

	if (process_checks && !IsA(expr, Const))
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only immutable expressions allowed in time bucket function"),
					 errhint("Use an immutable expression as %s argument to the time bucket "
							 "function.",
							 position)));
		else if (msg)
			appendStringInfo(msg,
							 "non-immutable expression as %s argument to the time bucket function",
							 position);
		return NULL;
	}

	return castNode(Const, expr);
}

/*
 * Handle additional parameter of the timebucket function such as timezone, offset, or origin
 */
static bool
process_additional_timebucket_parameter(ContinuousAggBucketFunction *bf, Const *arg,
										bool *custom_origin, bool process_checks)
{
	char *tz_name;
	switch (exprType((Node *) arg))
	{
		/* Timezone as text */
		case TEXTOID:
			tz_name = TextDatumGetCString(arg->constvalue);
			if (!ts_is_valid_timezone_name(tz_name))
			{
				if (process_checks)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid timezone name \"%s\"", tz_name)));
				return false;
			}

			bf->bucket_time_timezone = tz_name;
			break;
		case INTERVALOID:
			/* Bucket offset as interval */
			bf->bucket_time_offset = DatumGetIntervalP(arg->constvalue);
			break;
		case DATEOID:
			/* Bucket origin as Date */
			if (!arg->constisnull)
				bf->bucket_time_origin =
					date2timestamptz_opt_overflow(DatumGetDateADT(arg->constvalue), NULL);
			*custom_origin = true;
			break;
		case TIMESTAMPOID:
			/* Bucket origin as Timestamp */
			bf->bucket_time_origin = DatumGetTimestamp(arg->constvalue);
			*custom_origin = true;
			break;
		case TIMESTAMPTZOID:
			/* Bucket origin as TimestampTZ */
			bf->bucket_time_origin = DatumGetTimestampTz(arg->constvalue);
			*custom_origin = true;
			break;
		case INT2OID:
			/* Bucket offset as smallint */
			bf->bucket_integer_offset = DatumGetInt16(arg->constvalue);
			break;
		case INT4OID:
			/* Bucket offset as int */
			bf->bucket_integer_offset = DatumGetInt32(arg->constvalue);
			break;
		case INT8OID:
			/* Bucket offset as bigint */
			bf->bucket_integer_offset = DatumGetInt64(arg->constvalue);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unable to handle time_bucket parameter of type: %s",
							format_type_be(exprType((Node *) arg)))));
			pg_unreachable();
	}
	return true;
}

/*
 * Process the FuncExpr node to fill the bucket function data structure. The other
 * parameters are used when `process_check` is true that means we need to raise errors
 * when invalid parameters are passed to the time bucket function when creating a cagg.
 */
static void
process_timebucket_parameters(FuncExpr *fe, ContinuousAggBucketFunction *bf, bool process_checks,
							  bool is_cagg_create, AttrNumber htpartcolno, StringInfo msg,
							  bool for_rewrites)
{
	Node *width_arg;
	Node *col_arg;
	bool custom_origin = false;
	TIMESTAMP_NOBEGIN(bf->bucket_time_origin);
	int nargs;

	/* Only column allowed : time_bucket('1day', <column> ) */
	col_arg = lsecond(fe->args);

	/* Could be a named argument */
	if (IsA(col_arg, NamedArgExpr))
		col_arg = (Node *) castNode(NamedArgExpr, col_arg)->arg;

	if (process_checks && htpartcolno != InvalidAttrNumber &&
		(!(IsA(col_arg, Var)) || castNode(Var, col_arg)->varattno != htpartcolno))
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("time bucket function must reference the primary hypertable "
							"dimension column")));
		else if (msg)
			appendStringInfoString(msg,
								   "time bucket function must reference the primary hypertable "
								   "dimension column");
		return;
	}

	nargs = list_length(fe->args);
	Assert(nargs >= 2 && nargs <= 5);

	/*
	 * Process the third argument of the time bucket function. This could be `timezone`, `offset`,
	 * or `origin`.
	 *
	 * Time bucket function variations with 3 and 5 arguments:
	 *   - time_bucket(width SMALLINT, ts SMALLINT,    offset SMALLINT)
	 *   - time_bucket(width INTEGER,  ts INTEGER,     offset INTEGER)
	 *   - time_bucket(width BIGINT,   ts BIGINT,      offset BIGINT)
	 *   - time_bucket(width INTERVAL, ts DATE,        offset INTERVAL)
	 *   - time_bucket(width INTERVAL, ts DATE,        origin DATE)
	 *   - time_bucket(width INTERVAL, ts TIMESTAMPTZ, offset INTERVAL)
	 *   - time_bucket(width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ)
	 *   - time_bucket(width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, origin TIMESTAMPTZ,
	 *                 offset INTERVAL)
	 *   - time_bucket(width INTERVAL, ts TIMESTAMP,   offset INTERVAL)
	 *   - time_bucket(width INTERVAL, ts TIMESTAMP,   origin TIMESTAMP)
	 */
	if (nargs >= 3)
	{
		Const *arg = check_time_bucket_argument(lthird(fe->args),
												"third",
												process_checks,
												msg,
												for_rewrites);
		if (!arg)
			return;
		if (!process_additional_timebucket_parameter(bf, arg, &custom_origin, process_checks))
			return;
	}

	/*
	 * Process the fourth and fifth arguments of the time bucket function. This could be `origin` or
	 * `offset`.
	 *
	 * Time bucket function variation with 5 arguments:
	 *   - time_bucket(width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, origin TIMESTAMPTZ,
	 *                 offset INTERVAL)
	 */
	if (nargs >= 4)
	{
		Const *arg = check_time_bucket_argument(lfourth(fe->args),
												"fourth",
												process_checks,
												msg,
												for_rewrites);
		if (!arg)
			return;
		if (!process_additional_timebucket_parameter(bf, arg, &custom_origin, process_checks))
			return;
	}

	if (nargs == 5)
	{
		Const *arg = check_time_bucket_argument(lfifth(fe->args),
												"fifth",
												process_checks,
												msg,
												for_rewrites);
		if (!arg)
			return;
		if (!process_additional_timebucket_parameter(bf, arg, &custom_origin, process_checks))
			return;
	}

	if (process_checks && custom_origin && TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid origin value: infinity")));
		else if (msg)
			appendStringInfoString(msg, "invalid time bucket origin value: infinity");
		return;
	}

	/*
	 * We constify width expression here so any immutable expression will be allowed.
	 * Otherwise it would make it harder to create caggs for hypertables with e.g. int8
	 * partitioning column as int constants default to int4 and so expression would
	 * have a cast and not be a Const.
	 */
	width_arg = linitial(fe->args);

	if (IsA(width_arg, NamedArgExpr))
		width_arg = (Node *) castNode(NamedArgExpr, width_arg)->arg;

	width_arg = eval_const_expressions(NULL, width_arg);
	if (IsA(width_arg, Const))
	{
		Const *width = castNode(Const, width_arg);
		bf->bucket_width_type = width->consttype;

		if (width->constisnull)
		{
			if (process_checks && is_cagg_create)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid bucket width for time bucket function")));
		}
		else
		{
			if (width->consttype == INTERVALOID)
			{
				bf->bucket_time_width = DatumGetIntervalP(width->constvalue);
			}

			if (!IS_TIME_BUCKET_INFO_TIME_BASED(bf))
			{
				bf->bucket_integer_width =
					ts_interval_value_to_internal(width->constvalue, width->consttype);
			}
		}
	}
	else if (process_checks)
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only immutable expressions allowed in time bucket function"),
					 errhint("Use an immutable expression as first argument to the time bucket "
							 "function.")));
		else if (msg)
			appendStringInfoString(msg,
								   "non-immutable expression as first argument to the time bucket "
								   "function");
		return;
	}

	bf->bucket_function = fe->funcid;
	bf->bucket_time_based = ts_continuous_agg_bucket_on_interval(bf->bucket_function);
	bf->bucket_fixed_interval = ts_time_bucket_info_has_fixed_width(bf);
}

bool
ts_caggtimebucket_validate(ContinuousAggBucketFunction *bf, List *groupClause, List *targetList,
						   List *rtable, int ht_partcolno, StringInfo msg, bool is_cagg_create,
						   const bool for_rewrites)
{
	ListCell *l;
	bool found = false;

	/* Make sure tbinfo was initialized. This assumption is used below. */
	Assert(bf->bucket_integer_width == 0);
	Assert(bf->bucket_time_timezone == NULL);
	Assert(TIMESTAMP_NOT_FINITE(bf->bucket_time_origin));

	List *group_exprs = get_sortgrouplist_exprs(groupClause, targetList);

#if PG18_GE
	/* PG18 introduced RTEs for group clauses so
	 * we can just use rtable to look for GROUP BY expressions.
	 *
	 * https://github.com/postgres/postgres/commit/247dea89
	 */
	List *group_rte_exprs = NIL;
	foreach (l, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		if (rte->rtekind == RTE_GROUP)
			group_rte_exprs = list_concat(group_rte_exprs, rte->groupexprs);
	}

	group_exprs = group_rte_exprs;
#endif

	foreach (l, group_exprs)
	{
		Expr *expr = (Expr *) lfirst(l);

		if (IsA(expr, FuncExpr))
		{
			FuncExpr *fe = castNode(FuncExpr, expr);

			/* Filter any non bucketing functions */
			FuncInfo *finfo = ts_func_cache_get_bucketing_func(fe->funcid);
			if (finfo == NULL || !finfo->is_bucketing_func)
			{
				continue;
			}

			/* Do we have a bucketing function that is not allowed in the CAgg definition?
			 *
			 * This is only validated upon creation. If an older TSDB version has allowed us to use
			 * the function and it's now removed from the list of allowed functions, we should not
			 * error out (e.g., materialized_only setting is changed on a CAgg that uses the
			 * deprecated time_bucket_ng function). */
			if (!ts_function_allowed_in_cagg_definition(fe->funcid))
			{
				if (IS_DEPRECATED_TIME_BUCKET_NG_FUNC(finfo))
				{
					if (is_cagg_create)
					{
						if (!for_rewrites)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg(
										 "experimental bucket functions are not supported inside a "
										 "CAgg "
										 "definition"),
									 errhint("Use a function from the %s schema instead.",
											 FUNCTIONS_SCHEMA_NAME)));
						else if (msg)
							appendStringInfoString(msg,
												   "experimental bucket functions are not "
												   "supported with CAggs");
						return false;
					}
				}
				else
				{
					continue;
				}
			}

			if (found)
			{
				if (!for_rewrites)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("continuous aggregate view cannot contain"
									" multiple time bucket functions")));
				else if (msg)
					appendStringInfoString(msg,
										   "multiple time bucket functions are not supported with "
										   "CAggs");
				return false;
			}
			else
				found = true;

			process_timebucket_parameters(fe,
										  bf,
										  true,
										  is_cagg_create,
										  ht_partcolno,
										  msg,
										  for_rewrites);
			if (!OidIsValid(bf->bucket_function))
				return false;
		}
	}

	if (bf->bucket_time_offset != NULL && TIMESTAMP_NOT_FINITE(bf->bucket_time_origin) == false)
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("using offset and origin in a time_bucket function at the same time is "
							"not "
							"supported")));
		else if (msg)
			appendStringInfoString(msg,
								   "using offset and origin in a time_bucket function at the same "
								   "time is not supported");
		return false;
	}

	if (!ts_time_bucket_info_has_fixed_width(bf))
	{
		/* Variable-sized buckets can be used only with intervals. */
		Assert(bf->bucket_time_width != NULL);
		Assert(IS_TIME_BUCKET_INFO_TIME_BASED(bf));

		if ((bf->bucket_time_width->month != 0) &&
			((bf->bucket_time_width->day != 0) || (bf->bucket_time_width->time != 0)))
		{
			if (!for_rewrites)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid interval specified"),
						 errhint(
							 "Use either months or days and hours, but not months, days and hours "
							 "together")));
			else if (msg)
				appendStringInfoString(msg,
									   "invalid time bucket interval of months, days, hours "
									   "together");

			return false;
		}
	}

	if (!found)
	{
		if (!for_rewrites)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "continuous aggregate view must include a valid time bucket function")));
		else if (msg)
			appendStringInfoString(msg, "should be a valid time bucket function in the query");

		return false;
	}
	return true;
}

/* Get time bucket function info based on the view definition */
ContinuousAggBucketFunction *
ts_cagg_get_bucket_function_info(Oid view_oid)
{
	Relation view_rel = relation_open(view_oid, AccessShareLock);
	Query *query = copyObject(get_view_query(view_rel));
	relation_close(view_rel, NoLock);

	Assert(query != NULL);
	Assert(query->commandType == CMD_SELECT);

	ContinuousAggBucketFunction *bf = palloc0(sizeof(ContinuousAggBucketFunction));

	ListCell *l;
	foreach (l, query->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, l);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		Expr *expr = tle->expr;
#if PG18_GE
		/* PG18 introduced RTEs for group clauses so
		 * we can use rtable to look up GROUP BY expressions.
		 *
		 * https://github.com/postgres/postgres/commit/247dea89
		 */
		if (IsA(expr, Var))
		{
			Var *var = castNode(Var, tle->expr);
			Assert((int) var->varno <= list_length(query->rtable));
			RangeTblEntry *rte = list_nth(query->rtable, var->varno - 1);
			Assert(rte->rtekind == RTE_GROUP);
			Assert(var->varattno > 0);
			Expr *node = list_nth(rte->groupexprs, var->varattno - 1);
			if (IsA(node, FuncExpr))
				expr = node;
		}
#endif

		if (IsA(expr, FuncExpr))
		{
			FuncExpr *fe = castNode(FuncExpr, expr);

			/* Filter any non bucketing functions */
			FuncInfo *finfo = ts_func_cache_get_bucketing_func(fe->funcid);
			if (finfo == NULL)
				continue;

			Assert(finfo->is_bucketing_func);

			process_timebucket_parameters(fe, bf, false, false, InvalidAttrNumber, NULL, false);
			break;
		}
	}

	return bf;
}
