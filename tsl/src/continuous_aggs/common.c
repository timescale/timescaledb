/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "common.h"

static Const *check_time_bucket_argument(Node *arg, char *position);
static void caggtimebucketinfo_init(CAggTimebucketInfo *src, int32 hypertable_id,
									Oid hypertable_oid, AttrNumber hypertable_partition_colno,
									Oid hypertable_partition_coltype,
									int64 hypertable_partition_col_interval,
									int32 parent_mat_hypertable_id);
static void caggtimebucket_validate(CAggTimebucketInfo *tbinfo, List *groupClause,
									List *targetList);
static bool cagg_agg_validate(Node *node, void *context);
static bool cagg_query_supported(const Query *query, StringInfo hint, StringInfo detail,
								 const bool finalized);
static Oid cagg_get_boundary_converter_funcoid(Oid typoid);
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

static Const *
check_time_bucket_argument(Node *arg, char *position)
{
	if (IsA(arg, NamedArgExpr))
		arg = (Node *) castNode(NamedArgExpr, arg)->arg;

	Node *expr = eval_const_expressions(NULL, arg);

	if (!IsA(expr, Const))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only immutable expressions allowed in time bucket function"),
				 errhint("Use an immutable expression as %s argument to the time bucket function.",
						 position)));

	return castNode(Const, expr);
}

/*
 * Initialize caggtimebucket.
 */
static void
caggtimebucketinfo_init(CAggTimebucketInfo *src, int32 hypertable_id, Oid hypertable_oid,
						AttrNumber hypertable_partition_colno, Oid hypertable_partition_coltype,
						int64 hypertable_partition_col_interval, int32 parent_mat_hypertable_id)
{
	src->htid = hypertable_id;
	src->parent_mat_hypertable_id = parent_mat_hypertable_id;
	src->htoid = hypertable_oid;
	src->htpartcolno = hypertable_partition_colno;
	src->htpartcoltype = hypertable_partition_coltype;
	src->htpartcol_interval_len = hypertable_partition_col_interval;
	src->bucket_width = 0;				 /* invalid value */
	src->bucket_width_type = InvalidOid; /* invalid oid */
	src->interval = NULL;				 /* not specified by default */
	src->timezone = NULL;				 /* not specified by default */
	TIMESTAMP_NOBEGIN(src->origin);		 /* origin is not specified by default */
}

/*
 * Check if the supplied OID belongs to a valid bucket function
 * for continuous aggregates.
 */
bool
function_allowed_in_cagg_definition(Oid funcid)
{
	FuncInfo *finfo = ts_func_cache_get_bucketing_func(funcid);
	if (finfo == NULL)
		return false;

	return finfo->allowed_in_cagg_definition;
}

/*
 * Return Oid for a schema-qualified relation.
 */
Oid
relation_oid(Name schema, Name name)
{
	return get_relname_relid(NameStr(*name), get_namespace_oid(NameStr(*schema), false));
}

/*
 * When a view is created (StoreViewQuery), 2 dummy rtable entries corresponding to "old" and
 * "new" are prepended to the rtable list. We remove these and adjust the varnos to recreate
 * the user or direct view query.
 */
void
RemoveRangeTableEntries(Query *query)
{
	List *rtable = query->rtable;
	Assert(list_length(rtable) >= 3);
	rtable = list_delete_first(rtable);
	query->rtable = list_delete_first(rtable);
	OffsetVarNodes((Node *) query, -2, 0);
	Assert(list_length(query->rtable) >= 1);
}

/*
 * Extract the final view from the UNION ALL query.
 *
 * q1 is the query on the materialization hypertable with the finalize call
 * q2 is the query on the raw hypertable which was supplied in the inital CREATE VIEW statement
 * returns q1 from:
 * SELECT * from (  SELECT * from q1 where <coale_qual>
 *                  UNION ALL
 *                  SELECT * from q2 where existing_qual and <coale_qual>
 * where coale_qual is: time < ----> (or >= )
 * COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark( <htid>)),
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
caggtimebucket_validate(CAggTimebucketInfo *tbinfo, List *groupClause, List *targetList)
{
	ListCell *l;
	bool found = false;
	bool custom_origin = false;
	Const *const_arg;

	/* Make sure tbinfo was initialized. This assumption is used below. */
	Assert(tbinfo->bucket_width == 0);
	Assert(tbinfo->timezone == NULL);
	Assert(TIMESTAMP_NOT_FINITE(tbinfo->origin));

	foreach (l, groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, targetList);

		if (IsA(tle->expr, FuncExpr))
		{
			FuncExpr *fe = ((FuncExpr *) tle->expr);
			Node *width_arg;
			Node *col_arg;

			if (!function_allowed_in_cagg_definition(fe->funcid))
				continue;

			/*
			 * Offset variants of time_bucket functions are not
			 * supported at the moment.
			 */
			if (list_length(fe->args) >= 5 ||
				(list_length(fe->args) == 4 && exprType(lfourth(fe->args)) == INTERVALOID))
				continue;

			if (found)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("continuous aggregate view cannot contain"
								" multiple time bucket functions")));
			else
				found = true;

			tbinfo->bucket_func = fe;

			/* Only column allowed : time_bucket('1day', <column> ) */
			col_arg = lsecond(fe->args);
			/* Could be a named argument */
			if (IsA(col_arg, NamedArgExpr))
				col_arg = (Node *) castNode(NamedArgExpr, col_arg)->arg;

			if (!(IsA(col_arg, Var)) || ((Var *) col_arg)->varattno != tbinfo->htpartcolno)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg(
							 "time bucket function must reference a hypertable dimension column")));

			if (list_length(fe->args) >= 3)
			{
				Const *arg = check_time_bucket_argument(lthird(fe->args), "third");
				if (exprType((Node *) arg) == TEXTOID)
				{
					const char *tz_name = TextDatumGetCString(arg->constvalue);
					if (!ts_is_valid_timezone_name(tz_name))
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("invalid timezone name \"%s\"", tz_name)));
					}

					tbinfo->timezone = tz_name;
					tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
				}
			}

			if (list_length(fe->args) >= 4)
			{
				/* origin */
				Const *arg = check_time_bucket_argument(lfourth(fe->args), "fourth");
				if (exprType((Node *) arg) == TEXTOID)
				{
					const char *tz_name = TextDatumGetCString(arg->constvalue);
					if (!ts_is_valid_timezone_name(tz_name))
					{
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("invalid timezone name \"%s\"", tz_name)));
					}

					tbinfo->timezone = tz_name;
					tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
				}
			}

			/* Check for custom origin. */
			switch (exprType(col_arg))
			{
				case DATEOID:
					/* Origin is always 3rd arg for date variants. */
					if (list_length(fe->args) == 3)
					{
						Node *arg = lthird(fe->args);
						custom_origin = true;
						/* this function also takes care of named arguments */
						const_arg = check_time_bucket_argument(arg, "third");
						tbinfo->origin = DatumGetTimestamp(
							DirectFunctionCall1(date_timestamp, const_arg->constvalue));
					}
					break;
				case TIMESTAMPOID:
					/* Origin is always 3rd arg for timestamp variants. */
					if (list_length(fe->args) == 3)
					{
						Node *arg = lthird(fe->args);
						custom_origin = true;
						const_arg = check_time_bucket_argument(arg, "third");
						tbinfo->origin = DatumGetTimestamp(const_arg->constvalue);
					}
					break;
				case TIMESTAMPTZOID:
					/* Origin can be 3rd or 4th arg for timestamptz variants. */
					if (list_length(fe->args) >= 3 && exprType(lthird(fe->args)) == TIMESTAMPTZOID)
					{
						custom_origin = true;
						tbinfo->origin =
							DatumGetTimestampTz(castNode(Const, lthird(fe->args))->constvalue);
					}
					else if (list_length(fe->args) >= 4 &&
							 exprType(lfourth(fe->args)) == TIMESTAMPTZOID)
					{
						custom_origin = true;
						if (IsA(lfourth(fe->args), Const))
						{
							tbinfo->origin =
								DatumGetTimestampTz(castNode(Const, lfourth(fe->args))->constvalue);
						}
						/* could happen in a statement like time_bucket('1h', .., 'utc', origin =>
						 * ...) */
						else if (IsA(lfourth(fe->args), NamedArgExpr))
						{
							Const *constval =
								check_time_bucket_argument(lfourth(fe->args), "fourth");

							tbinfo->origin = DatumGetTimestampTz(constval->constvalue);
						}
					}
			}
			if (custom_origin && TIMESTAMP_NOT_FINITE(tbinfo->origin))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid origin value: infinity")));
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
				tbinfo->bucket_width_type = width->consttype;

				if (width->consttype == INTERVALOID)
				{
					tbinfo->interval = DatumGetIntervalP(width->constvalue);
					if (tbinfo->interval->month != 0)
						tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
				}

				if (tbinfo->bucket_width != BUCKET_WIDTH_VARIABLE)
				{
					/* The bucket size is fixed. */
					tbinfo->bucket_width =
						ts_interval_value_to_internal(width->constvalue, width->consttype);
				}
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("only immutable expressions allowed in time bucket function"),
						 errhint("Use an immutable expression as first argument to the time bucket "
								 "function.")));

			if (tbinfo->interval && tbinfo->interval->month)
			{
				tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
			}
		}
	}

	if (tbinfo->bucket_width == BUCKET_WIDTH_VARIABLE)
	{
		/* Variable-sized buckets can be used only with intervals. */
		Assert(tbinfo->interval != NULL);

		if ((tbinfo->interval->month != 0) &&
			((tbinfo->interval->day != 0) || (tbinfo->interval->time != 0)))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid interval specified"),
					 errhint("Use either months or days and hours, but not months, days and hours "
							 "together")));
		}
	}

	if (!found)
		elog(ERROR, "continuous aggregate view must include a valid time bucket function");
}

static bool
cagg_agg_validate(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *agg = (Aggref *) node;
		HeapTuple aggtuple;
		Form_pg_aggregate aggform;
		if (agg->aggorder || agg->aggdistinct || agg->aggfilter)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates with FILTER / DISTINCT / ORDER BY are not supported")));
		}
		/* Fetch the pg_aggregate row. */
		aggtuple = SearchSysCache1(AGGFNOID, agg->aggfnoid);
		if (!HeapTupleIsValid(aggtuple))
			elog(ERROR, "cache lookup failed for aggregate %u", agg->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggtuple);
		if (aggform->aggkind != 'n')
		{
			ReleaseSysCache(aggtuple);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ordered set/hypothetical aggregates are not supported")));
		}
		if (!OidIsValid(aggform->aggcombinefn) ||
			(aggform->aggtranstype == INTERNALOID && !OidIsValid(aggform->aggdeserialfn)))
		{
			ReleaseSysCache(aggtuple);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates which are not parallelizable are not supported")));
		}
		ReleaseSysCache(aggtuple);

		return false;
	}
	return expression_tree_walker(node, cagg_agg_validate, context);
}

/*
 * Check query and extract error details and error hints.
 *
 * Returns:
 *   True if the query is supported, false otherwise with hints and errors
 *   added.
 */
static bool
cagg_query_supported(const Query *query, StringInfo hint, StringInfo detail, const bool finalized)
{
/*
 * For now deprecate partial aggregates on release builds only.
 * Once migration tests are made compatible with PG15 enable deprecation
 * on debug builds as well.
 */
#ifndef DEBUG
#if PG15_GE
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
#endif
#endif
	if (!query->jointree->fromlist)
	{
		appendStringInfoString(hint, "FROM clause missing in the query");
		return false;
	}
	if (query->commandType != CMD_SELECT)
	{
		appendStringInfoString(hint, "Use a SELECT query in the continuous aggregate view.");
		return false;
	}

	if (query->hasWindowFuncs)
	{
		appendStringInfoString(detail,
							   "Window functions are not supported by continuous aggregates.");
		return false;
	}

	if (query->hasDistinctOn || query->distinctClause)
	{
		appendStringInfoString(detail,
							   "DISTINCT / DISTINCT ON queries are not supported by continuous "
							   "aggregates.");
		return false;
	}

	if (query->limitOffset || query->limitCount)
	{
		appendStringInfoString(detail,
							   "LIMIT and LIMIT OFFSET are not supported in queries defining "
							   "continuous aggregates.");
		appendStringInfoString(hint,
							   "Use LIMIT and LIMIT OFFSET in SELECTS from the continuous "
							   "aggregate view instead.");
		return false;
	}

	if (query->sortClause && !finalized)
	{
		appendStringInfoString(detail,
							   "ORDER BY is not supported in queries defining continuous "
							   "aggregates.");
		appendStringInfoString(hint,
							   "Use ORDER BY clauses in SELECTS from the continuous aggregate view "
							   "instead.");
		return false;
	}

	if (query->hasRecursive || query->hasSubLinks || query->hasTargetSRFs || query->cteList)
	{
		appendStringInfoString(detail,
							   "CTEs, subqueries and set-returning functions are not supported by "
							   "continuous aggregates.");
		return false;
	}

	if (query->hasForUpdate || query->hasModifyingCTE)
	{
		appendStringInfoString(detail,
							   "Data modification is not allowed in continuous aggregate view "
							   "definitions.");
		return false;
	}

	if (query->hasRowSecurity)
	{
		appendStringInfoString(detail,
							   "Row level security is not supported by continuous aggregate "
							   "views.");
		return false;
	}

	if (query->groupingSets)
	{
		appendStringInfoString(detail,
							   "GROUP BY GROUPING SETS, ROLLUP and CUBE are not supported by "
							   "continuous aggregates");
		appendStringInfoString(hint,
							   "Define multiple continuous aggregates with different grouping "
							   "levels.");
		return false;
	}

	if (query->setOperations)
	{
		appendStringInfoString(detail,
							   "UNION, EXCEPT & INTERSECT are not supported by continuous "
							   "aggregates");
		return false;
	}

	if (!query->groupClause)
	{
		/*
		 * Query can have aggregate without group by , so look
		 * for groupClause.
		 */
		appendStringInfoString(hint,
							   "Include at least one aggregate function"
							   " and a GROUP BY clause with time bucket.");
		return false;
	}

	return true; /* Query was OK and is supported. */
}

static Datum
get_bucket_width_datum(CAggTimebucketInfo bucket_info)
{
	Datum width = (Datum) 0;

	switch (bucket_info.bucket_width_type)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			width = ts_internal_to_interval_value(bucket_info.bucket_width,
												  bucket_info.bucket_width_type);
			break;
		case INTERVALOID:
			width = IntervalPGetDatum(bucket_info.interval);
			break;
		default:
			Assert(false);
	}

	return width;
}

static int64
get_bucket_width(CAggTimebucketInfo bucket_info)
{
	int64 width = 0;

	/* Calculate the width. */
	switch (bucket_info.bucket_width_type)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			width = bucket_info.bucket_width;
			break;
		case INTERVALOID:
		{
			/*
			 * Original interval should not be changed, hence create a local copy
			 * for this check.
			 */
			Interval interval = *bucket_info.interval;

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

CAggTimebucketInfo
cagg_validate_query(const Query *query, const bool finalized, const char *cagg_schema,
					const char *cagg_name)
{
	CAggTimebucketInfo bucket_info = { 0 }, bucket_info_parent;
	Cache *hcache;
	Hypertable *ht = NULL, *ht_parent = NULL;
	RangeTblRef *rtref = NULL, *rtref_other = NULL;
	RangeTblEntry *rte = NULL, *rte_other = NULL;
	JoinType jointype = JOIN_FULL;
	OpExpr *op = NULL;
	List *fromList = NIL;
	StringInfo hint = makeStringInfo();
	StringInfo detail = makeStringInfo();
	bool is_hierarchical = false;
	Query *prev_query = NULL;
	ContinuousAgg *cagg_parent = NULL;
	Oid normal_table_id = InvalidOid;

	if (!cagg_query_supported(query, hint, detail, finalized))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid continuous aggregate query"),
				 hint->len > 0 ? errhint("%s", hint->data) : 0,
				 detail->len > 0 ? errdetail("%s", detail->data) : 0));
	}

	/* Finalized cagg doesn't have those restrictions anymore. */
	if (!finalized)
	{
		/* Validate aggregates allowed. */
		cagg_agg_validate((Node *) query->targetList, NULL);
		cagg_agg_validate((Node *) query->havingQual, NULL);
	}
	/* Check if there are only two tables in the from list. */
	fromList = query->jointree->fromlist;
	if (list_length(fromList) > CONTINUOUS_AGG_MAX_JOIN_RELATIONS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only two tables with one hypertable and one normal table"
						"are  allowed in continuous aggregate view")));
	}
	/* Extra checks for joins in Caggs. */
	if (list_length(fromList) == CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(query->jointree->fromlist), RangeTblRef))
	{
		/* Using old format caggs is not supported */
		if (!finalized)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("old format of continuous aggregate is not supported with joins"),
					 errhint("Set timescaledb.finalized to TRUE.")));

		if (list_length(fromList) == CONTINUOUS_AGG_MAX_JOIN_RELATIONS)
		{
			if (!IsA(linitial(fromList), RangeTblRef) || !IsA(lsecond(fromList), RangeTblRef))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid continuous aggregate view"),
						 errdetail(
							 "From clause can only have one hypertable and one normal table.")));

			rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
			rte = list_nth(query->rtable, rtref->rtindex - 1);
			rtref_other = lsecond_node(RangeTblRef, query->jointree->fromlist);
			rte_other = list_nth(query->rtable, rtref_other->rtindex - 1);
			jointype = rte->jointype || rte_other->jointype;

			if (query->jointree->quals != NULL && IsA(query->jointree->quals, OpExpr))
				op = (OpExpr *) query->jointree->quals;
		}
		else
		{
			ListCell *l;
			foreach (l, query->jointree->fromlist)
			{
				Node *jtnode = (Node *) lfirst(l);
				JoinExpr *join = NULL;
				if (IsA(jtnode, JoinExpr))
				{
					join = castNode(JoinExpr, jtnode);
#if PG13_LT
					if (join->usingClause != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("invalid continuous aggregate view"),
								 errdetail(
									 "Joins with USING clause in continuous aggregate definition"
									 " work for Postgres versions 13 and above.")));
#endif
					jointype = join->jointype;
					op = (OpExpr *) join->quals;
					rte = list_nth(query->rtable, ((RangeTblRef *) join->larg)->rtindex - 1);
					rte_other = list_nth(query->rtable, ((RangeTblRef *) join->rarg)->rtindex - 1);
					if (rte->subquery != NULL || rte_other->subquery != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("invalid continuous aggregate view"),
								 errdetail("Sub-queries are not supported in FROM clause.")));
					RangeTblEntry *jrte = rt_fetch(join->rtindex, query->rtable);
					if (jrte->joinaliasvars == NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("invalid continuous aggregate view")));
				}
			}
		}

		/*
		 * Error out if there is aynthing else than one normal table and one hypertable
		 * in the from clause, e.g. sub-query, lateral, two hypertables, etc.
		 */
		if (rte->lateral || rte_other->lateral)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view"),
					 errdetail("Lateral joins are not supported in FROM clause.")));
		if ((rte->relkind == RELKIND_VIEW && ts_is_hypertable(rte_other->relid)) ||
			(rte_other->relkind == RELKIND_VIEW && ts_is_hypertable(rte->relid)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view"),
					 errdetail("Views are not supported in FROM clause.")));
		if (rte->relkind != RELKIND_VIEW && rte_other->relkind != RELKIND_VIEW &&
			(ts_is_hypertable(rte->relid) == ts_is_hypertable(rte_other->relid)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view"),
					 errdetail("Multiple hypertables or normal tables are not supported in FROM "
							   "clause.")));

		/* Only inner joins are allowed. */
		if (jointype != JOIN_INNER)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only inner joins are supported in continuous aggregates")));

		/* Only equality conditions are permitted on joins. */
		if (op && IsA(op, OpExpr) &&
			list_length(castNode(OpExpr, op)->args) == CONTINUOUS_AGG_MAX_JOIN_RELATIONS)
		{
			Oid left_type = exprType(linitial(op->args));
			Oid right_type = exprType(lsecond(op->args));
			if (!ts_is_equality_operator(op->opno, left_type, right_type))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid continuous aggregate view"),
						 errdetail(
							 "Only equality conditions are supported in continuous aggregates.")));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view"),
					 errdetail("Unsupported expression in join clause."),
					 errhint("Only equality conditions are supported in continuous aggregates.")));
		/*
		 * Record the table oid of the normal table. This is required so
		 * that we know which one is hypertable to carry out the related
		 * processing in later parts of code.
		 */
		if (rte->relkind == RELKIND_VIEW)
			normal_table_id = rte_other->relid;
		else if (rte_other->relkind == RELKIND_VIEW)
			normal_table_id = rte->relid;
		else
			normal_table_id = ts_is_hypertable(rte->relid) ? rte_other->relid : rte->relid;
		if (normal_table_id == rte->relid)
			rte = rte_other;
	}
	else
	{
		/* Check if we have a hypertable in the FROM clause. */
		rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
		rte = list_nth(query->rtable, rtref->rtindex - 1);
	}
	/* FROM only <tablename> sets rte->inh to false. */
	if (rte->rtekind != RTE_JOIN)
	{
		if ((rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_VIEW) ||
			rte->tablesample || rte->inh == false)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid continuous aggregate view")));
	}

	if (rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_VIEW)
	{
		const Dimension *part_dimension = NULL;
		int32 parent_mat_hypertable_id = INVALID_HYPERTABLE_ID;

		if (rte->relkind == RELKIND_RELATION)
			ht = ts_hypertable_cache_get_cache_and_entry(rte->relid, CACHE_FLAG_NONE, &hcache);
		else
		{
			cagg_parent = ts_continuous_agg_find_by_relid(rte->relid);

			if (!cagg_parent)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid continuous aggregate query"),
						 errhint("Continuous aggregate needs to query hypertable or another "
								 "continuous aggregate.")));
			}

			if (!ContinuousAggIsFinalized(cagg_parent))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("old format of continuous aggregate is not supported"),
						 errhint("Run \"CALL cagg_migrate('%s.%s');\" to migrate to the new "
								 "format.",
								 NameStr(cagg_parent->data.user_view_schema),
								 NameStr(cagg_parent->data.user_view_name))));
			}

			parent_mat_hypertable_id = cagg_parent->data.mat_hypertable_id;
			hcache = ts_hypertable_cache_pin();
			ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg_parent->data.mat_hypertable_id);

			/* If parent cagg is hierarchical then we should get the matht otherwise the rawht. */
			if (ContinuousAggIsHierarchical(cagg_parent))
				ht_parent =
					ts_hypertable_cache_get_entry_by_id(hcache,
														cagg_parent->data.mat_hypertable_id);
			else
				ht_parent =
					ts_hypertable_cache_get_entry_by_id(hcache,
														cagg_parent->data.raw_hypertable_id);

			/* Get the querydef for the source cagg. */
			is_hierarchical = true;
			prev_query = ts_continuous_agg_get_query(cagg_parent);
		}

		if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertable is an internal compressed hypertable")));

		if (rte->relkind == RELKIND_RELATION)
		{
			ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);

			/* Prevent create a CAGG over an existing materialization hypertable. */
			if (status == HypertableIsMaterialization ||
				status == HypertableIsMaterializationAndRaw)
			{
				const ContinuousAgg *cagg = ts_continuous_agg_find_by_mat_hypertable_id(ht->fd.id);
				Assert(cagg != NULL);

				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertable is a continuous aggregate materialization table"),
						 errdetail("Materialization hypertable \"%s.%s\".",
								   NameStr(ht->fd.schema_name),
								   NameStr(ht->fd.table_name)),
						 errhint("Do you want to use continuous aggregate \"%s.%s\" instead?",
								 NameStr(cagg->data.user_view_schema),
								 NameStr(cagg->data.user_view_name))));
			}
		}

		/* Get primary partitioning column information. */
		part_dimension = hyperspace_get_open_dimension(ht->space, 0);

		/*
		 * NOTE: if we ever allow custom partitioning functions we'll need to
		 *       change part_dimension->fd.column_type to partitioning_type
		 *       below, along with any other fallout.
		 */
		if (part_dimension->partitioning != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("custom partitioning functions not supported"
							" with continuous aggregates")));

		if (IS_INTEGER_TYPE(ts_dimension_get_partition_type(part_dimension)) &&
			rte->relkind == RELKIND_RELATION)
		{
			const char *funcschema = NameStr(part_dimension->fd.integer_now_func_schema);
			const char *funcname = NameStr(part_dimension->fd.integer_now_func);

			if (strlen(funcschema) == 0 || strlen(funcname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("custom time function required on hypertable \"%s\"",
								get_rel_name(ht->main_table_relid)),
						 errdetail("An integer-based hypertable requires a custom time function to "
								   "support continuous aggregates."),
						 errhint("Set a custom time function on the hypertable.")));
		}

		caggtimebucketinfo_init(&bucket_info,
								ht->fd.id,
								ht->main_table_relid,
								part_dimension->column_attno,
								part_dimension->fd.column_type,
								part_dimension->fd.interval_length,
								parent_mat_hypertable_id);

		if (is_hierarchical)
		{
			const Dimension *part_dimension_parent =
				hyperspace_get_open_dimension(ht_parent->space, 0);

			caggtimebucketinfo_init(&bucket_info_parent,
									ht_parent->fd.id,
									ht_parent->main_table_relid,
									part_dimension_parent->column_attno,
									part_dimension_parent->fd.column_type,
									part_dimension_parent->fd.interval_length,
									INVALID_HYPERTABLE_ID);
		}

		ts_cache_release(hcache);

		/*
		 * We need a GROUP By clause with time_bucket on the partitioning
		 * column of the hypertable
		 */
		Assert(query->groupClause);
		caggtimebucket_validate(&bucket_info, query->groupClause, query->targetList);
	}

	/* Check row security settings for the table. */
	if (ts_has_row_security(rte->relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create continuous aggregate on hypertable with row security")));

	/* hierarchical cagg validations */
	if (is_hierarchical)
	{
		int64 bucket_width = 0, bucket_width_parent = 0;
		bool is_greater_or_equal_than_parent = true, is_multiple_of_parent = true;

		Assert(prev_query->groupClause);
		caggtimebucket_validate(&bucket_info_parent,
								prev_query->groupClause,
								prev_query->targetList);

		/* Cannot create cagg with fixed bucket on top of variable bucket. */
		if ((bucket_info_parent.bucket_width == BUCKET_WIDTH_VARIABLE &&
			 bucket_info.bucket_width != BUCKET_WIDTH_VARIABLE))
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
			Datum width, width_parent;
			Oid outfuncid = InvalidOid;
			bool isvarlena;
			char *width_out, *width_out_parent;
			char *message = NULL;

			getTypeOutputInfo(bucket_info.bucket_width_type, &outfuncid, &isvarlena);
			width = get_bucket_width_datum(bucket_info);
			width_out = DatumGetCString(OidFunctionCall1(outfuncid, width));

			getTypeOutputInfo(bucket_info_parent.bucket_width_type, &outfuncid, &isvarlena);
			width_parent = get_bucket_width_datum(bucket_info_parent);
			width_out_parent = DatumGetCString(OidFunctionCall1(outfuncid, width_parent));

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
							   width_out,
							   message,
							   NameStr(cagg_parent->data.user_view_schema),
							   NameStr(cagg_parent->data.user_view_name),
							   width_out_parent)));
		}
	}

	return bucket_info;
}

/*
 * Get oid of function to convert from our internal representation
 * to postgres representation.
 */
static Oid
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
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("no converter function defined for datatype: %s",
							format_type_be(typoid))));
			pg_unreachable();
	}

	List *func_name = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(function_name));
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
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(type))));
			pg_unreachable();
	}
}

/*
 * Build function call that returns boundary for a hypertable
 * wrapped in type conversion calls when required.
 */
static FuncExpr *
build_boundary_call(int32 ht_id, Oid type)
{
	Oid argtyp[] = { INT4OID };
	FuncExpr *boundary;

	Oid boundary_func_oid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(BOUNDARY_FUNCTION)),
					   lengthof(argtyp),
					   argtyp,
					   false);
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
	rte->inFromCl = true;

	return rte;
}

/*
 * Build union query combining the materialized data with data from the raw data hypertable.
 *
 * q1 is the query on the materialization hypertable with the finalize call
 * q2 is the query on the raw hypertable which was supplied in the inital CREATE VIEW statement
 * returns a query as
 * SELECT * from (  SELECT * from q1 where <coale_qual>
 *                  UNION ALL
 *                  SELECT * from q2 where existing_qual and <coale_qual>
 * where coale_qual is: time < ----> (or >= )
 * COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark( <htid>)),
 * '-infinity'::timestamp with time zone)
 * See build_union_quals for COALESCE clauses.
 */
Query *
build_union_query(CAggTimebucketInfo *tbinfo, int matpartcolno, Query *q1, Query *q2,
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

	/*
	 * In case of joins it is enough to check if the first node is not RangeTblRef,
	 * because the jointree has RangeTblRef as leaves and JoinExpr above them.
	 * So if JoinExpr is present, it is the first node.
	 * Other cases of join i.e. without explicit JOIN clause is confirmed
	 * by reading the length of rtable.
	 */
	if (list_length(q2->rtable) == CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(q2->jointree->fromlist), RangeTblRef))
	{
		Oid normal_table_id = InvalidOid;
		RangeTblEntry *rte = NULL;
		RangeTblEntry *rte_other = NULL;

		if (list_length(q2->rtable) == CONTINUOUS_AGG_MAX_JOIN_RELATIONS)
		{
			RangeTblRef *rtref = linitial_node(RangeTblRef, q2->jointree->fromlist);
			rte = list_nth(q2->rtable, rtref->rtindex - 1);
			RangeTblRef *rtref_other = lsecond_node(RangeTblRef, q2->jointree->fromlist);
			rte_other = list_nth(q2->rtable, rtref_other->rtindex - 1);
		}
		else if (!IsA(linitial(q2->jointree->fromlist), RangeTblRef))
		{
			ListCell *l;
			foreach (l, q2->jointree->fromlist)
			{
				Node *jtnode = (Node *) lfirst(l);
				JoinExpr *join = NULL;
				if (IsA(jtnode, JoinExpr))
				{
					join = castNode(JoinExpr, jtnode);
					rte = list_nth(q2->rtable, ((RangeTblRef *) join->larg)->rtindex - 1);
					rte_other = list_nth(q2->rtable, ((RangeTblRef *) join->rarg)->rtindex - 1);
				}
			}
		}
		if (rte->relkind == RELKIND_VIEW)
			normal_table_id = rte_other->relid;
		else if (rte_other->relkind == RELKIND_VIEW)
			normal_table_id = rte->relid;
		else
			normal_table_id = ts_is_hypertable(rte->relid) ? rte_other->relid : rte->relid;
		if (normal_table_id == rte->relid)
			varno = 2;
		else
			varno = 1;
	}
	else
		varno = list_length(q2->rtable);
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

	if (sortClause)
	{
		query->sortClause = sortClause;
		query->jointree = makeFromExpr(NIL, NULL);
	}

	setop->colTypes = col_types;
	setop->colTypmods = col_typmods;
	setop->colCollations = col_collations;

	return query;
}
