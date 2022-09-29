/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "common.h"

#define DEFAULT_MATPARTCOLUMN_NAME "time_partition_col"

#define PRINT_MATCOLNAME(colbuf, type, original_query_resno, colno)                                \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(colbuf, NAMEDATALEN, "%s_%d_%d", type, original_query_resno, colno);    \
		if (ret < 0 || ret >= NAMEDATALEN)                                                         \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("bad materialization table column name")));                            \
	} while (0);

/* creates a partialize expr for the passed in agg:
 * partialize_agg( agg)
 */
static FuncExpr *
get_partialize_funcexpr(Aggref *agg)
{
	FuncExpr *partialize_fnexpr;
	Oid partfnoid, partargtype;
	partargtype = ANYELEMENTOID;
	partfnoid = LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(PARTIALFN)),
							   1,
							   &partargtype,
							   false);
	partialize_fnexpr = makeFuncExpr(partfnoid,
									 BYTEAOID,
									 list_make1(agg), /*args*/
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);
	return partialize_fnexpr;
}

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

/* initialize caggtimebucket */
static void
caggtimebucketinfo_init(CAggTimebucketInfo *src, int32 hypertable_id, Oid hypertable_oid,
						AttrNumber hypertable_partition_colno, Oid hypertable_partition_coltype,
						int64 hypertable_partition_col_interval)
{
	src->htid = hypertable_id;
	src->htoid = hypertable_oid;
	src->htpartcolno = hypertable_partition_colno;
	src->htpartcoltype = hypertable_partition_coltype;
	src->htpartcol_interval_len = hypertable_partition_col_interval;
	src->bucket_width = 0;			/* invalid value */
	src->interval = NULL;			/* not specified by default */
	src->timezone = NULL;			/* not specified by default */
	TIMESTAMP_NOBEGIN(src->origin); /* origin is not specified by default */
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
 * Add Information required to create and populate the materialization table columns
 * a) create a columndef for the materialization table
 * b) create the corresponding expr to populate the column of the materialization table (e..g for a
 *    column that is an aggref, we create a partialize_agg expr to populate the column Returns: the
 *    Var corresponding to the newly created column of the materialization table
 *
 * Notes: make sure the materialization table columns do not save
 *        values computed by mutable function.
 *
 * Notes on TargetEntry fields:
 * - (resname != NULL) means it's projected in our case
 * - (ressortgroupref > 0) means part of GROUP BY, which can be projected or not, depending of the
 *                         value of the resjunk
 * - (resjunk == true) applies for GROUP BY columns that are not projected
 *
 */
Var *
mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input, int original_query_resno,
							bool finalized, bool *skip_adding)
{
	int matcolno = list_length(out->matcollist) + 1;
	char colbuf[NAMEDATALEN];
	char *colname;
	TargetEntry *part_te = NULL;
	ColumnDef *col;
	Var *var;
	Oid coltype, colcollation;
	int32 coltypmod;

	*skip_adding = false;

	if (contain_mutable_functions(input))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only immutable functions supported in continuous aggregate view"),
				 errhint("Make sure all functions in the continuous aggregate definition"
						 " have IMMUTABLE volatility. Note that functions or expressions"
						 " may be IMMUTABLE for one data type, but STABLE or VOLATILE for"
						 " another.")));
	}

	switch (nodeTag(input))
	{
		case T_Aggref:
		{
			FuncExpr *fexpr = get_partialize_funcexpr((Aggref *) input);
			PRINT_MATCOLNAME(colbuf, "agg", original_query_resno, matcolno);
			colname = colbuf;
			coltype = BYTEAOID;
			coltypmod = -1;
			colcollation = InvalidOid;
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) fexpr, matcolno, pstrdup(colname), false);
		}
		break;

		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *) input;
			bool timebkt_chk = false;

			if (IsA(tle->expr, FuncExpr))
				timebkt_chk = function_allowed_in_cagg_definition(((FuncExpr *) tle->expr)->funcid);

			if (tle->resname)
				colname = pstrdup(tle->resname);
			else
			{
				if (timebkt_chk)
					colname = DEFAULT_MATPARTCOLUMN_NAME;
				else
				{
					PRINT_MATCOLNAME(colbuf, "grp", original_query_resno, matcolno);
					colname = colbuf;

					/* for finalized form we skip adding extra group by columns */
					*skip_adding = finalized;
				}
			}

			if (timebkt_chk)
			{
				tle->resname = pstrdup(colname);
				out->matpartcolno = matcolno;
				out->matpartcolname = pstrdup(colname);
			}
			else
			{
				/*
				 * Add indexes only for columns that are part of the GROUP BY clause
				 * and for finals form we skip adding it because we'll not add the
				 * extra group by columns to the materialization hypertable anymore
				 */
				if (!*skip_adding && tle->ressortgroupref > 0)
					out->mat_groupcolname_list =
						lappend(out->mat_groupcolname_list, pstrdup(colname));
			}

			coltype = exprType((Node *) tle->expr);
			coltypmod = exprTypmod((Node *) tle->expr);
			colcollation = exprCollation((Node *) tle->expr);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = (TargetEntry *) copyObject(input);

			/* keep original resjunk if finalized or not time bucket */
			if (!finalized || timebkt_chk)
			{
				/*
				 * Need to project all the partial entries so that
				 * materialization table is filled
				 */
				part_te->resjunk = false;
			}

			part_te->resno = matcolno;

			if (timebkt_chk)
			{
				col->is_not_null = true;
			}

			if (part_te->resname == NULL)
			{
				part_te->resname = pstrdup(colname);
			}
		}
		break;

		case T_Var:
		{
			PRINT_MATCOLNAME(colbuf, "var", original_query_resno, matcolno);
			colname = colbuf;

			coltype = exprType(input);
			coltypmod = exprTypmod(input);
			colcollation = exprCollation(input);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) input, matcolno, pstrdup(colname), false);

			/* need to project all the partial entries so that materialization table is filled */
			part_te->resjunk = false;
			part_te->resno = matcolno;
		}
		break;

		default:
			elog(ERROR, "invalid node type %d", nodeTag(input));
			break;
	}
	Assert((!finalized && list_length(out->matcollist) == list_length(out->partial_seltlist)) ||
		   (finalized && list_length(out->matcollist) <= list_length(out->partial_seltlist)));
	Assert(col != NULL);
	Assert(part_te != NULL);

	if (!*skip_adding)
	{
		out->matcollist = lappend(out->matcollist, col);
	}

	out->partial_seltlist = lappend(out->partial_seltlist, part_te);

	var = makeVar(1, matcolno, coltype, coltypmod, colcollation, 0);
	return var;
}

/*
 * Return Oid for a schema-qualified relation
 */
Oid
relation_oid(NameData schema, NameData name)
{
	return get_relname_relid(NameStr(name), get_namespace_oid(NameStr(schema), false));
}

/*
 * When a view is created (StoreViewQuery), 2 dummy rtable entries corresponding to "old" and
 * "new" are prepended to the rtable list. We remove these and adjust the varnos to recreate
 * the user or direct view query.
 */
void
remove_old_and_new_rte_from_query(Query *query)
{
	List *rtable = query->rtable;
	Assert(list_length(rtable) >= 3);
	rtable = list_delete_first(rtable);
	query->rtable = list_delete_first(rtable);
	OffsetVarNodes((Node *) query, -2, 0);
	Assert(list_length(query->rtable) >= 1);
}

/*
 * Extract the final view from the UNION ALL query
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

	/* Get RTE of the left-hand side of UNION ALL */
	RangeTblEntry *rte = linitial(q->rtable);
	Assert(rte->rtekind == RTE_SUBQUERY);

	Query *query = copyObject(rte->subquery);

	/* Delete the WHERE clause from the final view */
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
			 * offset variants of time_bucket functions are not
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

			/*only column allowed : time_bucket('1day', <column> ) */
			col_arg = lsecond(fe->args);

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

			/* check for custom origin */
			switch (exprType(col_arg))
			{
				case DATEOID:
					/* origin is always 3rd arg for date variants */
					if (list_length(fe->args) == 3)
					{
						custom_origin = true;
						tbinfo->origin = DatumGetTimestamp(
							DirectFunctionCall1(date_timestamp,
												castNode(Const, lthird(fe->args))->constvalue));
					}
					break;
				case TIMESTAMPOID:
					/* origin is always 3rd arg for timestamp variants */
					if (list_length(fe->args) == 3)
					{
						custom_origin = true;
						tbinfo->origin =
							DatumGetTimestamp(castNode(Const, lthird(fe->args))->constvalue);
					}
					break;
				case TIMESTAMPTZOID:
					/* origin can be 3rd or 4th arg for timestamptz variants */
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
						tbinfo->origin =
							DatumGetTimestampTz(castNode(Const, lfourth(fe->args))->constvalue);
					}
			}
			if (custom_origin && TIMESTAMP_NOT_FINITE(tbinfo->origin))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid origin value: infinity")));
			}

			/*
			 * We constify width expression here so any immutable expression will be allowed
			 * otherwise it would make it harder to create caggs for hypertables with e.g. int8
			 * partitioning column as int constants default to int4 and so expression would
			 * have a cast and not be a Const.
			 */
			width_arg = eval_const_expressions(NULL, linitial(fe->args));
			if (IsA(width_arg, Const))
			{
				Const *width = castNode(Const, width_arg);
				if (width->consttype == INTERVALOID)
				{
					tbinfo->interval = DatumGetIntervalP(width->constvalue);
					if (tbinfo->interval->month != 0)
						tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
				}

				if (tbinfo->bucket_width != BUCKET_WIDTH_VARIABLE)
				{
					/* The bucket size is fixed */
					tbinfo->bucket_width =
						ts_interval_value_to_internal(width->constvalue, width->consttype);
				}
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("only immutable expressions allowed in time bucket function"),
						 errhint("Use an immutable expression as first argument"
								 " to the time bucket function.")));

			if (tbinfo->interval && tbinfo->interval->month)
			{
				tbinfo->bucket_width = BUCKET_WIDTH_VARIABLE;
			}
		}
	}

	if (tbinfo->bucket_width == BUCKET_WIDTH_VARIABLE)
	{
		/* variable-sized buckets can be used only with intervals */
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
		/* Fetch the pg_aggregate row */
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
		if (aggform->aggcombinefn == InvalidOid ||
			(aggform->aggtranstype == INTERNALOID && aggform->aggdeserialfn == InvalidOid))
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
		/*query can have aggregate without group by , so look
		 * for groupClause*/
		appendStringInfoString(hint,
							   "Include at least one aggregate function"
							   " and a GROUP BY clause with time bucket.");
		return false;
	}

	return true; /* Query was OK and is supported */
}

CAggTimebucketInfo
cagg_validate_query(const Query *query, const bool finalized)
{
	CAggTimebucketInfo ret;
	Cache *hcache;
	Hypertable *ht = NULL;
	RangeTblRef *rtref = NULL;
	RangeTblEntry *rte;
	List *fromList;
	StringInfo hint = makeStringInfo();
	StringInfo detail = makeStringInfo();

	if (!cagg_query_supported(query, hint, detail, finalized))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid continuous aggregate query"),
				 hint->len > 0 ? errhint("%s", hint->data) : 0,
				 detail->len > 0 ? errdetail("%s", detail->data) : 0));
	}

	/* finalized cagg doesn't have those restrictions anymore */
	if (!finalized)
	{
		/* validate aggregates allowed */
		cagg_agg_validate((Node *) query->targetList, NULL);
		cagg_agg_validate((Node *) query->havingQual, NULL);
	}

	fromList = query->jointree->fromlist;
	if (list_length(fromList) != 1 || !IsA(linitial(fromList), RangeTblRef))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only one hypertable allowed in continuous aggregate view")));
	}
	/* check if we have a hypertable in the FROM clause */
	rtref = linitial_node(RangeTblRef, query->jointree->fromlist);
	rte = list_nth(query->rtable, rtref->rtindex - 1);
	/* FROM only <tablename> sets rte->inh to false */
	if (rte->relkind != RELKIND_RELATION || rte->tablesample || rte->inh == false)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid continuous aggregate view")));
	}
	if (rte->relkind == RELKIND_RELATION)
	{
		const Dimension *part_dimension = NULL;

		ht = ts_hypertable_cache_get_cache_and_entry(rte->relid, CACHE_FLAG_NONE, &hcache);

		if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertable is an internal compressed hypertable")));

		/* there can only be one continuous aggregate per table */
		switch (ts_continuous_agg_hypertable_status(ht->fd.id))
		{
			case HypertableIsMaterialization:
			case HypertableIsMaterializationAndRaw:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertable is a continuous aggregate materialization table")));
			case HypertableIsRawTable:
				break;
			case HypertableIsNotContinuousAgg:
				break;
			default:
				Assert(false);
		}

		/* get primary partitioning column information */
		part_dimension = hyperspace_get_open_dimension(ht->space, 0);

		/* NOTE: if we ever allow custom partitioning functions we'll need to
		 *       change part_dimension->fd.column_type to partitioning_type
		 *       below, along with any other fallout
		 */
		if (part_dimension->partitioning != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("custom partitioning functions not supported"
							" with continuous aggregates")));

		if (IS_INTEGER_TYPE(ts_dimension_get_partition_type(part_dimension)))
		{
			const char *funcschema = NameStr(part_dimension->fd.integer_now_func_schema);
			const char *funcname = NameStr(part_dimension->fd.integer_now_func);

			if (strlen(funcschema) == 0 || strlen(funcname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("custom time function required on hypertable \"%s\"",
								get_rel_name(ht->main_table_relid)),
						 errdetail("An integer-based hypertable requires a custom time"
								   " function to support continuous aggregates."),
						 errhint("Set a custom time function on the hypertable.")));
		}

		caggtimebucketinfo_init(&ret,
								ht->fd.id,
								ht->main_table_relid,
								part_dimension->column_attno,
								part_dimension->fd.column_type,
								part_dimension->fd.interval_length);

		ts_cache_release(hcache);
	}

	/*check row security settings for the table */
	if (ts_has_row_security(rte->relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot create continuous aggregate on hypertable with row security")));

	/* we need a GROUP By clause with time_bucket on the partitioning
	 * column of the hypertable
	 */
	Assert(query->groupClause);

	caggtimebucket_validate(&ret, query->groupClause, query->targetList);
	return ret;
}

/*
 * get oid of function to convert from our internal representation to postgres representation
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
			 * this should never be reached and unsupported datatypes
			 * should be caught at much earlier stages
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
	 * if the partitioning column type is not integer we need to convert to proper representation
	 */
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		{
			/* since the boundary function returns int8 we need to cast to proper type here */
			Oid cast_oid = ts_get_cast_func(INT8OID, type);

			return makeFuncExpr(cast_oid,
								type,
								list_make1(boundary),
								InvalidOid,
								InvalidOid,
								COERCE_IMPLICIT_CAST);
		}
		case INT8OID:
			/* nothing to do for int8 */
			return boundary;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			/* date/timestamp/timestamptz need to be converted since we store them differently from
			 * postgres format */
			Oid converter_oid = cagg_get_boundary_converter_funcoid(type);
			return makeFuncExpr(converter_oid,
								type,
								list_make1(boundary),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);
		}

		default:
			/* all valid types should be handled above, this should never be reached and error
			 * handling at earlier stages should catch this */
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("unsupported datatype for continuous aggregates: %s",
							format_type_be(type))));
			pg_unreachable();
	}
}

/*
 * build function call that returns boundary for a hypertable
 * wrapped in type conversion calls when required
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
 * create Const of proper type for lower bound of watermark when
 * watermark has not been set yet
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
make_subquery_rte(Query *subquery, const char *aliasname)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	ListCell *lc;

	rte->rtekind = RTE_SUBQUERY;
	rte->relid = InvalidOid;
	rte->subquery = subquery;
	rte->alias = makeAlias(aliasname, NIL);
	rte->eref = copyObject(rte->alias);

	foreach (lc, subquery->targetList)
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
 * build union query combining the materialized data with data from the raw data hypertable
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
 * see build_union_quals for COALESCE clause
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
	varno = list_length(q2->rtable);
	q2_quals = build_union_query_quals(materialize_htid,
									   tbinfo->htpartcoltype,
									   get_negator(tce->lt_opr),
									   varno,
									   tbinfo->htpartcolno);
	q2->jointree->quals = make_and_qual(q2->jointree->quals, q2_quals);

	Query *query = makeNode(Query);
	SetOperationStmt *setop = makeNode(SetOperationStmt);
	RangeTblEntry *rte_q1 = make_subquery_rte(q1, "*SELECT* 1");
	RangeTblEntry *rte_q2 = make_subquery_rte(q2, "*SELECT* 2");
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
			 * we need to use resname from q2 because that is the query from the
			 * initial CREATE VIEW statement so the VIEW can be updated in place
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
