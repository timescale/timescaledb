/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains the implementation for altering continuous aggregates,
 * specifically adding columns to an existing continuous aggregate.
 */

#include <postgres.h>

#include "export.h"

#include <access/table.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <optimizer/optimizer.h>
#include <parser/analyze.h>
#include <parser/parse_expr.h>
#include <parser/parse_node.h>
#include <parser/parse_relation.h>
#include <parser/parse_target.h>
#include <parser/parser.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "alter.h"
#include "common.h"
#include "create.h"
#include "debug_assert.h"
#include "dimension.h"
#include "extension_constants.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/continuous_agg.h"

/*
 * Structure to hold parsed aggregate expression information
 */
typedef struct AggregateExprInfo
{
	char *column_alias;	  /* alias for the result column */
	Oid result_type;	  /* return type of aggregate */
	int32 result_typmod;  /* typmod of result type */
	Oid result_collation; /* collation of result */
	Aggref *aggref;		  /* the parsed Aggref node */
} AggregateExprInfo;

/*
 * Walker function to collect column references from an expression
 */
typedef struct CollectColumnContext
{
	List *column_names;
} CollectColumnContext;

static bool
collect_column_walker(Node *node, CollectColumnContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		ColumnRef *cref = (ColumnRef *) node;
		/* Get the column name (last element of fields list) */
		Node *field = llast(cref->fields);
		if (IsA(field, String))
		{
			char *colname = strVal(field);
			/* Add to list if not already present */
			ListCell *lc;
			bool found = false;
			foreach (lc, context->column_names)
			{
				if (strcmp(strVal(lfirst(lc)), colname) == 0)
				{
					found = true;
					break;
				}
			}
			if (!found)
				context->column_names =
					lappend(context->column_names, makeString(pstrdup(colname)));
		}
		return false;
	}

	return raw_expression_tree_walker(node, collect_column_walker, context);
}

/*
 * Parse and validate an aggregate expression
 *
 * Returns an AggregateExprInfo structure with parsed information.
 * If the expression is not a valid aggregate, returns NULL.
 */
static AggregateExprInfo *
parse_aggregate_expression(const char *expr_str, Oid source_relid)
{
	AggregateExprInfo *info;
	char *query_str;
	RawStmt *raw_stmt;
	List *raw_parsetree_list;
	SelectStmt *select_stmt;
	ResTarget *res_target;
	CollectColumnContext col_ctx = { .column_names = NIL };

	/* Build a SELECT statement to parse the expression */
	query_str = psprintf("SELECT %s", expr_str);

	/* Parse the query */
	raw_parsetree_list = raw_parser(query_str, RAW_PARSE_DEFAULT);
	if (list_length(raw_parsetree_list) != 1)
	{
		pfree(query_str);
		return NULL;
	}

	raw_stmt = linitial_node(RawStmt, raw_parsetree_list);
	if (!IsA(raw_stmt->stmt, SelectStmt))
	{
		pfree(query_str);
		return NULL;
	}

	select_stmt = (SelectStmt *) raw_stmt->stmt;
	if (list_length(select_stmt->targetList) != 1)
	{
		pfree(query_str);
		return NULL;
	}

	res_target = linitial_node(ResTarget, select_stmt->targetList);

	/* Check if it's a FuncCall (aggregate functions are parsed as FuncCall initially) */
	if (!IsA(res_target->val, FuncCall))
	{
		pfree(query_str);
		return NULL;
	}

	FuncCall *func_call = (FuncCall *) res_target->val;

	/* Look up the function to check if it's an aggregate */
	List *funcname = func_call->funcname;
	FuncCandidateList clist;
	Oid funcoid = InvalidOid;
	HeapTuple proc_tuple;
	Form_pg_proc proc_form;

	/* Get the number of arguments - special case for count(*) which has agg_star=true */
	int nargs = func_call->agg_star ? 0 : list_length(func_call->args);

	/* Get function candidates with the correct number of arguments */
	clist = FuncnameGetCandidates(funcname, nargs, NIL, true, false, false, false);

	/* Find an aggregate function among candidates */
	while (clist)
	{
		proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
		if (HeapTupleIsValid(proc_tuple))
		{
			proc_form = (Form_pg_proc) GETSTRUCT(proc_tuple);
			if (proc_form->prokind == PROKIND_AGGREGATE)
			{
				funcoid = clist->oid;
				ReleaseSysCache(proc_tuple);
				break;
			}
			ReleaseSysCache(proc_tuple);
		}
		clist = clist->next;
	}

	if (!OidIsValid(funcoid))
	{
		pfree(query_str);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an aggregate function", NameListToString(funcname))));
	}

	/* Collect column references from the aggregate arguments */
	collect_column_walker((Node *) func_call->args, &col_ctx);

	/* Validate that all referenced columns exist in source relation */
	ListCell *lc;
	foreach (lc, col_ctx.column_names)
	{
		char *colname = strVal(lfirst(lc));
		AttrNumber attnum = get_attnum(source_relid, colname);
		if (!AttributeNumberIsValid(attnum))
		{
			pfree(query_str);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" referenced in aggregate does not exist in source "
							"relation",
							colname)));
		}
	}

	/* Now we need to transform the expression to get the Aggref and type info */
	/* Create a ParseState with the source relation */
	ParseState *pstate = make_parsestate(NULL);
	Relation source_rel = table_open(source_relid, AccessShareLock);
	ParseNamespaceItem *nsitem =
		addRangeTableEntryForRelation(pstate, source_rel, AccessShareLock, NULL, false, true);

	/* Add to namespace */
	addNSItemToQuery(pstate, nsitem, true, true, true);

	/* Transform the expression */
	Node *transformed = transformExpr(pstate, res_target->val, EXPR_KIND_SELECT_TARGET);

	/* Close the relation */
	table_close(source_rel, AccessShareLock);

	if (!IsA(transformed, Aggref))
	{
		pfree(query_str);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("expression is not an aggregate function")));
	}

	Aggref *aggref = (Aggref *) transformed;

	/* Build the result structure */
	info = palloc0(sizeof(AggregateExprInfo));
	info->result_type = aggref->aggtype;
	info->result_typmod = -1; /* aggregates typically don't have typmod */
	info->result_collation = aggref->aggcollid;
	info->aggref = aggref;

	/* Determine the column alias */
	if (res_target->name != NULL)
	{
		/* User provided an explicit alias */
		info->column_alias = pstrdup(res_target->name);
	}
	else
	{
		/* Generate alias from function name */
		char *funcname_str = NameListToString(funcname);
		/* Convert to lowercase and use as alias */
		info->column_alias = pstrdup(funcname_str);
		for (char *p = info->column_alias; *p; p++)
			*p = pg_tolower((unsigned char) *p);
	}

	pfree(query_str);
	return info;
}

/*
 * Find the maximum ressortgroupref value in a query's targetList
 */
static Index
get_max_sortgroupref(Query *query)
{
	ListCell *lc;
	Index max_ref = 0;

	foreach (lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->ressortgroupref > max_ref)
			max_ref = tle->ressortgroupref;
	}

	return max_ref;
}

/*
 * Check if a column name already exists in the query's targetList
 */
static bool
column_exists_in_targetlist(Query *query, const char *column_name)
{
	ListCell *lc;

	foreach (lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->resname && strcmp(tle->resname, column_name) == 0)
			return true;
	}

	return false;
}

/*
 * Find the RTE index (varno) for a given relation OID in the query's rtable
 */
static int
find_rte_index_for_relid(Query *query, Oid relid)
{
	ListCell *lc;
	int varno = 1;

	foreach (lc, query->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
		if (rte->rtekind == RTE_RELATION && rte->relid == relid)
			return varno;
		varno++;
	}

	return 0; /* Not found */
}

/*
 * Add a column to a query's targetList and groupClause
 *
 * This function:
 * 1. Creates a Var node for the column
 * 2. Creates a TargetEntry and appends it to the targetList
 * 3. Creates a SortGroupClause and appends it to the groupClause
 */
static void
add_column_to_query(Query *query, int varno, AttrNumber attnum, Oid atttype, int32 atttypmod,
					Oid attcollation, const char *column_name)
{
	/* Create Var node for the column */
	Var *var = makeVar(varno, attnum, atttype, atttypmod, attcollation, 0);

	/* Create TargetEntry */
	TargetEntry *tle = makeTargetEntry((Expr *) var,
									   list_length(query->targetList) + 1,
									   pstrdup(column_name),
									   false); /* not resjunk */
	tle->ressortgroupref = 0;

	/* Add to targetList */
	query->targetList = lappend(query->targetList, tle);

	if (query->groupClause != NIL)
	{
		/* Get next sortgroupref value */
		tle->ressortgroupref = get_max_sortgroupref(query) + 1;

		/* Create SortGroupClause for GROUP BY */
		TypeCacheEntry *tce = lookup_type_cache(atttype, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR);

		SortGroupClause *sgc = makeNode(SortGroupClause);
		sgc->tleSortGroupRef = tle->ressortgroupref;
		sgc->eqop = tce->eq_opr;
		sgc->sortop = tce->lt_opr;
		sgc->nulls_first = false;
		sgc->hashable = op_hashjoinable(tce->eq_opr, atttype);

		/* Add to groupClause */
		query->groupClause = lappend(query->groupClause, sgc);
	}
}

/*
 * Mutator function to adjust varno in Var nodes within an Aggref
 */
typedef struct AdjustVarnoContext
{
	int new_varno;
} AdjustVarnoContext;

static Node *
adjust_varno_mutator(Node *node, AdjustVarnoContext *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		Var *newvar = copyObject(var);
		newvar->varno = context->new_varno;
		return (Node *) newvar;
	}

	return expression_tree_mutator(node, adjust_varno_mutator, context);
}

/*
 * Add an aggregate expression to a query's targetList
 *
 * This function:
 * 1. Copies the Aggref and adjusts varno to match the query's RTE
 * 2. Creates a TargetEntry and appends it to the targetList
 * Note: Aggregates are NOT added to groupClause
 */
static void
add_aggregate_to_query(Query *query, int varno, Aggref *aggref, const char *column_alias)
{
	/* Copy and adjust the Aggref's Var nodes to use the correct varno */
	AdjustVarnoContext ctx = { .new_varno = varno };
	Aggref *new_aggref = (Aggref *) adjust_varno_mutator((Node *) aggref, &ctx);

	/* Create TargetEntry */
	TargetEntry *tle = makeTargetEntry((Expr *) new_aggref,
									   list_length(query->targetList) + 1,
									   pstrdup(column_alias),
									   false); /* not resjunk */
	tle->ressortgroupref = 0;				   /* Aggregates don't go in GROUP BY */

	/* Add to targetList */
	query->targetList = lappend(query->targetList, tle);
}

/*
 * Add a column to a view relation using ALTER VIEW ADD COLUMN
 */
static void
add_column_to_view_relation(Oid view_oid, const char *view_schema, const char *view_name,
							const char *column_name, Oid atttype, int32 atttypmod, Oid attcollation)
{
	int sec_ctx;
	Oid uid, saved_uid;

	/* Create column definition */
	ColumnDef *coldef = makeColumnDef(column_name, atttype, atttypmod, attcollation);

	/* Create ALTER VIEW ADD COLUMN command */
	AlterTableCmd *cmd = makeNode(AlterTableCmd);
	cmd->subtype = AT_AddColumnToView;
	cmd->def = (Node *) coldef;
	cmd->behavior = DROP_RESTRICT;
	cmd->missing_ok = false;

	/* Create AlterTableStmt for the view */
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt,
		.relation = makeRangeVar((char *) view_schema, (char *) view_name, -1),
		.cmds = list_make1(cmd),
		.objtype = OBJECT_VIEW,
		.missing_ok = false,
	};

	/* Execute the ALTER VIEW */
	SWITCH_TO_TS_USER(view_schema, uid, saved_uid, sec_ctx);
	LOCKMODE lockmode = AlterTableGetLockLevel(stmt.cmds);
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&stmt, lockmode),
	};
	AlterTable(&stmt, lockmode, &atcontext);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Update a view's query definition to add a new column or aggregate expression.
 *
 * If aggref is NULL, adds a simple column (with GROUP BY).
 * If aggref is non-NULL, adds an aggregate expression (no GROUP BY).
 */
static void
update_view_add_expr(Oid view_oid, const char *view_schema, const char *view_name,
					 Oid source_relid, AttrNumber attnum, Aggref *aggref, Oid atttype,
					 int32 atttypmod, Oid attcollation, const char *column_name)
{
	int sec_ctx;
	Oid uid, saved_uid;

	/* Step 1: Open the view and get its query BEFORE adding the column */
	Relation view_rel = relation_open(view_oid, AccessShareLock);
	Query *query = copyObject(get_view_query(view_rel));
	relation_close(view_rel, NoLock);

	/* Remove dummy RTEs for PG16+ */
	RemoveRangeTableEntries(query);

	/* Find the varno for the source relation */
	int varno = find_rte_index_for_relid(query, source_relid);
	if (varno == 0)
	{
		/* Source relation not directly in rtable - this can happen with hierarchical CAggs */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add %s to this continuous aggregate",
						aggref ? "aggregate" : "column"),
				 errhint("The source relation is not directly referenced in the view.")));
	}

	/* Add the column or aggregate to the query */
	if (aggref)
		add_aggregate_to_query(query, varno, aggref, column_name);
	else
		add_column_to_query(query, varno, attnum, atttype, atttypmod, attcollation, column_name);

	/* Step 2: Add the column to the view relation */
	add_column_to_view_relation(view_oid,
								view_schema,
								view_name,
								column_name,
								atttype,
								atttypmod,
								attcollation);

	/* Step 3: Store the updated query */
	SWITCH_TO_TS_USER(view_schema, uid, saved_uid, sec_ctx);
	StoreViewQuery(view_oid, query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Add a column to the materialization hypertable
 */
static void
add_column_to_mat_hypertable(Hypertable *mat_ht, const char *column_name, Oid atttype,
							 int32 atttypmod, Oid attcollation)
{
	/* Create column definition */
	ColumnDef *coldef = makeColumnDef(column_name, atttype, atttypmod, attcollation);

	/* Create ALTER TABLE ADD COLUMN command */
	AlterTableCmd *cmd = makeNode(AlterTableCmd);
	cmd->subtype = AT_AddColumn;
	cmd->def = (Node *) coldef;
	cmd->behavior = DROP_RESTRICT;
	cmd->missing_ok = false;

	/* Create AlterTableStmt */
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt,
		.relation =
			makeRangeVar(NameStr(mat_ht->fd.schema_name), NameStr(mat_ht->fd.table_name), -1),
		.cmds = list_make1(cmd),
		.objtype = OBJECT_TABLE,
		.missing_ok = false,
	};

	/* Execute the ALTER TABLE using correct PG18 signature */
	LOCKMODE lockmode = AlterTableGetLockLevel(stmt.cmds);
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&stmt, lockmode),
	};

	AlterTable(&stmt, lockmode, &atcontext);
	CommandCounterIncrement();
}

/*
 * Main function to add a column or aggregate to a continuous aggregate
 */
Datum
continuous_agg_add_column(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_GETARG_OID(0);
	char *expr_str = text_to_cstring(PG_GETARG_TEXT_PP(1));
	bool if_not_exists = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	/* Get the continuous aggregate */
	ContinuousAgg *cagg = cagg_get_by_relid_or_fail(cagg_relid);

	/* Get the raw hypertable */
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *raw_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.raw_hypertable_id);

	if (raw_ht == NULL)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not find raw hypertable for continuous aggregate")));
	}

	/*
	 * Determine the source relation for the partial/direct views:
	 * - For regular CAggs: the raw hypertable
	 * - For hierarchical CAggs: the parent CAgg's user view
	 */
	Oid source_relid = raw_ht->main_table_relid;
	AttrNumber attnum = InvalidAttrNumber;
	Oid atttype;
	int32 atttypmod;
	Oid attcollation;
	const char *column_name;
	ContinuousAgg *parent_cagg = NULL;
	AggregateExprInfo *agg_info = NULL;
	bool is_aggregate = false;

	if (ContinuousAggIsHierarchical(cagg))
	{
		/* Get the parent continuous aggregate */
		parent_cagg =
			ts_continuous_agg_find_by_mat_hypertable_id(cagg->data.parent_mat_hypertable_id, false);

		if (parent_cagg == NULL)
		{
			ts_cache_release(&hcache);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not find parent continuous aggregate")));
		}

		/* Get the parent CAgg's user view OID */
		source_relid = ts_get_relation_relid(NameStr(parent_cagg->data.user_view_schema),
											 NameStr(parent_cagg->data.user_view_name),
											 false);
	}

	/* Parse and validate the aggregate expression */
	agg_info = parse_aggregate_expression(expr_str, source_relid);
	if (agg_info)
	{
		is_aggregate = true;
		column_name = agg_info->column_alias;
		atttype = agg_info->result_type;
		atttypmod = agg_info->result_typmod;
		attcollation = agg_info->result_collation;
	}
	else
	{
		/* Simple column reference */
		column_name = expr_str;

		/* Check if column exists in source relation */
		attnum = get_attnum(source_relid, column_name);
		if (!AttributeNumberIsValid(attnum))
		{
			ts_cache_release(&hcache);
			if (ContinuousAggIsHierarchical(cagg))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist in parent continuous aggregate "
								"\"%s\".\"%s\"",
								column_name,
								NameStr(parent_cagg->data.user_view_schema),
								NameStr(parent_cagg->data.user_view_name))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist in hypertable \"%s\".\"%s\"",
								column_name,
								NameStr(raw_ht->fd.schema_name),
								NameStr(raw_ht->fd.table_name))));
		}

		/* Get column type information from the source relation */
		Relation source_rel = table_open(source_relid, AccessShareLock);
		TupleDesc source_tupdesc = RelationGetDescr(source_rel);
		Form_pg_attribute attr = TupleDescAttr(source_tupdesc, AttrNumberGetAttrOffset(attnum));
		atttype = attr->atttypid;
		atttypmod = attr->atttypmod;
		attcollation = attr->attcollation;
		table_close(source_rel, AccessShareLock);
	}

	/* Get the materialization hypertable */
	Hypertable *mat_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.mat_hypertable_id);
	if (mat_ht == NULL)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find materialization hypertable for continuous aggregate")));
	}

	/* Check if column already exists in the continuous aggregate */
	Oid partial_view_oid = ts_get_relation_relid(NameStr(cagg->data.partial_view_schema),
												 NameStr(cagg->data.partial_view_name),
												 false);

	Relation partial_view_rel = relation_open(partial_view_oid, AccessShareLock);
	Query *partial_query = copyObject(get_view_query(partial_view_rel));
	relation_close(partial_view_rel, NoLock);

	if (column_exists_in_targetlist(partial_query, column_name))
	{
		ts_cache_release(&hcache);
		if (if_not_exists)
		{
			ereport(NOTICE,
					(errmsg("column \"%s\" already exists in continuous aggregate, skipping",
							column_name)));
			PG_RETURN_VOID();
		}
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("column \"%s\" already exists in continuous aggregate", column_name)));
	}

	/* Check if the materialization hypertable has compressed chunks */
	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(mat_ht) ||
		TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add column to continuous aggregate with compression enabled"),
				 errhint("Disable compression on the continuous aggregate first.")));
	}

	/*
	 * Step 1: Add column to materialization hypertable
	 * (for both simple columns and aggregates, we store the result in mat_ht)
	 */
	add_column_to_mat_hypertable(mat_ht, column_name, atttype, atttypmod, attcollation);

	/*
	 * Step 2: Update partial view
	 * The partial view queries the source relation (raw hypertable or parent CAgg's user view)
	 */
	Aggref *aggref = is_aggregate ? agg_info->aggref : NULL;
	update_view_add_expr(partial_view_oid,
						 NameStr(cagg->data.partial_view_schema),
						 NameStr(cagg->data.partial_view_name),
						 source_relid,
						 attnum,
						 aggref,
						 atttype,
						 atttypmod,
						 attcollation,
						 column_name);

	/*
	 * Step 3: Update direct view
	 * The direct view also queries the source relation
	 */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);
	update_view_add_expr(direct_view_oid,
						 NameStr(cagg->data.direct_view_schema),
						 NameStr(cagg->data.direct_view_name),
						 source_relid,
						 attnum,
						 aggref,
						 atttype,
						 atttypmod,
						 attcollation,
						 column_name);

	/*
	 * Step 4: Update user view
	 * The user view queries the materialization hypertable (and possibly raw hypertable in
	 * real-time mode)
	 */
	Oid user_view_oid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											  NameStr(cagg->data.user_view_name),
											  false);

	/* Get the new attnum from the materialization hypertable after adding the column */
	AttrNumber mat_attnum = get_attnum(mat_ht->main_table_relid, column_name);

	Relation user_view_rel = relation_open(user_view_oid, AccessShareLock);
	Query *user_query = copyObject(get_view_query(user_view_rel));
	relation_close(user_view_rel, NoLock);
	RemoveRangeTableEntries(user_query);

	if (user_query->setOperations)
	{
		/*
		 * Real-time mode: UNION ALL query
		 * Need to update both subqueries and the SetOperationStmt
		 */
		RangeTblEntry *mat_rte = linitial(user_query->rtable);
		RangeTblEntry *raw_rte = lsecond(user_query->rtable);

		if (mat_rte->rtekind != RTE_SUBQUERY || raw_rte->rtekind != RTE_SUBQUERY)
		{
			ts_cache_release(&hcache);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected query structure in real-time continuous aggregate")));
		}

		/* Update materialized subquery (queries mat_ht) - always a simple column read
		 * since data is pre-aggregated in the materialization hypertable */
		Query *mat_subquery = mat_rte->subquery;
		int mat_varno = find_rte_index_for_relid(mat_subquery, mat_ht->main_table_relid);
		if (mat_varno > 0)
		{
			add_column_to_query(mat_subquery,
								mat_varno,
								mat_attnum,
								atttype,
								atttypmod,
								attcollation,
								column_name);
			/* Also update the RTE's column names to match the subquery's targetList */
			mat_rte->eref->colnames =
				lappend(mat_rte->eref->colnames, makeString(pstrdup(column_name)));
		}

		/* Update raw subquery (queries source relation)
		 * - For simple columns: add to GROUP BY
		 * - For aggregates: compute the aggregate on the fly */
		Query *raw_subquery = raw_rte->subquery;
		int raw_varno = find_rte_index_for_relid(raw_subquery, source_relid);
		if (raw_varno > 0)
		{
			if (is_aggregate)
			{
				/* Add aggregate expression to raw subquery */
				add_aggregate_to_query(raw_subquery, raw_varno, agg_info->aggref, column_name);
			}
			else
			{
				/* Add simple column to raw subquery with GROUP BY */
				add_column_to_query(raw_subquery,
									raw_varno,
									attnum,
									atttype,
									atttypmod,
									attcollation,
									column_name);
			}
			/* Also update the RTE's column names to match the subquery's targetList */
			raw_rte->eref->colnames =
				lappend(raw_rte->eref->colnames, makeString(pstrdup(column_name)));
		}

		/* Update SetOperationStmt column type lists */
		SetOperationStmt *setop = (SetOperationStmt *) user_query->setOperations;
		setop->colTypes = lappend_int(setop->colTypes, atttype);
		setop->colTypmods = lappend_int(setop->colTypmods, atttypmod);
		setop->colCollations = lappend_int(setop->colCollations, attcollation);

		/* Add column to outer targetList (no GROUP BY for UNION ALL outer query) */
		Var *outer_var = makeVar(1, /* first RTE is always the UNION result */
								 list_length(user_query->targetList) + 1,
								 atttype,
								 atttypmod,
								 attcollation,
								 0);

		TargetEntry *outer_tle = makeTargetEntry((Expr *) outer_var,
												 list_length(user_query->targetList) + 1,
												 pstrdup(column_name),
												 false);
		outer_tle->ressortgroupref = 0;
		user_query->targetList = lappend(user_query->targetList, outer_tle);
	}
	else
	{
		/*
		 * Materialized-only mode: Direct query on mat_ht
		 * No GROUP BY needed since data is pre-aggregated
		 */
		int varno = find_rte_index_for_relid(user_query, mat_ht->main_table_relid);
		if (varno > 0)
		{
			add_column_to_query(user_query,
								varno,
								mat_attnum,
								atttype,
								atttypmod,
								attcollation,
								column_name);
		}
	}

	/* Add the column to the user view relation */
	add_column_to_view_relation(user_view_oid,
								NameStr(cagg->data.user_view_schema),
								NameStr(cagg->data.user_view_name),
								column_name,
								atttype,
								atttypmod,
								attcollation);

	/* Store the updated user view query */
	int sec_ctx;
	Oid uid, saved_uid;
	SWITCH_TO_TS_USER(NameStr(cagg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, user_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);

	ts_cache_release(&hcache);

	PG_RETURN_VOID();
}
