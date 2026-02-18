/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains the implementation for altering continuous aggregates,
 * specifically adding aggregate expressions to an existing continuous aggregate.
 */

#include <postgres.h>

#include "export.h"

#include <access/htup_details.h>
#include <access/table.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_class.h>
#include <catalog/pg_proc.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <parser/analyze.h>
#include <parser/parse_expr.h>
#include <parser/parse_node.h>
#include <parser/parse_relation.h>
#include <parser/parser.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "alter.h"
#include "common.h"
#include "create.h"
#include "debug_assert.h"
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
	Oid source_relid;	  /* source relation OID used for parsing */
} AggregateExprInfo;

/*
 * Walker function to collect column references from an expression
 */
static bool
collect_column_walker(Node *node, List **column_names)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		ColumnRef *cref = castNode(ColumnRef, node);
		/* Get the column name (last element of fields list) */
		Node *field = llast(cref->fields);
		if (IsA(field, String))
		{
			char *colname = strVal(field);
			/* Add to list if not already present */
			ListCell *lc;
			bool found = false;
			foreach (lc, *column_names)
			{
				if (strcmp(strVal(lfirst(lc)), colname) == 0)
				{
					found = true;
					break;
				}
			}
			if (!found)
				*column_names = lappend(*column_names, makeString(colname));
		}
		return false;
	}

	return raw_expression_tree_walker(node, collect_column_walker, column_names);
}

/*
 * Parse and validate an aggregate expression
 *
 * Returns an AggregateExprInfo structure with parsed information.
 * Errors out if the expression is not a valid aggregate.
 */
static AggregateExprInfo *
parse_aggregate_expression(const char *expr_str, Oid source_relid)
{
	StringInfoData query_str;
	List *raw_parsetree_list;

	/* Build a SELECT statement to parse the expression */
	initStringInfo(&query_str);
	appendStringInfo(&query_str, "SELECT %s", expr_str);

	const MemoryContext oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		/* Parse the query */
		raw_parsetree_list = raw_parser(query_str.data, RAW_PARSE_DEFAULT);
	}
	PG_CATCH();
	{
		/* We do this fandango to avoid exhausting the error stack if we get
		 * anything else but a syntax error, for example, an out of memory
		 * error. */
		ErrorData *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		if (edata->sqlerrcode == ERRCODE_SYNTAX_ERROR)
		{
			edata->cursorpos = edata->internalpos = 0;
			edata->detail = edata->message;
			edata->message = psprintf("unable to parse the aggregate expression \"%s\"", expr_str);
		}
		ReThrowError(edata);
	}
	PG_END_TRY();

	if (list_length(raw_parsetree_list) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid aggregate expression: \"%s\"", expr_str)));

	RawStmt *raw_stmt = linitial_node(RawStmt, raw_parsetree_list);
	Assert(IsA(raw_stmt->stmt, SelectStmt));

	SelectStmt *select_stmt = castNode(SelectStmt, raw_stmt->stmt);
	if (list_length(select_stmt->targetList) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("only one aggregate expression allowed")));

	ResTarget *res_target = linitial_node(ResTarget, select_stmt->targetList);

	/* Check if it's a FuncCall (aggregate functions are parsed as FuncCall initially) */
	if (!IsA(res_target->val, FuncCall))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("expression must be an aggregate function"),
				 errhint("Use syntax like 'sum(column) AS alias' or 'avg(column)'.")));

	FuncCall *func_call = castNode(FuncCall, res_target->val);

	/* Get the number of arguments - special case for count(*) which has agg_star=true */
	int nargs = func_call->agg_star ? 0 : list_length(func_call->args);

	/* Look up the function to check if it's an aggregate */
	List *funcname = func_call->funcname;
	FuncCandidateList clist =
		FuncnameGetCandidates(funcname, nargs, NIL, true, false, false, false);

	if (clist == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function \"%s\" does not exist", NameListToString(funcname))));

	/* Find an aggregate function among candidates */
	Oid funcoid = InvalidOid;
	while (clist)
	{
		HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
		if (HeapTupleIsValid(proc_tuple))
		{
			Form_pg_proc proc_form = (Form_pg_proc) GETSTRUCT(proc_tuple);
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
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an aggregate function", NameListToString(funcname))));

	/* Collect column references from the aggregate arguments */
	List *column_names = NIL;
	collect_column_walker((Node *) func_call->args, &column_names);

	/* Validate that all referenced columns exist in source relation */
	ListCell *lc;
	foreach (lc, column_names)
	{
		char *colname = strVal(lfirst(lc));
		AttrNumber attnum = get_attnum(source_relid, colname);
		if (!AttributeNumberIsValid(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" referenced in aggregate does not exist in source "
							"relation",
							colname)));
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

	/* Clean up */
	table_close(source_rel, NoLock);
	free_parsestate(pstate);

	Assert(IsA(transformed, Aggref));

	Aggref *aggref = castNode(Aggref, transformed);

	/* Build the result structure */
	AggregateExprInfo *info = palloc0(sizeof(AggregateExprInfo));
	info->result_type = aggref->aggtype;
	info->result_typmod = -1; /* aggregates typically don't have typmod */
	info->result_collation = aggref->aggcollid;
	info->aggref = aggref;
	info->source_relid = source_relid;

	/* Determine the column alias */
	if (res_target->name != NULL)
	{
		/* User provided an explicit alias */
		info->column_alias = res_target->name;
	}
	else
	{
		/* Generate alias from function name (use last element to skip schema qualification) */
		info->column_alias = strVal(llast(func_call->funcname));
	}

	return info;
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
 * Mutator function to adjust varno in Var nodes within an expression
 */
static Node *
adjust_varno_mutator(Node *node, int *new_varno)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = copyObject(castNode(Var, node));
		var->varno = *new_varno;
		return (Node *) var;
	}

	return expression_tree_mutator(node, adjust_varno_mutator, new_varno);
}

/*
 * Add an expression to a query's targetList, optionally adjusting Var nodes.
 *
 * If relid is valid, finds the varno for relid in the query's rtable and adjusts
 * all Var nodes in the expression to use that varno. The input expression should
 * use varno=0 as a placeholder in this case.
 *
 * If relid is InvalidOid, adds the expression as-is without adjustment.
 */
static void
add_expr_to_query(Query *query, Oid relid, Expr *expr, char *column_name)
{
	if (OidIsValid(relid))
	{
		int varno = find_rte_index_for_relid(query, relid);
		Assert(varno != 0);
		expr = (Expr *) adjust_varno_mutator((Node *) expr, &varno);
	}

	TargetEntry *tle = makeTargetEntry(expr,
									   list_length(query->targetList) + 1,
									   column_name,
									   false); /* not resjunk */
	tle->ressortgroupref = 0;

	query->targetList = lappend(query->targetList, tle);
}

/*
 * Add a column to a relation (table or view) using ALTER TABLE/VIEW ADD COLUMN.
 *
 * For views, switches to the TimescaleDB internal user for the operation.
 */
static void
add_column_to_relation(char *schema_name, char *rel_name, AggregateExprInfo *agg_info, bool is_view)
{
	int sec_ctx;
	Oid uid, saved_uid;

	/* Create column definition */
	ColumnDef *coldef = makeColumnDef(agg_info->column_alias,
									  agg_info->result_type,
									  agg_info->result_typmod,
									  agg_info->result_collation);

	/* Create ALTER TABLE/VIEW ADD COLUMN command */
	AlterTableCmd *cmd = makeNode(AlterTableCmd);
	cmd->subtype = is_view ? AT_AddColumnToView : AT_AddColumn;
	cmd->def = (Node *) coldef;
	cmd->behavior = DROP_RESTRICT;
	cmd->missing_ok = false;

	AlterTableStmt stmt = {
		.type = T_AlterTableStmt,
		.relation = makeRangeVar(schema_name, rel_name, -1),
		.cmds = list_make1(cmd),
		.objtype = is_view ? OBJECT_VIEW : OBJECT_TABLE,
		.missing_ok = false,
	};

	if (is_view)
		SWITCH_TO_TS_USER(schema_name, uid, saved_uid, sec_ctx);

	LOCKMODE lockmode = AlterTableGetLockLevel(stmt.cmds);
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&stmt, lockmode),
	};
	AlterTable(&stmt, lockmode, &atcontext);
	CommandCounterIncrement();

	if (is_view)
		RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Update a view's query definition to add a new aggregate expression.
 */
static void
update_view_add_aggregate(Oid view_oid, char *view_schema, char *view_name,
						  AggregateExprInfo *agg_info)
{
	int sec_ctx;
	Oid uid, saved_uid;

	/* Step 1: Get the view's query BEFORE adding the column */
	Query *query = get_view_query_tree(view_oid);

	/* Remove dummy RTEs for PG16+ */
	RemoveRangeTableEntries(query);

	/* Add the aggregate to the query */
	add_expr_to_query(query,
					  agg_info->source_relid,
					  (Expr *) agg_info->aggref,
					  agg_info->column_alias);

	/* Step 2: Add the column to the view relation */
	add_column_to_relation(view_schema, view_name, agg_info, true);

	/* Step 3: Store the updated query */
	SWITCH_TO_TS_USER(view_schema, uid, saved_uid, sec_ctx);
	StoreViewQuery(view_oid, query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Update the user view to include a new column.
 *
 * Handles both real-time mode (UNION ALL query with materialized + raw subqueries)
 * and materialized-only mode (direct query on mat_ht).
 */
static void
update_user_view_add_column(ContinuousAgg *cagg, Hypertable *mat_ht, AggregateExprInfo *agg_info)
{
	int sec_ctx;
	Oid uid, saved_uid;

	Oid user_view_oid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											  NameStr(cagg->data.user_view_name),
											  false);

	/* Get the new attnum from the materialization hypertable after adding the column */
	AttrNumber mat_attnum = get_attnum(mat_ht->main_table_relid, agg_info->column_alias);

	Query *user_query = get_view_query_tree(user_view_oid);
	RemoveRangeTableEntries(user_query);

	if (user_query->setOperations)
	{
		/*
		 * Real-time mode: UNION ALL query
		 * Need to update both subqueries and the SetOperationStmt
		 */
		Assert(list_length(user_query->rtable) == 2);
		RangeTblEntry *mat_rte = linitial(user_query->rtable);
		RangeTblEntry *raw_rte = lsecond(user_query->rtable);

		if (mat_rte->rtekind != RTE_SUBQUERY || raw_rte->rtekind != RTE_SUBQUERY)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected query structure in real-time continuous aggregate")));

		/* Update materialized subquery (queries mat_ht) - always a simple column read
		 * since data is pre-aggregated in the materialization hypertable */
		Expr *mat_var = (Expr *) makeVar(0,
										 mat_attnum,
										 agg_info->result_type,
										 agg_info->result_typmod,
										 agg_info->result_collation,
										 0);
		add_expr_to_query(mat_rte->subquery,
						  mat_ht->main_table_relid,
						  mat_var,
						  agg_info->column_alias);
		mat_rte->eref->colnames =
			lappend(mat_rte->eref->colnames, makeString(agg_info->column_alias));

		/* Update raw subquery (queries source relation) - compute the aggregate on the fly */
		add_expr_to_query(raw_rte->subquery,
						  agg_info->source_relid,
						  (Expr *) agg_info->aggref,
						  agg_info->column_alias);
		raw_rte->eref->colnames =
			lappend(raw_rte->eref->colnames, makeString(agg_info->column_alias));

		/* Update SetOperationStmt column type lists */
		SetOperationStmt *setop = castNode(SetOperationStmt, user_query->setOperations);
		setop->colTypes = lappend_int(setop->colTypes, agg_info->result_type);
		setop->colTypmods = lappend_int(setop->colTypmods, agg_info->result_typmod);
		setop->colCollations = lappend_int(setop->colCollations, agg_info->result_collation);

		/* Add column to outer targetList (no GROUP BY for UNION ALL outer query) */
		Expr *outer_var = (Expr *) makeVar(1, /* first RTE is always the UNION result */
										   list_length(user_query->targetList) + 1,
										   agg_info->result_type,
										   agg_info->result_typmod,
										   agg_info->result_collation,
										   0);
		add_expr_to_query(user_query, InvalidOid, outer_var, agg_info->column_alias);
	}
	else
	{
		/*
		 * Materialized-only mode: Direct query on mat_ht
		 * No GROUP BY needed since data is pre-aggregated
		 */
		Expr *var = (Expr *) makeVar(0,
									 mat_attnum,
									 agg_info->result_type,
									 agg_info->result_typmod,
									 agg_info->result_collation,
									 0);
		add_expr_to_query(user_query, mat_ht->main_table_relid, var, agg_info->column_alias);
	}

	/* Add the column to the user view relation */
	add_column_to_relation(NameStr(cagg->data.user_view_schema),
						   NameStr(cagg->data.user_view_name),
						   agg_info,
						   true);

	/* Store the updated user view query */
	SWITCH_TO_TS_USER(NameStr(cagg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, user_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Remove an expression from a query's targetList by column name.
 *
 * Returns the 1-based position (resno) of the removed entry, which is needed
 * for removing corresponding items from SetOperationStmt lists and
 * eref->colnames in the user view.
 */
static int
remove_expr_from_query(Query *query, const char *column_name)
{
	ListCell *lc;
	int found_resno = 0;

	foreach (lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->resname && strcmp(tle->resname, column_name) == 0)
		{
			found_resno = tle->resno;
			query->targetList = foreach_delete_current(query->targetList, lc);
			break;
		}
	}

	if (found_resno == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in continuous aggregate", column_name)));

	/* Renumber remaining entries */
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->resno > found_resno)
			tle->resno--;
	}

	return found_resno;
}

/*
 * Delete a column's pg_attribute entry from a view and renumber remaining
 * attributes. This is needed because PostgreSQL does not support
 * AT_DropColumn on views, and RemoveAttributeById (which marks columns as
 * dropped) is incompatible with StoreViewQuery's checkViewColumns which
 * rejects views containing dropped columns.
 *
 * This function physically deletes the pg_attribute row and renumbers
 * subsequent attributes so the view's TupleDesc stays contiguous.
 */
static void
delete_view_column(Oid view_oid, const char *column_name)
{
	AttrNumber target_attnum = get_attnum(view_oid, column_name);

	if (!AttributeNumberIsValid(target_attnum))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist", column_name)));

	/* Lock the view relation before modifying its catalog entries */
	LockRelationOid(view_oid, AccessExclusiveLock);

	/* Open pg_attribute for modification */
	Relation attrel = table_open(AttributeRelationId, RowExclusiveLock);

	/* Delete the target attribute tuple */
	HeapTuple tuple =
		SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(view_oid), Int16GetDatum(target_attnum));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in pg_attribute", column_name)));

	CatalogTupleDelete(attrel, &tuple->t_self);
	heap_freetuple(tuple);

	/* Get the current number of attributes from pg_class */
	Relation classrel = table_open(RelationRelationId, RowExclusiveLock);
	HeapTuple classtuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(view_oid));

	if (!HeapTupleIsValid(classtuple))
		elog(ERROR, "cache lookup failed for relation %u", view_oid);

	Form_pg_class classform = (Form_pg_class) GETSTRUCT(classtuple);
	int16 old_natts = classform->relnatts;

	/* Renumber remaining attributes with attnum > target_attnum */
	for (AttrNumber attnum = target_attnum + 1; attnum <= old_natts; attnum++)
	{
		HeapTuple atttuple =
			SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(view_oid), Int16GetDatum(attnum));
		if (HeapTupleIsValid(atttuple))
		{
			Form_pg_attribute attform = (Form_pg_attribute) GETSTRUCT(atttuple);
			attform->attnum = attnum - 1;
			CatalogTupleUpdate(attrel, &atttuple->t_self, atttuple);
			heap_freetuple(atttuple);
		}
	}

	/* Update pg_class.relnatts */
	classform->relnatts = old_natts - 1;
	CatalogTupleUpdate(classrel, &classtuple->t_self, classtuple);
	heap_freetuple(classtuple);

	table_close(classrel, RowExclusiveLock);
	table_close(attrel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Drop a column from a relation (table or view).
 *
 * For tables, uses ALTER TABLE DROP COLUMN via AlterTable.
 * For views, directly modifies pg_attribute since PostgreSQL does not
 * support AT_DropColumn on views.
 */
static void
drop_column_from_relation(char *schema_name, char *rel_name, char *column_name, bool is_view)
{
	int sec_ctx;
	Oid uid, saved_uid;

	if (is_view)
	{
		SWITCH_TO_TS_USER(schema_name, uid, saved_uid, sec_ctx);

		Oid view_oid = ts_get_relation_relid(schema_name, rel_name, false);
		delete_view_column(view_oid, column_name);

		RESTORE_USER(uid, saved_uid, sec_ctx);
	}
	else
	{
		/* For tables, use standard ALTER TABLE DROP COLUMN */
		AlterTableCmd *cmd = makeNode(AlterTableCmd);
		cmd->subtype = AT_DropColumn;
		cmd->name = column_name;
		cmd->behavior = DROP_RESTRICT;
		cmd->missing_ok = false;

		AlterTableStmt stmt = {
			.type = T_AlterTableStmt,
			.relation = makeRangeVar(schema_name, rel_name, -1),
			.cmds = list_make1(cmd),
			.objtype = OBJECT_TABLE,
			.missing_ok = false,
		};

		LOCKMODE lockmode = AlterTableGetLockLevel(stmt.cmds);
		AlterTableUtilityContext atcontext = {
			.relid = AlterTableLookupRelation(&stmt, lockmode),
		};
		AlterTable(&stmt, lockmode, &atcontext);
		CommandCounterIncrement();
	}
}

/*
 * Update a view's query definition to remove an aggregate expression.
 * Reverse of update_view_add_aggregate().
 */
static void
update_view_drop_aggregate(Oid view_oid, char *view_schema, char *view_name, char *column_name)
{
	int sec_ctx;
	Oid uid, saved_uid;

	/* Step 1: Get the view's query BEFORE dropping the column */
	Query *query = get_view_query_tree(view_oid);

	/* Remove dummy RTEs for PG16+ */
	RemoveRangeTableEntries(query);

	/* Remove the expression from the query */
	remove_expr_from_query(query, column_name);

	/* Step 2: Drop the column from the view relation */
	drop_column_from_relation(view_schema, view_name, column_name, true);

	/* Step 3: Store the updated query */
	SWITCH_TO_TS_USER(view_schema, uid, saved_uid, sec_ctx);
	StoreViewQuery(view_oid, query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Update the user view to remove a column.
 * Reverse of update_user_view_add_column().
 *
 * Handles both real-time mode (UNION ALL query with materialized + raw subqueries)
 * and materialized-only mode (direct query on mat_ht).
 */
static void
update_user_view_drop_column(ContinuousAgg *cagg, char *column_name)
{
	int sec_ctx;
	Oid uid, saved_uid;

	Oid user_view_oid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											  NameStr(cagg->data.user_view_name),
											  false);

	Query *user_query = get_view_query_tree(user_view_oid);
	RemoveRangeTableEntries(user_query);

	if (user_query->setOperations)
	{
		/*
		 * Real-time mode: UNION ALL query
		 * Need to update both subqueries and the SetOperationStmt
		 */
		Assert(list_length(user_query->rtable) == 2);
		RangeTblEntry *mat_rte = linitial(user_query->rtable);
		RangeTblEntry *raw_rte = lsecond(user_query->rtable);

		if (mat_rte->rtekind != RTE_SUBQUERY || raw_rte->rtekind != RTE_SUBQUERY)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected query structure in real-time continuous aggregate")));

		/* Remove from materialized subquery - returns the position index */
		int pos = remove_expr_from_query(mat_rte->subquery, column_name);

		/* Remove from mat_rte eref->colnames at same position (0-based) */
		mat_rte->eref->colnames = list_delete_nth_cell(mat_rte->eref->colnames, pos - 1);

		/* Remove from raw subquery */
		remove_expr_from_query(raw_rte->subquery, column_name);

		/* Remove from raw_rte eref->colnames */
		raw_rte->eref->colnames = list_delete_nth_cell(raw_rte->eref->colnames, pos - 1);

		/* Remove from SetOperationStmt column type lists */
		SetOperationStmt *setop = castNode(SetOperationStmt, user_query->setOperations);
		setop->colTypes = list_delete_nth_cell(setop->colTypes, pos - 1);
		setop->colTypmods = list_delete_nth_cell(setop->colTypmods, pos - 1);
		setop->colCollations = list_delete_nth_cell(setop->colCollations, pos - 1);

		/* Remove from outer targetList */
		remove_expr_from_query(user_query, column_name);

		/* Fix up Var varattno references in remaining outer entries */
		ListCell *lc;
		foreach (lc, user_query->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			if (IsA(tle->expr, Var))
			{
				Var *var = castNode(Var, tle->expr);
				if (var->varattno > pos)
					var->varattno--;
			}
		}
	}
	else
	{
		/*
		 * Materialized-only mode: Direct query on mat_ht
		 */
		remove_expr_from_query(user_query, column_name);
	}

	/* Drop the column from the user view relation */
	drop_column_from_relation(NameStr(cagg->data.user_view_schema),
							  NameStr(cagg->data.user_view_name),
							  column_name,
							  true);

	/* Store the updated user view query */
	SWITCH_TO_TS_USER(NameStr(cagg->data.user_view_schema), uid, saved_uid, sec_ctx);
	StoreViewQuery(user_view_oid, user_query, true);
	CommandCounterIncrement();
	RESTORE_USER(uid, saved_uid, sec_ctx);
}

/*
 * Main function to add an aggregate expression to a continuous aggregate
 */
Datum
continuous_agg_add_column(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_GETARG_OID(0);
	char *expr_str = text_to_cstring(PG_GETARG_TEXT_PP(1));
	bool if_not_exists = PG_GETARG_BOOL(2);

	/* Only the owner of the continuous aggregate can alter it */
	ts_cagg_permissions_check(cagg_relid, GetUserId());

	/*
	 * Take an AccessExclusiveLock on the continuous aggregate relation to
	 * prevent concurrent modifications. This ensures that two concurrent
	 * add_continuous_aggregate_column calls are properly serialized.
	 */
	LockRelationOid(cagg_relid, AccessExclusiveLock);

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

	/* Get the materialization hypertable */
	Hypertable *mat_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.mat_hypertable_id);
	if (mat_ht == NULL)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find materialization hypertable for continuous aggregate")));
	}

	/* Check if the materialization hypertable has compressed chunks */
	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(mat_ht) ||
		TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add aggregate to continuous aggregate with compression enabled"),
				 errhint("Disable compression on the continuous aggregate first.")));
	}

	/*
	 * Determine the source relation for the partial/direct views:
	 * - For regular CAggs: the raw hypertable
	 * - For hierarchical CAggs: the parent CAgg's user view
	 */
	Oid source_relid = raw_ht->main_table_relid;

	if (ContinuousAggIsHierarchical(cagg))
	{
		/* Get the parent continuous aggregate */
		ContinuousAgg *parent_cagg =
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
	AggregateExprInfo *agg_info = parse_aggregate_expression(expr_str, source_relid);

	/* Check if column already exists in the continuous aggregate */
	Oid partial_view_oid = ts_get_relation_relid(NameStr(cagg->data.partial_view_schema),
												 NameStr(cagg->data.partial_view_name),
												 false);

	Query *partial_query = get_view_query_tree(partial_view_oid);

	if (column_exists_in_targetlist(partial_query, agg_info->column_alias))
	{
		ts_cache_release(&hcache);
		if (if_not_exists)
		{
			ereport(NOTICE,
					(errmsg("column \"%s\" already exists in continuous aggregate, skipping",
							agg_info->column_alias)));
			PG_RETURN_VOID();
		}
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_COLUMN),
				 errmsg("column \"%s\" already exists in continuous aggregate",
						agg_info->column_alias)));
	}

	/*
	 * Step 1: Add column to materialization hypertable
	 * (aggregates are stored as computed values in mat_ht)
	 */
	add_column_to_relation(NameStr(mat_ht->fd.schema_name),
						   NameStr(mat_ht->fd.table_name),
						   agg_info,
						   false);

	/*
	 * Step 2: Update partial view
	 * The partial view queries the source relation (raw hypertable or parent CAgg's user view)
	 */
	update_view_add_aggregate(partial_view_oid,
							  NameStr(cagg->data.partial_view_schema),
							  NameStr(cagg->data.partial_view_name),
							  agg_info);

	/*
	 * Step 3: Update direct view
	 * The direct view also queries the source relation
	 */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);
	update_view_add_aggregate(direct_view_oid,
							  NameStr(cagg->data.direct_view_schema),
							  NameStr(cagg->data.direct_view_name),
							  agg_info);

	/*
	 * Step 4: Update user view
	 */
	update_user_view_add_column(cagg, mat_ht, agg_info);

	ts_cache_release(&hcache);

	PG_RETURN_VOID();
}

/*
 * Main function to drop an aggregate column from a continuous aggregate.
 * Reverse of continuous_agg_add_column().
 *
 * Drop order is reversed from add order (user view first, mat_ht last)
 * to avoid referencing a dropped column.
 */
Datum
continuous_agg_drop_column(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_GETARG_OID(0);
	char *column_name = NameStr(*PG_GETARG_NAME(1));
	bool if_exists = PG_GETARG_BOOL(2);

	/* Only the owner of the continuous aggregate can alter it */
	ts_cagg_permissions_check(cagg_relid, GetUserId());

	/*
	 * Take an AccessExclusiveLock on the continuous aggregate relation to
	 * prevent concurrent modifications. This ensures that concurrent
	 * add/drop_continuous_aggregate_column calls are properly serialized.
	 */
	LockRelationOid(cagg_relid, AccessExclusiveLock);

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

	/* Get the materialization hypertable */
	Hypertable *mat_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.mat_hypertable_id);
	if (mat_ht == NULL)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find materialization hypertable for continuous aggregate")));
	}

	/* Check if the materialization hypertable has compressed chunks */
	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(mat_ht) ||
		TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop aggregate from continuous aggregate with compression enabled"),
				 errhint("Disable compression on the continuous aggregate first.")));
	}

	/* Get the partial view to validate the column */
	Oid partial_view_oid = ts_get_relation_relid(NameStr(cagg->data.partial_view_schema),
												 NameStr(cagg->data.partial_view_name),
												 false);

	Query *partial_query = get_view_query_tree(partial_view_oid);

	/* Validate column exists in partial view targetList */
	TargetEntry *found_tle = NULL;
	ListCell *lc;
	foreach (lc, partial_query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->resname && strcmp(tle->resname, column_name) == 0)
		{
			found_tle = tle;
			break;
		}
	}

	if (found_tle == NULL)
	{
		ts_cache_release(&hcache);
		if (if_exists)
		{
			ereport(NOTICE,
					(errmsg("column \"%s\" does not exist in continuous aggregate, skipping",
							column_name)));
			PG_RETURN_VOID();
		}
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in continuous aggregate", column_name)));
	}

	/* Validate column is NOT a GROUP BY column (ressortgroupref > 0 means GROUP BY) */
	if (found_tle->ressortgroupref > 0)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop GROUP BY column \"%s\" from continuous aggregate",
						column_name),
				 errhint("Only aggregate columns can be dropped.")));
	}

	/*
	 * Drop order is reversed from add order to avoid referencing a dropped column.
	 *
	 * Step 1: Update user view (drop column from user-facing view)
	 */
	update_user_view_drop_column(cagg, column_name);

	/*
	 * Step 2: Update direct view
	 */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);
	update_view_drop_aggregate(direct_view_oid,
							   NameStr(cagg->data.direct_view_schema),
							   NameStr(cagg->data.direct_view_name),
							   column_name);

	/*
	 * Step 3: Update partial view
	 */
	update_view_drop_aggregate(partial_view_oid,
							   NameStr(cagg->data.partial_view_schema),
							   NameStr(cagg->data.partial_view_name),
							   column_name);

	/*
	 * Step 4: Drop column from materialization hypertable
	 */
	drop_column_from_relation(NameStr(mat_ht->fd.schema_name),
							  NameStr(mat_ht->fd.table_name),
							  column_name,
							  false);

	ts_cache_release(&hcache);

	PG_RETURN_VOID();
}
