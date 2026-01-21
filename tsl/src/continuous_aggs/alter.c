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
#include <catalog/pg_type.h>
#include <commands/tablecmds.h>
#include <commands/view.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <optimizer/optimizer.h>
#include <parser/parse_relation.h>
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
 * Add a column to a query's targetList only (no GROUP BY)
 *
 * This function:
 * 1. Creates a Var node for the column
 * 2. Creates a TargetEntry and appends it to the targetList
 */
static void
add_column_to_targetlist_only(Query *query, int varno, AttrNumber attnum, Oid atttype,
							  int32 atttypmod, Oid attcollation, const char *column_name)
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

	/* Get next sortgroupref value */
	Index next_ref = get_max_sortgroupref(query) + 1;

	/* Create TargetEntry */
	TargetEntry *tle = makeTargetEntry((Expr *) var,
									   list_length(query->targetList) + 1,
									   pstrdup(column_name),
									   false); /* not resjunk */
	tle->ressortgroupref = next_ref;

	/* Add to targetList */
	query->targetList = lappend(query->targetList, tle);

	/* Create SortGroupClause for GROUP BY */
	TypeCacheEntry *tce = lookup_type_cache(atttype, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR);

	SortGroupClause *sgc = makeNode(SortGroupClause);
	sgc->tleSortGroupRef = next_ref;
	sgc->eqop = tce->eq_opr;
	sgc->sortop = tce->lt_opr;
	sgc->nulls_first = false;
	sgc->hashable = op_hashjoinable(tce->eq_opr, atttype);

	/* Add to groupClause */
	query->groupClause = lappend(query->groupClause, sgc);
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
 * Update a view's query definition to add a new column
 */
static void
update_view_add_column(Oid view_oid, const char *view_schema, const char *view_name,
					   Oid source_relid, AttrNumber attnum, Oid atttype, int32 atttypmod,
					   Oid attcollation, const char *column_name)
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
				 errmsg("cannot add column to this continuous aggregate"),
				 errhint("The source relation is not directly referenced in the view.")));
	}

	/* Add the column to the query */
	add_column_to_query(query, varno, attnum, atttype, atttypmod, attcollation, column_name);

	/* Step 2: Add the column to the view relation */
	add_column_to_view_relation(view_oid, view_schema, view_name, column_name, atttype, atttypmod,
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
 * Main function to add a column to a continuous aggregate
 */
TS_FUNCTION_INFO_V1(continuous_agg_add_column);

Datum
continuous_agg_add_column(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_GETARG_OID(0);
	Name column_name_arg = PG_GETARG_NAME(1);
	bool if_not_exists = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);

	const char *column_name = NameStr(*column_name_arg);

	/* Get the continuous aggregate */
	ContinuousAgg *cagg = cagg_get_by_relid_or_fail(cagg_relid);

	/* Get the raw hypertable */
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *raw_ht = ts_hypertable_cache_get_entry_by_id(hcache, cagg->data.raw_hypertable_id);

	if (raw_ht == NULL)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find raw hypertable for continuous aggregate")));
	}

	/* Check if column exists in raw hypertable */
	AttrNumber attnum = get_attnum(raw_ht->main_table_relid, column_name);
	if (attnum == InvalidAttrNumber)
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" does not exist in hypertable \"%s\".\"%s\"",
						column_name,
						NameStr(raw_ht->fd.schema_name),
						NameStr(raw_ht->fd.table_name))));
	}

	/* Get column type information by opening the relation and getting the attribute */
	Relation raw_rel = table_open(raw_ht->main_table_relid, AccessShareLock);
	TupleDesc raw_tupdesc = RelationGetDescr(raw_rel);
	Form_pg_attribute attr = TupleDescAttr(raw_tupdesc, attnum - 1); /* attnum is 1-based */
	Oid atttype = attr->atttypid;
	int32 atttypmod = attr->atttypmod;
	Oid attcollation = attr->attcollation;
	table_close(raw_rel, AccessShareLock);

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
	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(mat_ht) || TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(mat_ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add column to continuous aggregate with compression enabled"),
				 errhint("Disable compression on the continuous aggregate first.")));
	}

	/*
	 * Step 1: Add column to materialization hypertable
	 */
	add_column_to_mat_hypertable(mat_ht, column_name, atttype, atttypmod, attcollation);

	/*
	 * Step 2: Update partial view
	 * The partial view queries the raw hypertable
	 */
	update_view_add_column(partial_view_oid,
						   NameStr(cagg->data.partial_view_schema),
						   NameStr(cagg->data.partial_view_name),
						   raw_ht->main_table_relid,
						   attnum,
						   atttype,
						   atttypmod,
						   attcollation,
						   column_name);

	/*
	 * Step 3: Update direct view
	 * The direct view also queries the raw hypertable
	 */
	Oid direct_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												false);
	update_view_add_column(direct_view_oid,
						   NameStr(cagg->data.direct_view_schema),
						   NameStr(cagg->data.direct_view_name),
						   raw_ht->main_table_relid,
						   attnum,
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

		/* Update materialized subquery (queries mat_ht) - no GROUP BY needed since data is
		 * pre-aggregated */
		Query *mat_subquery = mat_rte->subquery;
		int mat_varno = find_rte_index_for_relid(mat_subquery, mat_ht->main_table_relid);
		if (mat_varno > 0)
		{
			add_column_to_targetlist_only(mat_subquery,
										  mat_varno,
										  mat_attnum,
										  atttype,
										  atttypmod,
										  attcollation,
										  column_name);
		}

		/* Update raw subquery (queries raw_ht) - needs GROUP BY for aggregation */
		Query *raw_subquery = raw_rte->subquery;
		int raw_varno = find_rte_index_for_relid(raw_subquery, raw_ht->main_table_relid);
		if (raw_varno > 0)
		{
			add_column_to_query(raw_subquery,
								raw_varno,
								attnum,
								atttype,
								atttypmod,
								attcollation,
								column_name);
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
			add_column_to_targetlist_only(user_query,
										  varno,
										  mat_attnum,
										  atttype,
										  atttypmod,
										  attcollation,
										  column_name);
		}
	}

	/* First add the column to the user view relation */
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
