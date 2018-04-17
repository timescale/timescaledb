#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <optimizer/plancat.h>
#include <optimizer/clauses.h>
#include <optimizer/prep.h>
#include <executor/executor.h>
#include <catalog/pg_class.h>
#include <utils/memutils.h>
#include <utils/lsyscache.h>
#include <commands/explain.h>

#include "constraint_aware_append.h"
#include "hypertable.h"
#include "compat.h"

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * This functions tries to reuse as much functionality as possible from standard
 * constraint exclusion in PostgreSQL that normally happens at planning
 * time. Therefore, we need to fake a number of planning-related data
 * structures.
 *
 * We also need to walk the expression trees of the restriction clauses and
 * update any Vars that reference the main table to instead reference the child
 * table (chunk) we want to exclude.
 */
static bool
excluded_by_constraint(RangeTblEntry *rte, AppendRelInfo *appinfo, List *restrictinfos)
{
	ListCell   *lc;
	RelOptInfo	rel = {
		.relid = appinfo->child_relid,
		.reloptkind = RELOPT_OTHER_MEMBER_REL,
		.baserestrictinfo = NIL,
	};
	Query		parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

	foreach(lc, restrictinfos)
	{
		/*
		 * We need a copy to retain the original parent ID in Vars for next
		 * chunk
		 */
		RestrictInfo *old = lfirst(lc);
		RestrictInfo *rinfo = makeNode(RestrictInfo);

		/*
		 * Update Vars to reference the child relation (chunk) instead of the
		 * hypertable main table
		 */
		rinfo->clause = (Expr *) adjust_appendrel_attrs(&root, (Node *) old->clause, appinfo);
		rel.baserestrictinfo = lappend(rel.baserestrictinfo, rinfo);
	}

	return relation_excluded_by_constraints(&root, &rel, rte);
}

/*
 * Convert restriction clauses to constants expressions (i.e., if there are
 * mutable functions, they need to be evaluated to constants).  For instance,
 * something like:
 *
 * ...WHERE time > now - interval '1 hour'
 *
 * becomes
 *
 * ...WHERE time > '2017-06-02 11:26:43.935712+02'
 */
static List *
constify_restrictinfos(List *restrictinfos)
{
	List	   *newinfos = NIL;
	ListCell   *lc;
	Query		parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

	foreach(lc, restrictinfos)
	{
		/* We need a copy to not mess up the plan */
		RestrictInfo *old = lfirst(lc);
		RestrictInfo *rinfo = makeNode(RestrictInfo);

		rinfo->clause = (Expr *) estimate_expression_value(&root, (Node *) old->clause);
		newinfos = lappend(newinfos, rinfo);
	}

	return newinfos;
}

/*
 * Initialize the scan state and prune any subplans from the Append node below
 * us in the plan tree. Pruning happens by evaluating the subplan's table
 * constraints against a folded version of the restriction clauses in the query.
 */
static void
ca_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Plan	   *subplan = copyObject(state->subplan);
	List	   *append_rel_info = lsecond(cscan->custom_private);
	List	   *restrictinfos = constify_restrictinfos(lthird(cscan->custom_private));
	List	  **appendplans,
			   *old_appendplans;
	ListCell   *lc_plan,
			   *lc_info;

	switch (nodeTag(subplan))
	{
		case T_Append:
			{
				Append	   *append = (Append *) subplan;

				old_appendplans = append->appendplans;
				append->appendplans = NIL;
				appendplans = &append->appendplans;
				break;
			}
		case T_MergeAppend:
			{
				MergeAppend *append = (MergeAppend *) subplan;

				old_appendplans = append->mergeplans;
				append->mergeplans = NIL;
				appendplans = &append->mergeplans;
				break;
			}
		case T_Result:

			/*
			 * Append plans are turned into a Result node if empty. This can
			 * happen if children are pruned first by constraint exclusion
			 * while we also remove the main table from the appendplans list,
			 * leaving an empty list. In that case, there is nothing to do.
			 */
			return;
		default:
			elog(ERROR, "Invalid plan %d", nodeTag(subplan));
	}

	forboth(lc_plan, old_appendplans, lc_info, append_rel_info)
	{
		Scan	   *scan = lfirst(lc_plan);
		AppendRelInfo *appinfo = lfirst(lc_info);
		RangeTblEntry *rte = rt_fetch(scan->scanrelid, estate->es_range_table);

		switch (nodeTag(scan))
		{
			case T_SeqScan:
			case T_SampleScan:
			case T_IndexScan:
			case T_IndexOnlyScan:
			case T_BitmapIndexScan:
			case T_BitmapHeapScan:
			case T_TidScan:
			case T_SubqueryScan:
			case T_FunctionScan:
			case T_ValuesScan:
			case T_CteScan:
			case T_WorkTableScan:
			case T_ForeignScan:
			case T_CustomScan:

				/*
				 * If this is a base rel (chunk), check if it can be excluded
				 * from the scan. Otherwise, fall through.
				 */
				if (rte->rtekind == RTE_RELATION &&
					rte->relkind == RELKIND_RELATION &&
					!rte->inh &&
					excluded_by_constraint(rte, appinfo, restrictinfos))
					break;
			default:
				*appendplans = lappend(*appendplans, scan);
		}
	}

	state->num_append_subplans = list_length(*appendplans);
	if (state->num_append_subplans > 0)
		node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

static TupleTableSlot *
ca_append_exec(CustomScanState *node)
{
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	TupleTableSlot *subslot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
#if PG96
	TupleTableSlot *resultslot;
	ExprDoneCond isDone;
#endif

	/*
	 * Check if all append subplans were pruned. In that case there is nothing
	 * to do.
	 */
	if (state->num_append_subplans == 0)
		return NULL;

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	ResetExprContext(econtext);

	while (true)
	{
		subslot = ExecProcNode(linitial(node->custom_ps));

		if (TupIsNull(subslot))
			return NULL;

		if (!node->ss.ps.ps_ProjInfo)
			return subslot;

		econtext->ecxt_scantuple = subslot;

#if PG10
		return ExecProject(node->ss.ps.ps_ProjInfo);
#elif PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#endif
	}
}

static void
ca_append_end(CustomScanState *node)
{
	if (node->custom_ps != NIL)
	{
		ExecEndNode(linitial(node->custom_ps));
	}
}

static void
ca_append_rescan(CustomScanState *node)
{
#if PG96
	node->ss.ps.ps_TupFromTlist = false;
#endif
	if (node->custom_ps != NIL)
	{
		ExecReScan(linitial(node->custom_ps));
	}
}

static void
ca_append_explain(CustomScanState *node,
				  List *ancestors,
				  ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	Oid			relid = linitial_oid(linitial(cscan->custom_private));

	ExplainPropertyText("Hypertable", get_rel_name(relid), es);
	ExplainPropertyInteger("Chunks left after exclusion", state->num_append_subplans, es);
}


static CustomExecMethods constraint_aware_append_state_methods = {
	.BeginCustomScan = ca_append_begin,
	.ExecCustomScan = ca_append_exec,
	.EndCustomScan = ca_append_end,
	.ReScanCustomScan = ca_append_rescan,
	.ExplainCustomScan = ca_append_explain,
};

static Node *
constraint_aware_append_state_create(CustomScan *cscan)
{
	ConstraintAwareAppendState *state;
	Append	   *append = linitial(cscan->custom_plans);

	state = (ConstraintAwareAppendState *) newNode(sizeof(ConstraintAwareAppendState),
												   T_CustomScanState);
	state->csstate.methods = &constraint_aware_append_state_methods;
	state->subplan = &append->plan;

	return (Node *) state;
}

static CustomScanMethods constraint_aware_append_plan_methods = {
	.CustomName = "ConstraintAwareAppend",
	.CreateCustomScanState = constraint_aware_append_state_create,
};

static Plan *
constraint_aware_append_plan_create(PlannerInfo *root,
									RelOptInfo *rel,
									struct CustomPath *path,
									List *tlist,
									List *clauses,
									List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan	   *subplan = linitial(custom_plans);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

	cscan->scan.scanrelid = 0;	/* Not a real relation we are scanning */
	cscan->scan.plan.targetlist = tlist;	/* Target list we expect as output */
	cscan->custom_plans = custom_plans;
	cscan->custom_private = list_make3(list_make1_oid(rte->relid),
									   list_copy(root->append_rel_list),
									   list_copy(clauses));
	cscan->custom_scan_tlist = subplan->targetlist; /* Target list of tuples
													 * we expect as input */
	cscan->flags = path->flags;
	cscan->methods = &constraint_aware_append_plan_methods;

	return &cscan->scan.plan;
}

static CustomPathMethods constraint_aware_append_path_methods = {
	.CustomName = "ConstraintAwareAppend",
	.PlanCustomPath = constraint_aware_append_plan_create,
};

static inline List *
remove_parent_subpath(PlannerInfo *root, List *subpaths, Oid parent_relid)
{
	Path	   *childpath;
	Oid			relid;

	childpath = linitial(subpaths);
	relid = root->simple_rte_array[childpath->parent->relid]->relid;

	if (relid == parent_relid)
		subpaths = list_delete_first(subpaths);

	return subpaths;
}

Path *
constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath)
{
	ConstraintAwareAppendPath *path;
	AppendRelInfo *appinfo;
	Oid			relid;

	path = (ConstraintAwareAppendPath *) newNode(sizeof(ConstraintAwareAppendPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;

	/*
	 * Set flags. We can set CUSTOMPATH_SUPPORT_BACKWARD_SCAN and
	 * CUSTOMPATH_SUPPORT_MARK_RESTORE. The only interesting flag is the first
	 * one (backward scan), but since we are not scanning a real relation we
	 * need not indicate that we support backward scans. Lower-level index
	 * scanning nodes will scan backward if necessary, so once tuples get to
	 * this node they will be in a given order already.
	 */
	path->cpath.flags = 0;
	path->cpath.custom_paths = list_make1(subpath);
	path->cpath.methods = &constraint_aware_append_path_methods;

	/*
	 * Remove the main table from the append_rel_list and Append's subpaths
	 * since it cannot contain any tuples
	 */
	switch (nodeTag(subpath))
	{
		case T_AppendPath:
			{
				AppendPath *append = (AppendPath *) subpath;

				append->subpaths = remove_parent_subpath(root, append->subpaths, ht->main_table_relid);
				break;
			}
		case T_MergeAppendPath:
			{
				MergeAppendPath *append = (MergeAppendPath *) subpath;

				append->subpaths = remove_parent_subpath(root, append->subpaths, ht->main_table_relid);
				break;
			}
		default:
			elog(ERROR, "Invalid node type %u", nodeTag(subpath));
			break;
	}

	if (list_length(root->append_rel_list) > 1)
	{
		appinfo = linitial(root->append_rel_list);
		relid = root->simple_rte_array[appinfo->child_relid]->relid;

		if (relid == ht->main_table_relid)
			root->append_rel_list = list_delete_first(root->append_rel_list);
	}

	return &path->cpath.path;
}
