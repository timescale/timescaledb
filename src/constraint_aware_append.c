/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_class.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <optimizer/plancat.h>
#include <optimizer/cost.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/syscache.h>

#include "compat.h"
#if PG12_LT
#include <optimizer/clauses.h>
#include <optimizer/prep.h>
#else
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#endif

#include "constraint_aware_append.h"
#include "hypertable.h"
#include "chunk_append/transform.h"
#include "guc.h"

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * This functions tries to reuse as much functionality as possible from standard
 * constraint exclusion in PostgreSQL that normally happens at planning
 * time. Therefore, we need to fake a number of planning-related data
 * structures.
 */
static bool
excluded_by_constraint(PlannerInfo *root, RangeTblEntry *rte, Index rt_index, List *restrictinfos)
{
	RelOptInfo rel = {
		.type = T_RelOptInfo,
		.relid = rt_index,
		.reloptkind = RELOPT_OTHER_MEMBER_REL,
		.baserestrictinfo = restrictinfos,
	};

	return relation_excluded_by_constraints(root, &rel, rte);
}

static Plan *
get_plans_for_exclusion(Plan *plan)
{
	/* Optimization: If we want to be able to prune */
	/* when the node is a T_Result or T_Sort, then we need to peek */
	/* into the subplans of this Result node. */

	switch (nodeTag(plan))
	{
		case T_Result:
		case T_Sort:
			Assert(plan->lefttree != NULL && plan->righttree == NULL);
			return plan->lefttree;

		default:
			return plan;
	}
}

static bool
can_exclude_chunk(PlannerInfo *root, EState *estate, Index rt_index, List *restrictinfos)
{
	RangeTblEntry *rte = rt_fetch(rt_index, estate->es_range_table);

	return rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_RELATION && !rte->inh &&
		   excluded_by_constraint(root, rte, rt_index, restrictinfos);
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
constify_restrictinfos(PlannerInfo *root, List *restrictinfos)
{
	ListCell *lc;

	foreach (lc, restrictinfos)
	{
		RestrictInfo *rinfo = lfirst(lc);

		rinfo->clause = (Expr *) estimate_expression_value(root, (Node *) rinfo->clause);
	}

	return restrictinfos;
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
	Plan *subplan = copyObject(state->subplan);
	List *chunk_ri_clauses = lsecond(cscan->custom_private);
	List *chunk_relids = lthird(cscan->custom_private);
	List **appendplans, *old_appendplans;
	ListCell *lc_plan;
	ListCell *lc_clauses;
	ListCell *lc_relid;

	/*
	 * create skeleton plannerinfo to reuse some PostgreSQL planner functions
	 */
	Query parse = {
		.resultRelation = InvalidOid,
	};
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
		.parse = &parse,
	};

#if PG12_GE
	/* CustomScan hard-codes the scan and result tuple slot to a fixed
	 * TTSOpsVirtual ops (meaning it expects the slot ops of the child tuple to
	 * also have this type). Oddly, when reading slots from subscan nodes
	 * (children), there is no knowing what tuple slot ops the child slot will
	 * have (e.g., for ChunkAppend it is common that the child is a
	 * seqscan/indexscan that produces a TTSOpsBufferHeapTuple
	 * slot). Unfortunately, any mismatch between slot types when projecting is
	 * asserted by PostgreSQL. To avoid this issue, we mark the scanops as
	 * non-fixed and reinitialize the projection state with this new setting.
	 *
	 * Alternatively, we could copy the child tuple into the scan slot to get
	 * the expected ops before projection, but this would require materializing
	 * and copying the tuple unnecessarily.
	 */
	node->ss.ps.scanopsfixed = false;

	/* Since we sometimes return the scan slot directly from the subnode, the
	 * result slot is not fixed either. */
	node->ss.ps.resultopsfixed = false;
	ExecAssignScanProjectionInfoWithVarno(&node->ss, INDEX_VAR);
#endif

	switch (nodeTag(subplan))
	{
		case T_Append:
		{
			Append *append = (Append *) subplan;

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
			elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(subplan));
	}

	/*
	 * clauses should always have the same length as appendplans because
	 * thats the base for building the lists
	 */
	Assert(list_length(old_appendplans) == list_length(chunk_ri_clauses));
	Assert(list_length(chunk_relids) == list_length(chunk_ri_clauses));

	forthree (lc_plan, old_appendplans, lc_clauses, chunk_ri_clauses, lc_relid, chunk_relids)
	{
		Plan *plan = get_plans_for_exclusion(lfirst(lc_plan));

		switch (nodeTag(plan))
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
			{
				/*
				 * If this is a base rel (chunk), check if it can be
				 * excluded from the scan. Otherwise, fall through.
				 */

				Index scanrelid = ((Scan *) plan)->scanrelid;
				List *restrictinfos = NIL;
				List *ri_clauses = lfirst(lc_clauses);
				ListCell *lc;

				Assert(scanrelid);

				foreach (lc, ri_clauses)
				{
					RestrictInfo *ri = makeNode(RestrictInfo);
					ri->clause = lfirst(lc);

					/*
					 * The index of the RangeTblEntry might have changed between planning
					 * because of flattening, so we need to adjust the expressions
					 * for the RestrictInfos if they are not equal.
					 */
					if (lfirst_oid(lc_relid) != scanrelid)
						ChangeVarNodes((Node *) ri->clause, lfirst_oid(lc_relid), scanrelid, 0);

					restrictinfos = lappend(restrictinfos, ri);
				}
				restrictinfos = constify_restrictinfos(&root, restrictinfos);

				if (can_exclude_chunk(&root, estate, scanrelid, restrictinfos))
					continue;

				*appendplans = lappend(*appendplans, lfirst(lc_plan));
				break;
			}
			default:
				elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(plan));
				break;
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

#if PG96
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone != ExprEndResult)
		{
			node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
			return resultslot;
		}
#else
		return ExecProject(node->ss.ps.ps_ProjInfo);
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
ca_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ConstraintAwareAppendState *state = (ConstraintAwareAppendState *) node;
	Oid relid = linitial_oid(linitial(cscan->custom_private));

	ExplainPropertyText("Hypertable", get_rel_name(relid), es);
	ExplainPropertyIntegerCompat("Chunks left after exclusion",
								 NULL,
								 state->num_append_subplans,
								 es);
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
	Append *append = linitial(cscan->custom_plans);

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
constraint_aware_append_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
									List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan;
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	List *chunk_ri_clauses = NIL;
	List *chunk_relids = NIL;
	List *children = NIL;
	ListCell *lc_child;

	/*
	 * Postgres will inject Result nodes above mergeappend when target lists don't match
	 * because the nodes themselves do not perform projection. The ConstraintAwareAppend
	 * node can do this projection itself, however, so just throw away the result node
	 * Removing the Result node is only safe if there is no one-time filter
	 */
	if (IsA(linitial(custom_plans), Result) &&
		castNode(Result, linitial(custom_plans))->resconstantqual == NULL)
	{
		Result *result = castNode(Result, linitial(custom_plans));

		if (result->plan.righttree != NULL)
			elog(ERROR, "unexpected right tree below result node in constraint aware append");

		custom_plans = list_make1(result->plan.lefttree);
	}
	subplan = linitial(custom_plans);

	cscan->scan.scanrelid = 0;			 /* Not a real relation we are scanning */
	cscan->scan.plan.targetlist = tlist; /* Target list we expect as output */
	cscan->custom_plans = custom_plans;

	/*
	 * create per chunk RestrictInfo
	 *
	 * We also need to walk the expression trees of the restriction clauses and
	 * update any Vars that reference the main table to instead reference the child
	 * table (chunk) we want to exclude.
	 */
	switch (nodeTag(linitial(custom_plans)))
	{
		case T_MergeAppend:
			children = castNode(MergeAppend, linitial(custom_plans))->mergeplans;
			break;
		case T_Append:
			children = castNode(Append, linitial(custom_plans))->appendplans;
			break;
		default:
			elog(ERROR,
				 "invalid child of constraint-aware append: %u",
				 nodeTag(linitial(custom_plans)));
			break;
	}

	/*
	 * we only iterate over the child chunks of this node
	 * so the list of metadata exactly matches the list of
	 * child nodes in the executor
	 */
	foreach (lc_child, children)
	{
		Plan *plan = get_plans_for_exclusion(lfirst(lc_child));

		switch (nodeTag(plan))
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
			{
				List *chunk_clauses = NIL;
				ListCell *lc;
				Index scanrelid = ((Scan *) plan)->scanrelid;
				AppendRelInfo *appinfo = ts_get_appendrelinfo(root, scanrelid, false);

				foreach (lc, clauses)
				{
					Node *clause = (Node *) ts_transform_cross_datatype_comparison(
						castNode(RestrictInfo, lfirst(lc))->clause);
					clause = adjust_appendrel_attrs_compat(root, clause, appinfo);
					chunk_clauses = lappend(chunk_clauses, clause);
				}
				chunk_ri_clauses = lappend(chunk_ri_clauses, chunk_clauses);
				chunk_relids = lappend_oid(chunk_relids, scanrelid);
				break;
			}
			default:
				elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(plan));
				break;
		}
	}

	cscan->custom_private = list_make3(list_make1_oid(rte->relid), chunk_ri_clauses, chunk_relids);
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

Path *
ts_constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath)
{
	ConstraintAwareAppendPath *path;

	path = (ConstraintAwareAppendPath *) newNode(sizeof(ConstraintAwareAppendPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;

	path->cpath.path.parallel_aware = false;
	path->cpath.path.parallel_safe = subpath->parallel_safe;
	path->cpath.path.parallel_workers = subpath->parallel_workers;

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
	 * Make sure our subpath is either an Append or MergeAppend node
	 */
	switch (nodeTag(subpath))
	{
		case T_AppendPath:
		case T_MergeAppendPath:
			break;
		default:
			elog(ERROR, "invalid child of constraint-aware append: %u", nodeTag(subpath));
			break;
	}

	return &path->cpath.path;
}

bool
ts_constraint_aware_append_possible(Path *path)
{
	RelOptInfo *rel = path->parent;
	ListCell *lc;
	int num_children;

	if (ts_guc_disable_optimizations || !ts_guc_constraint_aware_append ||
		constraint_exclusion == CONSTRAINT_EXCLUSION_OFF)
		return false;

	switch (nodeTag(path))
	{
		case T_AppendPath:
			num_children = list_length(castNode(AppendPath, path)->subpaths);
			break;
		case T_MergeAppendPath:
			num_children = list_length(castNode(MergeAppendPath, path)->subpaths);
			break;
		default:
			return false;
	}

	/* Never use constraint-aware append with only one child, since PG12 will
	 * later prune the (Merge)Append node from such plans, leaving us with an
	 * unexpected child node. */
	if (num_children <= 1)
		return false;

	/*
	 * If there are clauses that have mutable functions, this path is ripe for
	 * execution-time optimization.
	 */
	foreach (lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (contain_mutable_functions((Node *) rinfo->clause))
			return true;
	}
	return false;
}
void
_constraint_aware_append_init(void)
{
	RegisterCustomScanMethods(&constraint_aware_append_plan_methods);
}
