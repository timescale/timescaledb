/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/plannodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/prep.h>
#include <foreign/fdwapi.h>
#include <access/sysattr.h>
#include <miscadmin.h>

#include "async_append.h"
#include "fdw/scan_plan.h"
#include "fdw/scan_exec.h"
#include "fdw/data_node_scan_plan.h"
#include "planner.h"
#include "cache.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "remote/cursor.h"

/*
 * AsyncAppend provides an asynchronous API during query execution to create cursors and
 * fetch data from data nodes more efficiently. This should make better use of our data nodes
 * and do more things in parallel. AsyncAppend is only used for plans
 * that involve distributed hypertable (a plan that involves scanning of data nodes).
 * The node is injected as a parent of Append or MergeAppend nodes.
 * Here is how the modified plan looks like.
 *
 *       .......
 *          |
 *    -------------
 *   | AsyncAppend |
 *    -------------
 *          |           -----------------------
 *          -----------| Append or MergeAppend |
 *                      -----------------------
 *                                |               --------------
 *                                ---------------| DataNodeScan |
 *                                |               --------------
 *                                |               --------------
 *                                ---------------| DataNodeScan |
 *                                |                --------------
 *
 *                              .....
 *
 *
 * Since the PostgreSQL planner treats partitioned relations in a special way (throwing away
 * existing and generating new paths), we needed to adjust plan paths at a later stage,
 * thus using upper path hooks to do that.
 *
 */

/* Plan state node for AsyncAppend plan */
typedef struct AsyncAppendState
{
	CustomScanState css;
	PlanState *subplan_state; /* AppendState or MergeAppendState */
	List *data_node_scans;	/* DataNodeScan states */
	bool first_run;
} AsyncAppendState;

static TupleTableSlot *async_append_exec(CustomScanState *node);
static void async_append_begin(CustomScanState *node, EState *estate, int eflags);
static void async_append_end(CustomScanState *node);
static void async_append_rescan(CustomScanState *node);

static CustomExecMethods async_append_state_methods = {
	.CustomName = "AsyncAppendState",
	.BeginCustomScan = async_append_begin,
	.EndCustomScan = async_append_end,
	.ExecCustomScan = async_append_exec,
	.ReScanCustomScan = async_append_rescan,
};

static Node *
async_append_state_create(CustomScan *cscan)
{
	AsyncAppendState *state =
		(AsyncAppendState *) newNode(sizeof(AsyncAppendState), T_CustomScanState);

	state->subplan_state = NULL;
	state->css.methods = &async_append_state_methods;
	state->first_run = true;

	return (Node *) state;
}

static CustomScanMethods async_append_plan_methods = {
	.CustomName = "AsyncAppend",
	.CreateCustomScanState = async_append_state_create,
};

static PlanState *
find_data_node_scan_state_child(PlanState *state)
{
	if (state)
	{
		switch (nodeTag(state))
		{
			case T_CustomScanState:
				return state;
			case T_SortState:
			case T_AggState:
				/* Data scan state can be buried under AggState or SortState  */
				return find_data_node_scan_state_child(state->lefttree);
			default:
				elog(ERROR, "unexpected child node of Append or MergeAppend: %d", nodeTag(state));
		}
	}

	elog(ERROR, "could not find a DataNodeScan in plan state for AsyncAppend");
	pg_unreachable();
}

static List *
get_data_node_async_scan_states(AsyncAppendState *state)
{
	PlanState **child_plans;
	int num_child_plans;
	List *dn_plans = NIL;
	int i;

	if (IsA(state->subplan_state, AppendState))
	{
		AppendState *astate = castNode(AppendState, state->subplan_state);
		child_plans = astate->appendplans;
		num_child_plans = astate->as_nplans;
	}
	else if (IsA(state->subplan_state, MergeAppendState))
	{
		MergeAppendState *mstate = castNode(MergeAppendState, state->subplan_state);
		child_plans = mstate->mergeplans;
		num_child_plans = mstate->ms_nplans;
	}
	else
		elog(ERROR, "unexpected child node %u of AsyncAppend", nodeTag(state->subplan_state));

	for (i = 0; i < num_child_plans; i++)
		dn_plans = lappend(dn_plans, find_data_node_scan_state_child(child_plans[i]));

	return dn_plans;
}

static void
async_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	AsyncAppendState *state = (AsyncAppendState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *subplan;
	PlanState *subplan_state;

	Assert(cscan->custom_plans != NULL);
	Assert(list_length(cscan->custom_plans) == 1);

	subplan = linitial(cscan->custom_plans);
	subplan_state = ExecInitNode(subplan, estate, eflags);
	state->subplan_state = subplan_state;
	state->css.custom_ps = list_make1(state->subplan_state);
	state->data_node_scans = get_data_node_async_scan_states(state);
}

static void
iterate_data_nodes_and_exec(AsyncAppendState *aas, void (*dn_exec)(AsyncScanState *ass))
{
	ListCell *lc;

	foreach (lc, aas->data_node_scans)
	{
		AsyncScanState *dnss = lfirst(lc);
		dn_exec(dnss);
	}
}

static void
init(AsyncScanState *ass)
{
	ass->init(ass);
}

static void
fetch_tuples(AsyncScanState *ass)
{
	ass->fetch_tuples(ass);
}

static TupleTableSlot *
async_append_exec(CustomScanState *node)
{
	TupleTableSlot *slot;
	AsyncAppendState *state = (AsyncAppendState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	Assert(state->subplan_state != NULL);
	Assert(state->data_node_scans != NIL);

	if (state->first_run)
	{
		/* Since we can't start sending requests in the BeginCustomScan stage (it's not guaranteed
		 * that a node will execute so we might end up sending requests that never get completed) we
		 * do it'here */
		state->first_run = false;
		iterate_data_nodes_and_exec(state, init);
		iterate_data_nodes_and_exec(state, fetch_tuples);
	}

	ResetExprContext(econtext);

	slot = ExecProcNode(state->subplan_state);
	econtext->ecxt_scantuple = slot;

	if (!TupIsNull(slot))
	{
		if (!node->ss.ps.ps_ProjInfo)
			return slot;
		return ExecProject(node->ss.ps.ps_ProjInfo);
	}

	return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
}

static void
async_append_end(CustomScanState *node)
{
	AsyncAppendState *state = (AsyncAppendState *) node;

	ExecEndNode(state->subplan_state);
}

static void
async_append_rescan(CustomScanState *node)
{
	AsyncAppendState *state = (AsyncAppendState *) node;

	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(state->subplan_state, node->ss.ps.chgParam);

	ExecReScan(state->subplan_state);
}

static Plan *
async_append_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						 List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	Plan *subplan;

	cscan->methods = &async_append_plan_methods;
	/* output target list */
	cscan->scan.plan.targetlist = tlist;
	/* we don't scan a real relation here */
	cscan->scan.scanrelid = 0;
	cscan->flags = best_path->flags;

	/* remove Result node since AsyncAppend node will project */
	if (IsA(linitial(custom_plans), Result) &&
		castNode(Result, linitial(custom_plans))->resconstantqual == NULL)
	{
		Result *result = castNode(Result, linitial(custom_plans));

		if (result->plan.righttree != NULL)
			elog(ERROR, "unexpected right tree below result node in async append");

		custom_plans = list_make1(result->plan.lefttree);
	}

	cscan->custom_plans = custom_plans;
	subplan = linitial(custom_plans);
	if (!(IsA(subplan, MergeAppend) || IsA(subplan, Append)))
		elog(ERROR, "unexpected child node of AsyncAppend");

	/* input target list */
	cscan->custom_scan_tlist = subplan->targetlist;
	return &cscan->scan.plan;
}

static CustomPathMethods async_append_path_methods = {
	.CustomName = "AsyncAppendPath",
	.PlanCustomPath = async_append_plan_create,
};

static bool
is_data_node_scan_path(Path *path)
{
	CustomPath *cpath;
	if (!IsA(path, CustomPath))
		return false;

	cpath = castNode(CustomPath, path);
	if (strcmp(cpath->methods->CustomName, DATA_NODE_SCAN_PATH_NAME) != 0)
		return false;

	return true;
}

static void
path_process(PlannerInfo *root, Path **path)
{
	Path **subpath = path;
	Path *subp = *subpath;
	List *children = NIL;
	Path *child;
	AsyncAppendPath *aa_path;

	switch (nodeTag(*subpath))
	{
		case T_AppendPath:
			children = castNode(AppendPath, subp)->subpaths;
			break;
		case T_MergeAppendPath:
			children = castNode(MergeAppendPath, subp)->subpaths;
			break;
		case T_AggPath:
			path_process(root, &castNode(AggPath, subp)->subpath);
			return;
		case T_GroupPath:
			path_process(root, &castNode(GroupPath, subp)->subpath);
			return;
		case T_SortPath:
			path_process(root, &castNode(SortPath, subp)->subpath);
			return;
		case T_UpperUniquePath:
			path_process(root, &castNode(UpperUniquePath, subp)->subpath);
			return;
		case T_ProjectionPath:
			path_process(root, &castNode(ProjectionPath, subp)->subpath);
			return;
		case T_ProjectSetPath:
			path_process(root, &castNode(ProjectSetPath, subp)->subpath);
			return;
		case T_LimitPath:
			path_process(root, &castNode(LimitPath, subp)->subpath);
			return;
		case T_UniquePath:
			path_process(root, &castNode(UniquePath, subp)->subpath);
			return;
		case T_GatherPath:
			path_process(root, &castNode(GatherPath, subp)->subpath);
			return;
		case T_GatherMergePath:
			path_process(root, &castNode(GatherMergePath, subp)->subpath);
			return;
		case T_MaterialPath:
			path_process(root, &castNode(MaterialPath, subp)->subpath);
			return;
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			path_process(root, &((JoinPath *) subp)->outerjoinpath);
			path_process(root, &((JoinPath *) subp)->innerjoinpath);
			return;
		case T_MinMaxAggPath:
		{
			MinMaxAggPath *mm_path = castNode(MinMaxAggPath, subp);
			ListCell *mm_lc;
			foreach (mm_lc, mm_path->mmaggregates)
			{
				MinMaxAggInfo *mm_info = lfirst_node(MinMaxAggInfo, mm_lc);
				path_process(root, &mm_info->path);
			}
			return;
		}
		case T_WindowAggPath:
			path_process(root, &castNode(WindowAggPath, subp)->subpath);
			return;
		default:
			return;
	}

	if (children == NIL)
		return;

	/* AsyncAppend only makes sense when there are multiple children that we'd
	 * like to asynchronously scan. Also note that PG12 will remove append
	 * nodes when there's a single child and this will confuse AsyncAppend. */
	if (list_length(children) <= 1)
		return;

	child = linitial(children);

	/* sometimes data node scan is buried under ProjectionPath or AggPath */
	if (IsA(child, ProjectionPath))
		child = castNode(ProjectionPath, child)->subpath;
	else if (IsA(child, AggPath))
		child = castNode(AggPath, child)->subpath;

	if (!is_data_node_scan_path(child))
		return;

	aa_path = (AsyncAppendPath *) newNode(sizeof(AsyncAppendPath), T_CustomPath);

	aa_path->cpath.path.pathtype = T_CustomScan;
	aa_path->cpath.path.parent = subp->parent;
	aa_path->cpath.path.pathtarget = subp->pathtarget;
	aa_path->cpath.flags = 0;
	aa_path->cpath.methods = &async_append_path_methods;
	aa_path->cpath.path.parallel_aware = false;
	aa_path->cpath.path.param_info = subp->param_info;
	aa_path->cpath.path.parallel_safe = false;
	aa_path->cpath.path.parallel_workers = subp->parallel_workers;
	aa_path->cpath.path.pathkeys = subp->pathkeys;
	aa_path->cpath.custom_paths = list_make1(subp);

	/* reuse subpath estimated rows and costs */
	aa_path->cpath.path.rows = subp->rows;
	aa_path->cpath.path.startup_cost = subp->startup_cost;
	aa_path->cpath.path.total_cost = subp->total_cost;

	*subpath = &aa_path->cpath.path;
}

void
async_append_add_paths(PlannerInfo *root, RelOptInfo *final_rel)
{
	ListCell *lc;

	foreach (lc, final_rel->pathlist)
		path_process(root, (Path **) &lfirst(lc));
}
