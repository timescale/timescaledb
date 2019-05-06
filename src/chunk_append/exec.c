/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/plancat.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/typcache.h>

#include "chunk_append/chunk_append.h"
#include "chunk_append/exec.h"
#include "chunk_append/planner.h"
#include "compat.h"

static TupleTableSlot *chunk_append_exec(CustomScanState *node);
static void chunk_append_begin(CustomScanState *node, EState *estate, int eflags);
static void chunk_append_end(CustomScanState *node);
static void chunk_append_rescan(CustomScanState *node);

static void chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomExecMethods chunk_append_state_methods = {
	.BeginCustomScan = chunk_append_begin,
	.ExecCustomScan = chunk_append_exec,
	.EndCustomScan = chunk_append_end,
	.ReScanCustomScan = chunk_append_rescan,
	.ExplainCustomScan = chunk_append_explain,
};

static List *constify_restrictinfos(PlannerInfo *root, List *restrictinfos);
static bool can_exclude_chunk(PlannerInfo *root, EState *estate, Index rt_index,
							  List *restrictinfos);
static void do_startup_exclusion(ChunkAppendState *state);
static Node *constify_param_mutator(Node *node, void *context);
static List *constify_restrictinfo_params(PlannerInfo *root, EState *state, List *restrictinfos);

static void adjust_ri_clauses(ChunkAppendState *state, List *initial_rt_indexes);

Node *
chunk_append_state_create(CustomScan *cscan)
{
	ChunkAppendState *state;

	state = (ChunkAppendState *) newNode(sizeof(ChunkAppendState), T_CustomScanState);

	state->csstate.methods = &chunk_append_state_methods;

	state->num_subplans = 0;
	state->subplanstates = NULL;

	state->initial_subplans = cscan->custom_plans;
	state->initial_ri_clauses = lsecond(cscan->custom_private);
	adjust_ri_clauses(state, lthird(cscan->custom_private));

	state->ht_reloid = linitial_oid(linitial(cscan->custom_private));
	state->startup_exclusion = (bool) lsecond_oid(linitial(cscan->custom_private));
	state->runtime_exclusion = (bool) lthird_oid(linitial(cscan->custom_private));
	state->limit = lfourth_oid(linitial(cscan->custom_private));

	state->current = 0;
	state->runtime_initialized = false;
	state->filtered_subplans = state->initial_subplans;
	state->filtered_ri_clauses = state->initial_ri_clauses;

	return (Node *) state;
}

static void
do_startup_exclusion(ChunkAppendState *state)
{
	List *filtered_children = NIL;
	List *filtered_ri_clauses = NIL;
	ListCell *lc_plan;
	ListCell *lc_clauses;

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

	/*
	 * clauses should always have the same length as appendplans because
	 * the list of clauses is built from the list of appendplans
	 */
	Assert(list_length(state->initial_subplans) == list_length(state->initial_ri_clauses));

	forboth (lc_plan, state->initial_subplans, lc_clauses, state->initial_ri_clauses)
	{
		List *restrictinfos = NIL;
		List *ri_clauses = lfirst(lc_clauses);
		ListCell *lc;
		Scan *scan = chunk_append_get_scan_plan(lfirst(lc_plan));

		/*
		 * If this is a base rel (chunk), check if it can be
		 * excluded from the scan. Otherwise, fall through.
		 */
		if (scan != NULL && scan->scanrelid)
		{
			foreach (lc, ri_clauses)
			{
				RestrictInfo *ri = makeNode(RestrictInfo);
				ri->clause = lfirst(lc);
				restrictinfos = lappend(restrictinfos, ri);
			}
			restrictinfos = constify_restrictinfos(&root, restrictinfos);

			if (can_exclude_chunk(&root,
								  state->csstate.ss.ps.state,
								  scan->scanrelid,
								  restrictinfos))
				continue;
		}

		filtered_children = lappend(filtered_children, lfirst(lc_plan));
		filtered_ri_clauses = lappend(filtered_ri_clauses, ri_clauses);
	}
	state->filtered_subplans = filtered_children;
	state->filtered_ri_clauses = filtered_ri_clauses;
}

static void
chunk_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	ChunkAppendState *state = (ChunkAppendState *) node;
	ListCell *lc;
	int i;

	if (state->startup_exclusion)
		do_startup_exclusion(state);

	cscan->custom_plans = state->filtered_subplans;

	state->num_subplans = list_length(cscan->custom_plans);

	if (state->num_subplans == 0)
		return;

	state->subplanstates = palloc0(state->num_subplans * sizeof(PlanState *));

	i = 0;
	foreach (lc, cscan->custom_plans)
	{
		/*
		 * we use an array for the states but put it in custom_ps as well
		 * so explain and planstate_tree_walker can find it
		 */
		state->subplanstates[i] = ExecInitNode(lfirst(lc), estate, eflags);
		node->custom_ps = lappend(node->custom_ps, state->subplanstates[i]);

		/*
		 * pass down limit to child nodes
		 */
		if (state->limit)
			ExecSetTupleBound(state->limit, state->subplanstates[i]);

		i++;
	}
}

static void
initialize_runtime_exclusion(ChunkAppendState *state)
{
	ListCell *lc_clauses;
	int i = 0;

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

	Assert(state->num_subplans == list_length(state->filtered_ri_clauses));

	lc_clauses = list_head(state->filtered_ri_clauses);

	for (i = 0; i < state->num_subplans; i++)
	{
		PlanState *ps = state->subplanstates[i];
		Scan *scan = chunk_append_get_scan_plan(ps->plan);
		List *restrictinfos = NIL;
		ListCell *lc;

		if (scan == NULL || scan->scanrelid == 0)
		{
			state->valid_subplans = bms_add_member(state->valid_subplans, i);
		}
		else
		{
			foreach (lc, lfirst(lc_clauses))
			{
				RestrictInfo *ri = makeNode(RestrictInfo);
				ri->clause = lfirst(lc);
				restrictinfos = lappend(restrictinfos, ri);
			}
			restrictinfos = constify_restrictinfo_params(&root, ps->state, restrictinfos);

			if (!can_exclude_chunk(&root, ps->state, scan->scanrelid, restrictinfos))
				state->valid_subplans = bms_add_member(state->valid_subplans, i);
		}

		lc_clauses = lnext(lc_clauses);
	}

	state->runtime_initialized = true;
}

static TupleTableSlot *
chunk_append_exec(CustomScanState *node)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
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
	if (state->num_subplans == 0)
		return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	if (state->runtime_exclusion && !state->runtime_initialized)
	{
		initialize_runtime_exclusion(state);

		if (!state->valid_subplans || bms_num_members(state->valid_subplans) == 0)
			return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

		state->current = bms_next_member(state->valid_subplans, -1);
	}

#if PG96
	if (node->ss.ps.ps_TupFromTlist)
	{
		resultslot = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

		if (isDone == ExprMultipleResult)
			return resultslot;

		node->ss.ps.ps_TupFromTlist = false;
	}
#endif

	while (true)
	{
		PlanState *subnode;

		CHECK_FOR_INTERRUPTS();

		/*
		 * figure out which subplan we are currently processing
		 */
		subnode = state->subplanstates[state->current];

		/*
		 * get a tuple from the subplan
		 */
		subslot = ExecProcNode(subnode);

		if (!TupIsNull(subslot))
		{
			/*
			 * If the subplan gave us something then return it as-is. We do
			 * NOT make use of the result slot that was set up in
			 * chunk_append_begin there's no need for it.
			 */
			if (node->ss.ps.ps_ProjInfo == NULL)
				return subslot;

			ResetExprContext(econtext);
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

		/*
		 * Go on to the "next" subplan in the appropriate direction. If no
		 * more subplans, return the empty slot set up for us by
		 * chunk_append_begin.
		 */
		if (state->runtime_exclusion)
			state->current = bms_next_member(state->valid_subplans, state->current);
		else
			state->current++;

		if (state->current >= state->num_subplans || state->current < 0)
			return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

		/* Else loop back and try to get a tuple from the new subplan */
	}
}

static void
chunk_append_end(CustomScanState *node)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	int i;

	for (i = 0; i < state->num_subplans; i++)
	{
		ExecEndNode(state->subplanstates[i]);
	}
}

static void
chunk_append_rescan(CustomScanState *node)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	int i;

	for (i = 0; i < state->num_subplans; i++)
	{
		ExecReScan(state->subplanstates[i]);
	}
	state->current = 0;

	if (state->runtime_exclusion)
	{
		bms_free(state->valid_subplans);
		state->valid_subplans = NULL;
		state->runtime_initialized = false;
	}
}

static void
chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ChunkAppendState *state = (ChunkAppendState *) node;

	ExplainPropertyText("Hypertable", get_rel_name(state->ht_reloid), es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Startup Exclusion", state->startup_exclusion, es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Runtime Exclusion", state->runtime_exclusion, es);

	if (state->startup_exclusion)
		ExplainPropertyIntegerCompat("Chunks excluded during startup",
									 NULL,
									 list_length(state->initial_subplans) -
										 list_length(node->custom_ps),
									 es);

	if (state->runtime_exclusion)
		ExplainPropertyIntegerCompat("Chunks excluded during runtime",
									 NULL,
									 list_length(state->filtered_subplans) -
										 bms_num_members(state->valid_subplans),
									 es);
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

static List *
constify_restrictinfo_params(PlannerInfo *root, EState *state, List *restrictinfos)
{
	ListCell *lc;

	foreach (lc, restrictinfos)
	{
		RestrictInfo *rinfo = lfirst(lc);

		rinfo->clause = (Expr *) constify_param_mutator((Node *) rinfo->clause, state);
		rinfo->clause = (Expr *) estimate_expression_value(root, (Node *) rinfo->clause);
	}

	return restrictinfos;
}

static Node *
constify_param_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		EState *state = (EState *) context;

		if (param->paramkind == PARAM_EXEC)
		{
			TypeCacheEntry *tce = lookup_type_cache(param->paramtype, 0);
			ParamExecData value = state->es_param_exec_vals[param->paramid];

			if (!value.execPlan)
				return (Node *) makeConst(param->paramtype,
										  param->paramtypmod,
										  param->paramcollid,
										  tce->typlen,
										  value.value,
										  value.isnull,
										  tce->typbyval);
		}
		return node;
	}

	return expression_tree_mutator(node, constify_param_mutator, context);
}

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * This functions tries to reuse as much functionality as possible from standard
 * constraint exclusion in PostgreSQL that normally happens at planning
 * time. Therefore, we need to fake a number of planning-related data
 * structures.
 */
static bool
can_exclude_chunk(PlannerInfo *root, EState *estate, Index rt_index, List *restrictinfos)
{
	RangeTblEntry *rte = rt_fetch(rt_index, estate->es_range_table);
	RelOptInfo rel = {
		.type = T_RelOptInfo,
		.relid = rt_index,
		.reloptkind = RELOPT_OTHER_MEMBER_REL,
		.baserestrictinfo = restrictinfos,
	};

	return rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_RELATION && !rte->inh &&
		   relation_excluded_by_constraints(root, &rel, rte);
}

/*
 * Adjust the RangeTableEntry indexes in the restrictinfo
 * clauses because during planning subquery indexes will be
 * different from the final index after flattening.
 */
static void
adjust_ri_clauses(ChunkAppendState *state, List *initial_rt_indexes)
{
	ListCell *lc_clauses, *lc_plan, *lc_relid;

	if (initial_rt_indexes == NIL)
		return;

	Assert(list_length(state->initial_subplans) == list_length(state->initial_ri_clauses));
	Assert(list_length(state->initial_subplans) == list_length(initial_rt_indexes));

	forthree (lc_plan,
			  state->initial_subplans,
			  lc_clauses,
			  state->initial_ri_clauses,
			  lc_relid,
			  initial_rt_indexes)
	{
		Scan *scan = chunk_append_get_scan_plan(lfirst(lc_plan));
		Index initial_index = lfirst_oid(lc_relid);

		if (scan != NULL && scan->scanrelid > 0 && scan->scanrelid != initial_index)
		{
			ChangeVarNodes(lfirst(lc_clauses), initial_index, scan->scanrelid, 0);
		}
	}
}
