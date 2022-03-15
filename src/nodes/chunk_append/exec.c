/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <catalog/pg_collation.h>
#include <executor/executor.h>
#include <executor/nodeSubplan.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/ruleutils.h>
#include <utils/typcache.h>

#include <math.h>

#include "nodes/chunk_append/chunk_append.h"
#include "loader/lwlocks.h"

#define INVALID_SUBPLAN_INDEX -1
#define NO_MATCHING_SUBPLANS -2

typedef struct ParallelChunkAppendState
{
	int next_plan;
	bool finished[FLEXIBLE_ARRAY_MEMBER];
} ParallelChunkAppendState;

typedef struct ChunkAppendState
{
	CustomScanState csstate;
	PlanState **subplanstates;

	MemoryContext exclusion_ctx;

	int num_subplans;
	int first_partial_plan;
	int filtered_first_partial_plan;
	int current;

	Oid ht_reloid;
	bool startup_exclusion;
	bool runtime_exclusion;
	bool runtime_initialized;
	uint32 limit;

	/* list of subplans after planning */
	List *initial_subplans;
	/* list of constraints indexed like initial_subplans */
	List *initial_constraints;
	/* list of restrictinfo clauses indexed like initial_subplans */
	List *initial_ri_clauses;

	/* list of subplans after startup exclusion */
	List *filtered_subplans;
	/* list of relation constraints after startup exclusion */
	List *filtered_constraints;
	/* list of restrictinfo clauses after startup exclusion */
	List *filtered_ri_clauses;

	/* valid subplans for runtime exclusion */
	Bitmapset *valid_subplans;
	Bitmapset *params;

	/* sort options if this append is ordered, only used for EXPLAIN */
	List *sort_options;

	/* number of loops and exclusions for EXPLAIN */
	int runtime_number_loops;
	int runtime_number_exclusions;

	LWLock *lock;
	ParallelContext *pcxt;
	ParallelChunkAppendState *pstate;
	void (*choose_next_subplan)(struct ChunkAppendState *);
} ChunkAppendState;

static TupleTableSlot *chunk_append_exec(CustomScanState *node);
static void chunk_append_begin(CustomScanState *node, EState *estate, int eflags);
static void chunk_append_end(CustomScanState *node);
static void chunk_append_rescan(CustomScanState *node);
static void chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static Size chunk_append_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void chunk_append_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
										void *coordinate);
static void chunk_append_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
										  void *coordinate);
static void chunk_append_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate);

static CustomExecMethods chunk_append_state_methods = {
	.BeginCustomScan = chunk_append_begin,
	.ExecCustomScan = chunk_append_exec,
	.EndCustomScan = chunk_append_end,
	.ReScanCustomScan = chunk_append_rescan,
	.ExplainCustomScan = chunk_append_explain,
	.EstimateDSMCustomScan = chunk_append_estimate_dsm,
	.InitializeDSMCustomScan = chunk_append_initialize_dsm,
	.ReInitializeDSMCustomScan = chunk_append_reinitialize_dsm,
	.InitializeWorkerCustomScan = chunk_append_initialize_worker,
};

static void choose_next_subplan_non_parallel(ChunkAppendState *state);
static void choose_next_subplan_for_worker(ChunkAppendState *state);

static List *constify_restrictinfos(PlannerInfo *root, List *restrictinfos);
static bool can_exclude_chunk(List *constraints, List *restrictinfos);
static void do_startup_exclusion(ChunkAppendState *state);
static Node *constify_param_mutator(Node *node, void *context);
static List *constify_restrictinfo_params(PlannerInfo *root, EState *state, List *restrictinfos);

static void initialize_constraints(ChunkAppendState *state, List *initial_rt_indexes);
static LWLock *chunk_append_get_lock_pointer(void);

static void show_sort_group_keys(ChunkAppendState *planstate, List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation,
								   bool nullsFirst);

Node *
ts_chunk_append_state_create(CustomScan *cscan)
{
	ChunkAppendState *state;
	List *settings = linitial(cscan->custom_private);

	state = (ChunkAppendState *) newNode(sizeof(ChunkAppendState), T_CustomScanState);

	state->csstate.methods = &chunk_append_state_methods;

	state->initial_subplans = cscan->custom_plans;
	state->initial_ri_clauses = lsecond(cscan->custom_private);
	state->sort_options = lfourth(cscan->custom_private);

	state->startup_exclusion = (bool) linitial_int(settings);
	state->runtime_exclusion = (bool) lsecond_int(settings);
	state->limit = lthird_int(settings);
	state->first_partial_plan = lfourth_int(settings);

	state->filtered_subplans = state->initial_subplans;
	state->filtered_ri_clauses = state->initial_ri_clauses;
	state->filtered_first_partial_plan = state->first_partial_plan;

	state->current = INVALID_SUBPLAN_INDEX;
	state->choose_next_subplan = choose_next_subplan_non_parallel;

	state->exclusion_ctx = AllocSetContextCreate(CurrentMemoryContext,
												 "ChunkApppend exclusion",
												 ALLOCSET_DEFAULT_SIZES);

	return (Node *) state;
}

static void
do_startup_exclusion(ChunkAppendState *state)
{
	List *filtered_children = NIL;
	List *filtered_ri_clauses = NIL;
	List *filtered_constraints = NIL;
	ListCell *lc_plan;
	ListCell *lc_clauses;
	ListCell *lc_constraints;
	int i = -1;
	int filtered_first_partial_plan = state->first_partial_plan;

	/*
	 * create skeleton plannerinfo for estimate_expression_value
	 */
	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

	/*
	 * clauses and constraints should always have the same length as initial_subplans
	 */
	Assert(list_length(state->initial_subplans) == list_length(state->initial_ri_clauses));
	Assert(list_length(state->initial_subplans) == list_length(state->initial_constraints));

	forthree (lc_plan,
			  state->initial_subplans,
			  lc_constraints,
			  state->initial_constraints,
			  lc_clauses,
			  state->initial_ri_clauses)
	{
		List *restrictinfos = NIL;
		List *ri_clauses = lfirst(lc_clauses);
		ListCell *lc;
		Scan *scan = ts_chunk_append_get_scan_plan(lfirst(lc_plan));

		i++;

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

			if (can_exclude_chunk(lfirst(lc_constraints), restrictinfos))
			{
				if (i < state->first_partial_plan)
					filtered_first_partial_plan--;

				continue;
			}

			/*
			 * if this node does runtime exclusion we keep the constified
			 * expressions to save us some work during runtime exclusion
			 */
			if (state->runtime_exclusion)
			{
				List *const_ri_clauses = NIL;
				foreach (lc, restrictinfos)
				{
					RestrictInfo *ri = lfirst(lc);
					const_ri_clauses = lappend(const_ri_clauses, ri->clause);
				}
				ri_clauses = const_ri_clauses;
			}
		}

		filtered_children = lappend(filtered_children, lfirst(lc_plan));
		filtered_ri_clauses = lappend(filtered_ri_clauses, ri_clauses);
		filtered_constraints = lappend(filtered_constraints, lfirst(lc_constraints));
	}

	state->filtered_subplans = filtered_children;
	state->filtered_ri_clauses = filtered_ri_clauses;
	state->filtered_constraints = filtered_constraints;
	state->filtered_first_partial_plan = filtered_first_partial_plan;
}

/*
 * Complete initialization of the supplied CustomScanState.
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
chunk_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	ChunkAppendState *state = (ChunkAppendState *) node;
	ListCell *lc;
	int i;

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

	initialize_constraints(state, lthird(cscan->custom_private));

	if (state->startup_exclusion)
		do_startup_exclusion(state);

	state->num_subplans = list_length(state->filtered_subplans);

	if (state->num_subplans == 0)
	{
		state->current = NO_MATCHING_SUBPLANS;
		return;
	}

	state->subplanstates = palloc0(state->num_subplans * sizeof(PlanState *));

	i = 0;
	foreach (lc, state->filtered_subplans)
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

	if (state->runtime_exclusion)
	{
		state->params = state->subplanstates[0]->plan->allParam;
		/*
		 * make sure all params are initialized for runtime exclusion
		 */
		node->ss.ps.chgParam = bms_copy(state->subplanstates[0]->plan->allParam);
	}
}

/*
 * build bitmap of valid subplans for runtime exclusion
 */
static void
initialize_runtime_exclusion(ChunkAppendState *state)
{
	ListCell *lc_clauses, *lc_constraints;
	int i = 0;

	PlannerGlobal glob = {
		.boundParams = NULL,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

	Assert(state->num_subplans == list_length(state->filtered_ri_clauses));

	lc_clauses = list_head(state->filtered_ri_clauses);
	lc_constraints = list_head(state->filtered_constraints);

	if (state->num_subplans == 0)
	{
		state->runtime_initialized = true;
		return;
	}

	state->runtime_number_loops++;
	/*
	 * mark subplans as active/inactive in valid_subplans
	 */
	for (i = 0; i < state->num_subplans; i++)
	{
		PlanState *ps = state->subplanstates[i];
		Scan *scan = ts_chunk_append_get_scan_plan(ps->plan);
		List *restrictinfos = NIL;
		ListCell *lc;

		if (scan == NULL || scan->scanrelid == 0)
		{
			state->valid_subplans = bms_add_member(state->valid_subplans, i);
		}
		else
		{
			bool can_exclude;
			MemoryContext old = MemoryContextSwitchTo(state->exclusion_ctx);

			foreach (lc, lfirst(lc_clauses))
			{
				RestrictInfo *ri = makeNode(RestrictInfo);
				ri->clause = lfirst(lc);
				restrictinfos = lappend(restrictinfos, ri);
			}
			restrictinfos = constify_restrictinfo_params(&root, ps->state, restrictinfos);

			can_exclude = can_exclude_chunk(lfirst(lc_constraints), restrictinfos);

			MemoryContextReset(state->exclusion_ctx);
			MemoryContextSwitchTo(old);

			if (!can_exclude)
				state->valid_subplans = bms_add_member(state->valid_subplans, i);
			else
				state->runtime_number_exclusions++;
		}

		lc_clauses = lnext_compat(state->filtered_ri_clauses, lc_clauses);
		lc_constraints = lnext_compat(state->filtered_constraints, lc_constraints);
	}

	state->runtime_initialized = true;
}

/*
 * Fetch the next scan tuple.
 *
 * If any tuples remain, it should fill ps_ResultTupleSlot with the next
 * tuple in the current scan direction, and then return the tuple slot.
 * If not, NULL or an empty slot should be returned.
 */
static TupleTableSlot *
chunk_append_exec(CustomScanState *node)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ProjectionInfo *projinfo = node->ss.ps.ps_ProjInfo;
	TupleTableSlot *subslot;

	if (state->current == INVALID_SUBPLAN_INDEX)
		state->choose_next_subplan(state);

	while (true)
	{
		PlanState *subnode;

		CHECK_FOR_INTERRUPTS();

		if (state->current == NO_MATCHING_SUBPLANS)
			return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

		Assert(state->current >= 0 && state->current < state->num_subplans);
		subnode = state->subplanstates[state->current];

		/*
		 * get a tuple from the subplan
		 */
		subslot = ExecProcNode(subnode);

		if (!TupIsNull(subslot))
		{
			/*
			 * If the subplan gave us something check if we need
			 * to do projection otherwise return as is.
			 */
			if (projinfo == NULL)
				return subslot;

			ResetExprContext(econtext);
			econtext->ecxt_scantuple = subslot;

			return ExecProject(projinfo);
		}

		state->choose_next_subplan(state);

		/* loop back and try to get a tuple from the new subplan */
	}
}

static int
get_next_subplan(ChunkAppendState *state, int last_plan)
{
	if (last_plan == NO_MATCHING_SUBPLANS)
		return NO_MATCHING_SUBPLANS;

	if (state->runtime_exclusion)
	{
		if (!state->runtime_initialized)
			initialize_runtime_exclusion(state);

		/*
		 * bms_next_member will return -2 (NO_MATCHING_SUBPLANS) if there are
		 * no more members
		 */
		return bms_next_member(state->valid_subplans, last_plan);
	}
	else
	{
		int next_plan = last_plan + 1;

		if (next_plan >= state->num_subplans)
			return NO_MATCHING_SUBPLANS;

		return next_plan;
	}
}

static void
choose_next_subplan_non_parallel(ChunkAppendState *state)
{
	state->current = get_next_subplan(state, state->current);
}

static void
choose_next_subplan_for_worker(ChunkAppendState *state)
{
	ParallelChunkAppendState *pstate = state->pstate;
	int next_plan;
	int start;

	LWLockAcquire(state->lock, LW_EXCLUSIVE);

	/* mark just completed subplan as finished */
	if (state->current >= 0)
		pstate->finished[state->current] = true;

	if (pstate->next_plan == INVALID_SUBPLAN_INDEX)
		next_plan = get_next_subplan(state, INVALID_SUBPLAN_INDEX);
	else
		next_plan = pstate->next_plan;

	if (next_plan == NO_MATCHING_SUBPLANS)
	{
		/* all subplans are finished */
		pstate->next_plan = NO_MATCHING_SUBPLANS;
		state->current = NO_MATCHING_SUBPLANS;
		LWLockRelease(state->lock);
		return;
	}

	start = next_plan;

	/* skip finished subplans */
	while (pstate->finished[next_plan])
	{
		next_plan = get_next_subplan(state, next_plan);

		/* wrap around if we reach end of subplan list */
		if (next_plan < 0)
			next_plan = get_next_subplan(state, INVALID_SUBPLAN_INDEX);

		if (next_plan == start || next_plan < 0)
		{
			/*
			 * back at start of search so all subplans are finished
			 *
			 * next_plan should not be < 0 because this means there
			 * are no valid subplans and then the function would
			 * have returned at the check before the while loop but
			 * static analysis marked this so might as well include
			 * that in the check
			 */
			Assert(next_plan >= 0);
			pstate->next_plan = NO_MATCHING_SUBPLANS;
			state->current = NO_MATCHING_SUBPLANS;
			LWLockRelease(state->lock);
			return;
		}
	}

	Assert(next_plan >= 0 && next_plan < state->num_subplans);
	state->current = next_plan;

	/*
	 * if this is not a partial plan we mark it as finished
	 * immediately so it does not get assigned another worker
	 */
	if (next_plan < state->filtered_first_partial_plan)
		pstate->finished[next_plan] = true;

	/* advance next_plan for next worker */
	pstate->next_plan = get_next_subplan(state, state->current);
	/*
	 * if we reach the end of the list of subplans we set next_plan
	 * to INVALID_SUBPLAN_INDEX to allow rechecking unfinished subplans
	 * on next call
	 */
	if (pstate->next_plan < 0)
		pstate->next_plan = INVALID_SUBPLAN_INDEX;

	LWLockRelease(state->lock);
}

/*
 * Clean up any private data associated with the CustomScanState.
 *
 * This method is required, but it does not need to do anything if there
 * is no associated data or it will be cleaned up automatically.
 */
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

/*
 * Rewind the current scan to the beginning and prepare to rescan the relation.
 */
static void
chunk_append_rescan(CustomScanState *node)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	int i;

	for (i = 0; i < state->num_subplans; i++)
	{
		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(state->subplanstates[i], node->ss.ps.chgParam);

		ExecReScan(state->subplanstates[i]);
	}
	state->current = INVALID_SUBPLAN_INDEX;

	/*
	 * detect changed params and reset runtime exclusion state
	 */
	if (state->runtime_exclusion && bms_overlap(node->ss.ps.chgParam, state->params))
	{
		bms_free(state->valid_subplans);
		state->valid_subplans = NULL;
		state->runtime_initialized = false;
	}
}

/*
 * Estimate the amount of dynamic shared memory that will be required
 * for parallel operation.
 * This may be higher than the amount that will actually be used,
 * but it must not be lower. The return value is in bytes.
 * This callback is optional, and need only be supplied if this
 * custom scan provider supports parallel execution.
 */
static Size
chunk_append_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	return add_size(offsetof(ParallelChunkAppendState, finished),
					sizeof(bool) * state->num_subplans);
}

/*
 * Initialize the dynamic shared memory that will be required for
 * parallel operation.
 * coordinate points to a shared memory area of size equal to the return
 * value of EstimateDSMCustomScan.
 * This callback is optional, and need only be supplied if this custom scan
 * provider supports parallel execution.
 */
static void
chunk_append_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	ParallelChunkAppendState *pstate = (ParallelChunkAppendState *) coordinate;

	memset(pstate, 0, node->pscan_len);

	state->lock = chunk_append_get_lock_pointer();
	pstate->next_plan = INVALID_SUBPLAN_INDEX;

	/*
	 * Leader should use the same subplan selection as normal worker threads. If the user wishes to
	 * disallow running plans on the leader they should do so via the parallel_leader_participation
	 * GUC.
	 */
	state->choose_next_subplan = choose_next_subplan_for_worker;
	state->current = INVALID_SUBPLAN_INDEX;
	state->pcxt = pcxt;
	state->pstate = pstate;
}

/*
 * Re-initialize the dynamic shared memory required for parallel operation
 * when the custom-scan plan node is about to be re-scanned.
 * This callback is optional, and need only be supplied if this custom scan
 * provider supports parallel execution.
 * Recommended practice is that this callback reset only shared state,
 * while the ReScanCustomScan callback resets only local state.
 * Currently, this callback will be called before ReScanCustomScan,
 * but it's best not to rely on that ordering.
 */
static void
chunk_append_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	ParallelChunkAppendState *pstate = (ParallelChunkAppendState *) coordinate;

	pstate->next_plan = INVALID_SUBPLAN_INDEX;
	memset(pstate->finished, 0, sizeof(bool) * state->num_subplans);
}

/*
 * Initialize a parallel worker's local state based on the shared state
 * set up by the leader during InitializeDSMCustomScan.
 *
 * This callback is optional, and need only be supplied if this custom scan
 * provider supports parallel execution.
 */
static void
chunk_append_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate)
{
	ChunkAppendState *state = (ChunkAppendState *) node;
	ParallelChunkAppendState *pstate = (ParallelChunkAppendState *) coordinate;

	state->lock = chunk_append_get_lock_pointer();
	state->choose_next_subplan = choose_next_subplan_for_worker;
	state->current = INVALID_SUBPLAN_INDEX;
	state->pstate = pstate;
}

/*
 * get a pointer to the LWLock used for coordinating
 * parallel workers
 */
static LWLock *
chunk_append_get_lock_pointer()
{
	LWLock **lock = (LWLock **) find_rendezvous_variable(RENDEZVOUS_CHUNK_APPEND_LWLOCK);

	if (*lock == NULL)
		elog(ERROR, "LWLock for coordinating parallel workers not initialized");

	return *lock;
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

	/* Don't descend into subplans to constify their parameters, because they may not be valid yet
	 */
	if (IsA(node, SubPlan))
		return node;

	if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		EState *estate = (EState *) context;

		if (param->paramkind == PARAM_EXEC)
		{
			TypeCacheEntry *tce = lookup_type_cache(param->paramtype, 0);
			ParamExecData prm = estate->es_param_exec_vals[param->paramid];

			if (prm.execPlan != NULL)
			{
				ExprContext *econtext = GetPerTupleExprContext(estate);
				ExecSetParamPlan(prm.execPlan, econtext);
			}

			if (prm.execPlan == NULL)
				return (Node *) makeConst(param->paramtype,
										  param->paramtypmod,
										  param->paramcollid,
										  tce->typlen,
										  prm.value,
										  prm.isnull,
										  tce->typbyval);
		}
		return node;
	}

	return expression_tree_mutator(node, constify_param_mutator, context);
}

/*
 * stripped down version of postgres get_relation_constraints
 */
static List *
ca_get_relation_constraints(Oid relationObjectId, Index varno, bool include_notnull)
{
	List *result = NIL;
	Relation relation;
	TupleConstr *constr;

	/*
	 * We assume the relation has already been safely locked.
	 */
	relation = table_open(relationObjectId, AccessShareLock);

	constr = relation->rd_att->constr;
	if (constr != NULL)
	{
		int num_check = constr->num_check;
		int i;

		for (i = 0; i < num_check; i++)
		{
			Node *cexpr;

			/*
			 * If this constraint hasn't been fully validated yet, we must
			 * ignore it here.
			 */
			if (!constr->check[i].ccvalid)
				continue;

			cexpr = stringToNode(constr->check[i].ccbin);

			/*
			 * Run each expression through const-simplification and
			 * canonicalization.  This is not just an optimization, but is
			 * necessary, because we will be comparing it to
			 * similarly-processed qual clauses, and may fail to detect valid
			 * matches without this.  This must match the processing done to
			 * qual clauses in preprocess_expression()!  (We can skip the
			 * stuff involving subqueries, however, since we don't allow any
			 * in check constraints.)
			 */
			cexpr = eval_const_expressions(NULL, cexpr);
			cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);

			/* Fix Vars to have the desired varno */
			if (varno != 1)
				ChangeVarNodes(cexpr, 1, varno, 0);

			/*
			 * Finally, convert to implicit-AND format (that is, a List) and
			 * append the resulting item(s) to our output list.
			 */
			result = list_concat(result, make_ands_implicit((Expr *) cexpr));
		}

		/* Add NOT NULL constraints in expression form, if requested */
		if (include_notnull && constr->has_not_null)
		{
			int natts = relation->rd_att->natts;

			for (i = 1; i <= natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(relation->rd_att, i - 1);

				if (att->attnotnull && !att->attisdropped)
				{
					NullTest *ntest = makeNode(NullTest);

					ntest->arg = (Expr *)
						makeVar(varno, i, att->atttypid, att->atttypmod, att->attcollation, 0);
					ntest->nulltesttype = IS_NOT_NULL;

					/*
					 * argisrow=false is correct even for a composite column,
					 * because attnotnull does not represent a SQL-spec IS NOT
					 * NULL test in such a case, just IS DISTINCT FROM NULL.
					 */
					ntest->argisrow = false;
					ntest->location = -1;
					result = lappend(result, ntest);
				}
			}
		}
	}

	table_close(relation, NoLock);

	return result;
}

/*
 * Exclude child relations (chunks) at execution time based on constraints.
 *
 * constraints is the list of constraint expressions of the relation
 * baserestrictinfo is the list of RestrictInfos
 */
static bool
can_exclude_chunk(List *constraints, List *baserestrictinfo)
{
	/*
	 * Regardless of the setting of constraint_exclusion, detect
	 * constant-FALSE-or-NULL restriction clauses.  Because const-folding will
	 * reduce "anything AND FALSE" to just "FALSE", any such case should
	 * result in exactly one baserestrictinfo entry.  This doesn't fire very
	 * often, but it seems cheap enough to be worth doing anyway.  (Without
	 * this, we'd miss some optimizations that 9.5 and earlier found via much
	 * more roundabout methods.)
	 */
	if (list_length(baserestrictinfo) == 1)
	{
		RestrictInfo *rinfo = (RestrictInfo *) linitial(baserestrictinfo);
		Expr *clause = rinfo->clause;

		if (clause && IsA(clause, Const) &&
			(((Const *) clause)->constisnull || !DatumGetBool(((Const *) clause)->constvalue)))
			return true;
	}

	/*
	 * The constraints are effectively ANDed together, so we can just try to
	 * refute the entire collection at once.  This may allow us to make proofs
	 * that would fail if we took them individually.
	 *
	 * Note: we use rel->baserestrictinfo, not safe_restrictions as might seem
	 * an obvious optimization.  Some of the clauses might be OR clauses that
	 * have volatile and nonvolatile subclauses, and it's OK to make
	 * deductions with the nonvolatile parts.
	 *
	 * We need strong refutation because we have to prove that the constraints
	 * would yield false, not just NULL.
	 */
	if (predicate_refuted_by(constraints, baserestrictinfo, false))
		return true;

	return false;
}

/*
 * Fetch the constraints for a relation and adjust range table indexes
 * if necessary.
 */
static void
initialize_constraints(ChunkAppendState *state, List *initial_rt_indexes)
{
	ListCell *lc_clauses, *lc_plan, *lc_relid;
	List *constraints = NIL;
	EState *estate = state->csstate.ss.ps.state;

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
		Scan *scan = ts_chunk_append_get_scan_plan(lfirst(lc_plan));
		Index initial_index = lfirst_oid(lc_relid);
		List *relation_constraints = NIL;

		if (scan != NULL && scan->scanrelid > 0)
		{
			Index rt_index = scan->scanrelid;
			RangeTblEntry *rte = rt_fetch(rt_index, estate->es_range_table);
			relation_constraints = ca_get_relation_constraints(rte->relid, rt_index, true);

			/*
			 * Adjust the RangeTableEntry indexes in the restrictinfo
			 * clauses because during planning subquery indexes may be
			 * different from the final index after flattening.
			 */
			if (rt_index != initial_index)
				ChangeVarNodes(lfirst(lc_clauses), initial_index, scan->scanrelid, 0);
		}
		constraints = lappend(constraints, relation_constraints);
	}
	state->initial_constraints = constraints;
	state->filtered_constraints = constraints;
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 * This callback is optional. Common data stored in the ScanState,
 * such as the target list and scan relation, will be shown even without
 * this callback, but the callback allows the display of additional,
 * private state.
 */
static void
chunk_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ChunkAppendState *state = (ChunkAppendState *) node;

	if (state->sort_options != NIL)
		show_sort_group_keys(state, ancestors, es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Startup Exclusion", state->startup_exclusion, es);

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Runtime Exclusion", state->runtime_exclusion, es);

	if (state->startup_exclusion)
		ExplainPropertyInteger("Chunks excluded during startup",
							   NULL,
							   list_length(state->initial_subplans) - list_length(node->custom_ps),
							   es);

	if (state->runtime_exclusion && state->runtime_number_loops > 0)
	{
		int avg_excluded = state->runtime_number_exclusions / state->runtime_number_loops;
		ExplainPropertyInteger("Chunks excluded during runtime", NULL, avg_excluded, es);
	}
}

/*
 * adjusted from postgresql explain.c
 * since we have to keep the state in custom_private our sort state
 * is in lists instead of arrays
 */
static void
show_sort_group_keys(ChunkAppendState *state, List *ancestors, ExplainState *es)
{
	Plan *plan = state->csstate.ss.ps.plan;
	List *context;
	List *result = NIL;
	StringInfoData sortkeybuf;
	bool useprefix;
	int keyno;
	int nkeys = list_length(linitial(state->sort_options));
	List *sort_indexes = linitial(state->sort_options);
	List *sort_ops = lsecond(state->sort_options);
	List *sort_collations = lthird(state->sort_options);
	List *sort_nulls = lfourth(state->sort_options);

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
#if PG13_GE
	context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);
#else
	context = set_deparse_context_planstate(es->deparse_cxt, (Node *) state, ancestors);
#endif
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber keyresno = list_nth_oid(sort_indexes, keyno);
		TargetEntry *target =
			get_tle_by_resno(castNode(CustomScan, plan)->custom_scan_tlist, keyresno);
		char *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context, useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sort_ops != NIL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   list_nth_oid(sort_ops, keyno),
								   list_nth_oid(sort_collations, keyno),
								   list_nth_oid(sort_nulls, keyno));
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ExplainPropertyList("Order", result, es);
}

/* copied verbatim from postgresql explain.c */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation,
					   bool nullsFirst)
{
	Oid sortcoltype = exprType(sortexpr);
	bool reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}
