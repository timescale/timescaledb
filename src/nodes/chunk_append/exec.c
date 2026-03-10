/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_collation.h>
#include <executor/executor.h>
#include <executor/nodeSubplan.h>
#include <fmgr.h>
#include <miscadmin.h>
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

#include "compat/compat.h"
#include "loader/lwlocks.h"
#include "nodes/chunk_append/chunk_append.h"
#include "planner/planner.h"
#include "transform.h"
#include "ts_catalog/chunk_column_stats.h"

#if PG18_GE
#include <commands/explain_format.h>
#endif

#define INVALID_SUBPLAN_INDEX (-1)
#define NO_MATCHING_SUBPLANS (-2)

typedef enum ChunkAppendSubplanState
{
	CASS_Included = 1 << 0, /* Used and not removed by startup exclusion */
	CASS_Finished = 1 << 1, /* The subplan is finished */
} ChunkAppendSubplanState;

/*
 * ParallelChunkAppendState is stored in shared memory to coordinate the
 * parallel workers.
 */
typedef struct ParallelChunkAppendState
{
	int next_plan;
	int first_partial_plan;

	/*
	 * Follows the initial subplan list.
	 */
	ChunkAppendSubplanState subplan_state[FLEXIBLE_ARRAY_MEMBER];
} ParallelChunkAppendState;

typedef struct ChunkAppendState
{
	CustomScanState csstate;
	PlanState **subplanstates;

	MemoryContext exclusion_ctx;

	int first_partial_plan;
	int current;

	Oid ht_reloid;
	bool startup_exclusion;
	bool runtime_exclusion_parent;
	bool runtime_exclusion_children;
	bool runtime_initialized;
	uint32 limit;

#ifdef USE_ASSERT_CHECKING
	bool init_done;
#endif

	/* list of subplans after planning */
	List *initial_subplans;
	/* list of constraints indexed like initial_subplans */
	List *initial_constraints;
	/* list of restrictinfo clauses indexed like initial_subplans */
	List *initial_ri_clauses;
	/* List of restrictinfo clauses on the parent hypertable */
	List *initial_parent_clauses;

	/* subplans remaining after startup exclusion (into initial_subplans) */
	Bitmapset *subplans_after_startup;

	/* subplans remaining after runtime exclusion (subset of above) */
	Bitmapset *subplans_after_runtime;
	Bitmapset *params;

	/* sort options if this append is ordered, only used for EXPLAIN */
	List *sort_options;

	/* number of loops and exclusions for EXPLAIN */
	int runtime_number_loops;
	int runtime_number_exclusions_parent;
	int runtime_number_exclusions_children;

	LWLock *lock;
	ParallelContext *pcxt;
	ParallelChunkAppendState *parallel_state;
	EState *estate;
	int eflags;
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
static void choose_next_subplan_in_worker(ChunkAppendState *state);

static bool can_exclude_chunk(List *constraints, List *baserestrictinfo);
static void do_startup_exclusion(ChunkAppendState *state);
static Node *constify_param_mutator(Node *node, void *context);

static void initialize_constraints(ChunkAppendState *state, List *initial_rt_indexes);
static LWLock *chunk_append_get_lock_pointer(void);

static void show_sort_group_keys(ChunkAppendState *planstate, List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr, Oid sortOperator, Oid collation,
								   bool nullsFirst);

static void init_subplanstates(ChunkAppendState *state, EState *estate, int eflags);

Node *
ts_chunk_append_state_create(CustomScan *cscan)
{
	ChunkAppendState *state;

	Assert(list_length(cscan->custom_private) == CAP_Count);
	List *settings = list_nth(cscan->custom_private, CAP_Settings);
	Assert(list_length(settings) == CAS_Count);

	state = (ChunkAppendState *) newNode(sizeof(ChunkAppendState), T_CustomScanState);

	state->csstate.methods = &chunk_append_state_methods;

	state->initial_subplans = cscan->custom_plans;
	state->initial_ri_clauses = list_nth(cscan->custom_private, CAP_ChunkRIClauses);
	state->sort_options = list_nth(cscan->custom_private, CAP_SortOptions);
	state->initial_parent_clauses = list_nth(cscan->custom_private, CAP_ParentClauses);

	state->startup_exclusion = list_nth_int(settings, CAS_StartupExclusion);
	state->runtime_exclusion_parent = list_nth_int(settings, CAS_RuntimeExclusionParent);
	state->runtime_exclusion_children = list_nth_int(settings, CAS_RuntimeExclusionChildren);
	state->limit = list_nth_int(settings, CAS_Limit);
	state->first_partial_plan = list_nth_int(settings, CAS_FirstPartialPath);

	state->current = INVALID_SUBPLAN_INDEX;
	state->choose_next_subplan = choose_next_subplan_non_parallel;

	state->exclusion_ctx = AllocSetContextCreate(CurrentMemoryContext,
												 "ChunkApppend exclusion",
												 ALLOCSET_DEFAULT_SIZES);

	return (Node *) state;
}

/*
 * Build the subplans_after_startup bitmap.  When startup_exclusion is
 * enabled, chunks whose constraints contradict the (now-constant)
 * restriction clauses are excluded.  Otherwise all subplans are included.
 * Parallel workers rely on this bitmap (via shared memory) to know which
 * subplans to process.
 */
static void
do_startup_exclusion(ChunkAppendState *state)
{
	state->subplans_after_startup = NULL;

	Assert(list_length(state->initial_subplans) == list_length(state->initial_ri_clauses));
	Assert(list_length(state->initial_subplans) == list_length(state->initial_constraints));

	if (!state->startup_exclusion)
	{
		/* No exclusion -- include every subplan. */
		state->subplans_after_startup =
			bms_add_range(NULL, 0, list_length(state->initial_subplans) - 1);
		return;
	}

	PlannerGlobal glob = {
		.boundParams = state->csstate.ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

	for (int i = 0; i < list_length(state->initial_subplans); i++)
	{
		Plan *subplan = list_nth(state->initial_subplans, i);
		Scan *scan = ts_chunk_append_get_scan_plan(subplan);

		if (scan != NULL && scan->scanrelid)
		{
			List *ri_clauses = list_nth(state->initial_ri_clauses, i);
			List *restrictinfos = NIL;
			ListCell *lc;

			foreach (lc, ri_clauses)
			{
				RestrictInfo *ri = makeNode(RestrictInfo);
				ri->clause = lfirst(lc);
				restrictinfos = lappend(restrictinfos, ri);
			}
			restrictinfos = ts_constify_restrictinfos(&root, restrictinfos);

			if (can_exclude_chunk(list_nth(state->initial_constraints, i), restrictinfos))
			{
				continue;
			}
		}

		state->subplans_after_startup = bms_add_member(state->subplans_after_startup, i);
	}
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

	initialize_constraints(state, list_nth(cscan->custom_private, CAP_RTIndexes));

	/*
	 * In parallel mode, the leader performs startup exclusion and ships the
	 * result to workers via SUBPLAN_STATE_INCLUDED flags in shared memory.
	 * Workers must not perform exclusion themselves because they could disagree
	 * with the leader (e.g., due to volatile functions), causing index
	 * mismatches in the shared subplan coordination. See PR #5857.
	 *
	 * Workers defer initialization to chunk_append_initialize_worker, which
	 * reads the exclusion result from shared memory. We store estate and eflags
	 * here for that later initialization.
	 *
	 * The force_parallel_mode can run a non-parallel_aware ChunkAppend inside a
	 * parallel worker. In that case there is no shared memory coordination, so
	 * we fall through to the normal do_startup_exclusion + init_subplanstates
	 * path.
	 */
	if (IsParallelWorker() && node->ss.ps.plan->parallel_aware)
	{
		state->estate = estate;
		state->eflags = eflags;
		return;
	}

	do_startup_exclusion(state);
	init_subplanstates(state, estate, eflags);
}

/*
 * Initialize the subplanstates array from initial_subplans, skipping
 * entries not in subplans_after_startup.
 */
static void
init_subplanstates(ChunkAppendState *state, EState *estate, int eflags)
{
#ifdef USE_ASSERT_CHECKING
	Assert(state->init_done == false);
	state->init_done = true;
#endif

	if (bms_is_empty(state->subplans_after_startup))
	{
		state->current = NO_MATCHING_SUBPLANS;
		return;
	}

	state->subplanstates =
		(PlanState **) palloc0(list_length(state->initial_subplans) * sizeof(PlanState *));

	for (int i = bms_next_member(state->subplans_after_startup, -1); i >= 0;
		 i = bms_next_member(state->subplans_after_startup, i))
	{
		Plan *subplan = list_nth(state->initial_subplans, i);

		/*
		 * we use an array for the states but put it in custom_ps as well
		 * so explain and planstate_tree_walker can find it
		 */
		state->subplanstates[i] = ExecInitNode(subplan, estate, eflags);
		state->csstate.custom_ps = lappend(state->csstate.custom_ps, state->subplanstates[i]);

		if (state->limit)
		{
			ExecSetTupleBound(state->limit, state->subplanstates[i]);
		}
	}

	state->subplans_after_runtime = bms_copy(state->subplans_after_startup);

	if (state->runtime_exclusion_parent || state->runtime_exclusion_children)
	{
		int first = bms_next_member(state->subplans_after_startup, -1);
		Assert(first >= 0);
		state->params = state->subplanstates[first]->plan->allParam;
		/*
		 * make sure all params are initialized for runtime exclusion
		 */
		state->csstate.ss.ps.chgParam = bms_copy(state->subplanstates[first]->plan->allParam);
	}
}

static bool
can_exclude_constraints_using_clauses(ChunkAppendState *state, List *constraints, List *clauses,
									  PlannerInfo *root, PlanState *ps)
{
	bool can_exclude;
	ListCell *lc;
	MemoryContext old = MemoryContextSwitchTo(state->exclusion_ctx);
	List *restrictinfos = NIL;

	foreach (lc, clauses)
	{
		RestrictInfo *ri = makeNode(RestrictInfo);
		ri->clause = lfirst(lc);
		restrictinfos = lappend(restrictinfos, ri);
	}
	restrictinfos = ts_constify_restrictinfo_params(root, ps->state, restrictinfos);

	can_exclude = can_exclude_chunk(constraints, restrictinfos);

	MemoryContextReset(state->exclusion_ctx);
	MemoryContextSwitchTo(old);
	return can_exclude;
}

/*
 * Build subplans_after_runtime bitmap for runtime exclusion, considering
 * only subplans in subplans_after_startup.
 */
static void
do_runtime_exclusion(ChunkAppendState *state)
{
	PlannerGlobal glob = {
		.boundParams = state->csstate.ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};

	state->runtime_initialized = true;

	if (bms_is_empty(state->subplans_after_startup))
	{
		return;
	}

	/*
	 * Reset subplans_after_runtime to all startup-included subplans,
	 * then remove excluded ones.
	 */
	bms_free(state->subplans_after_runtime);
	state->subplans_after_runtime = bms_copy(state->subplans_after_startup);

	state->runtime_number_loops++;

	if (state->runtime_exclusion_parent)
	{
		/*
		 * Try to exclude all chunks using the parent clauses.
		 * All constraints are true but exclusion can still happen
		 * because of things like ANY(empty set) and NULL inference.
		 */
		if (can_exclude_constraints_using_clauses(state,
												  list_make1(makeBoolConst(true, false)),
												  state->initial_parent_clauses,
												  &root,
												  &state->csstate.ss.ps))
		{
			state->runtime_number_exclusions_parent++;
			bms_free(state->subplans_after_runtime);
			state->subplans_after_runtime = NULL;
			return;
		}
	}

	if (!state->runtime_exclusion_children)
	{
		return;
	}

	for (int i = bms_next_member(state->subplans_after_startup, -1); i >= 0;
		 i = bms_next_member(state->subplans_after_startup, i))
	{
		PlanState *ps = state->subplanstates[i];
		Scan *scan = ts_chunk_append_get_scan_plan(ps->plan);

		if (scan == NULL || scan->scanrelid == 0)
		{
			continue;
		}

		List *ri_clauses = list_nth(state->initial_ri_clauses, i);
		List *constraints = list_nth(state->initial_constraints, i);

		if (can_exclude_constraints_using_clauses(state, constraints, ri_clauses, &root, ps))
		{
			state->subplans_after_runtime = bms_del_member(state->subplans_after_runtime, i);
			state->runtime_number_exclusions_children++;
		}
	}
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

	Assert(state->init_done == true);

	if (state->current == INVALID_SUBPLAN_INDEX)
		state->choose_next_subplan(state);

	while (true)
	{
		PlanState *subnode;

		CHECK_FOR_INTERRUPTS();

		if (state->current == NO_MATCHING_SUBPLANS)
			return ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

		Assert(state->current >= 0 && state->current < list_length(state->initial_subplans));
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
	{
		return NO_MATCHING_SUBPLANS;
	}

	if (!state->runtime_initialized &&
		(state->runtime_exclusion_parent || state->runtime_exclusion_children))
	{
		do_runtime_exclusion(state);
	}

	int next = bms_next_member(state->subplans_after_runtime, last_plan);
	if (next < 0)
	{
		return NO_MATCHING_SUBPLANS;
	}
	return next;
}

static void
choose_next_subplan_non_parallel(ChunkAppendState *state)
{
	state->current = get_next_subplan(state, state->current);
}

static void
choose_next_subplan_in_worker(ChunkAppendState *worker_state)
{
	ParallelChunkAppendState *parallel_state = worker_state->parallel_state;
	int next_plan;
	int start;

	LWLockAcquire(worker_state->lock, LW_EXCLUSIVE);

	/* mark just completed subplan as finished */
	if (worker_state->current >= 0)
	{
		parallel_state->subplan_state[worker_state->current] =
			ts_set_flags_32(parallel_state->subplan_state[worker_state->current], CASS_Finished);
	}

	if (parallel_state->next_plan == INVALID_SUBPLAN_INDEX)
	{
		next_plan = get_next_subplan(worker_state, INVALID_SUBPLAN_INDEX);
	}
	else
	{
		next_plan = parallel_state->next_plan;
	}

	if (next_plan == NO_MATCHING_SUBPLANS)
	{
		/* all subplans are finished */
		parallel_state->next_plan = NO_MATCHING_SUBPLANS;
		worker_state->current = NO_MATCHING_SUBPLANS;
		LWLockRelease(worker_state->lock);
		return;
	}

	start = next_plan;

	/* skip finished subplans */
	while (ts_flags_are_set_32(parallel_state->subplan_state[next_plan], CASS_Finished))
	{
		next_plan = get_next_subplan(worker_state, next_plan);

		/* wrap around if we reach end of subplan list */
		if (next_plan < 0)
			next_plan = get_next_subplan(worker_state, INVALID_SUBPLAN_INDEX);

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
			parallel_state->next_plan = NO_MATCHING_SUBPLANS;
			worker_state->current = NO_MATCHING_SUBPLANS;
			LWLockRelease(worker_state->lock);
			return;
		}
	}

	Assert(next_plan >= 0 && next_plan < list_length(worker_state->initial_subplans));
	worker_state->current = next_plan;

	/*
	 * if this is not a partial plan we mark it as finished
	 * immediately so it does not get assigned another worker
	 */
	if (next_plan < parallel_state->first_partial_plan)
		parallel_state->subplan_state[next_plan] =
			ts_set_flags_32(parallel_state->subplan_state[next_plan], CASS_Finished);

	/* advance next_plan for next worker */
	parallel_state->next_plan = get_next_subplan(worker_state, worker_state->current);

	/*
	 * if we reach the end of the list of subplans we set next_plan
	 * to INVALID_SUBPLAN_INDEX to allow rechecking unfinished subplans
	 * on next call
	 */
	if (parallel_state->next_plan < 0)
		parallel_state->next_plan = INVALID_SUBPLAN_INDEX;

	LWLockRelease(worker_state->lock);
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

	/*
	 * End all initialized subplans. We iterate subplans_after_startup
	 * (not subplans_after_runtime) because ExecInitNode was called
	 * for every startup-included subplan.
	 */
	for (int i = bms_next_member(state->subplans_after_startup, -1); i >= 0;
		 i = bms_next_member(state->subplans_after_startup, i))
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

	for (int i = bms_next_member(state->subplans_after_startup, -1); i >= 0;
		 i = bms_next_member(state->subplans_after_startup, i))
	{
		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(state->subplanstates[i], node->ss.ps.chgParam);

		ExecReScan(state->subplanstates[i]);
	}
	state->current = INVALID_SUBPLAN_INDEX;

	/*
	 * detect changed params and reset runtime exclusion state
	 */
	if ((state->runtime_exclusion_parent || state->runtime_exclusion_children) &&
		bms_overlap(node->ss.ps.chgParam, state->params))
	{
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
	return add_size(offsetof(ParallelChunkAppendState, subplan_state),
					sizeof(uint32) * list_length(state->initial_subplans));
}

/*
 * Initialize the parallel state.
 */
static void
init_parallel_state(ChunkAppendState *state, ParallelChunkAppendState *parallel_state)
{
	Assert(state != NULL);
	Assert(parallel_state != NULL);
	Assert(state->csstate.pscan_len > 0);

	/* The parallel worker state has to be (re-)initialized by the parallel leader */
	Assert(!IsParallelWorker());

	memset(parallel_state, 0, state->csstate.pscan_len);

	parallel_state->next_plan = INVALID_SUBPLAN_INDEX;
	parallel_state->first_partial_plan = state->first_partial_plan;

	/* Mark active subplans in parallel state */
	for (int plan = bms_next_member(state->subplans_after_startup, -1); plan >= 0;
		 plan = bms_next_member(state->subplans_after_startup, plan))
	{
		parallel_state->subplan_state[plan] =
			ts_set_flags_32(parallel_state->subplan_state[plan], CASS_Included);
	}
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
	ParallelChunkAppendState *parallel_state = (ParallelChunkAppendState *) coordinate;
	init_parallel_state(state, parallel_state);

	state->lock = chunk_append_get_lock_pointer();

	/*
	 * Leader should use the same subplan selection as normal worker threads. If the user wishes to
	 * disallow running plans on the leader they should do so via the parallel_leader_participation
	 * GUC.
	 */
	state->choose_next_subplan = choose_next_subplan_in_worker;
	state->current = INVALID_SUBPLAN_INDEX;
	state->pcxt = pcxt;
	state->parallel_state = parallel_state;
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
	ParallelChunkAppendState *parallel_state = (ParallelChunkAppendState *) coordinate;
	init_parallel_state(state, parallel_state);
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
	ChunkAppendState *worker_state = (ChunkAppendState *) node;
	ParallelChunkAppendState *parallel_state = (ParallelChunkAppendState *) coordinate;

	Assert(IsParallelWorker());
	Assert(node->ss.ps.plan->parallel_aware);
	Assert(parallel_state != NULL);
	Assert(worker_state->estate != NULL);

	/*
	 * Read the leader's startup exclusion result from shared memory and
	 * rebuild subplans_after_startup, then initialize subplanstates.
	 */
	worker_state->subplans_after_startup = NULL;
	for (int plan = 0; plan < list_length(worker_state->initial_subplans); plan++)
	{
		if (ts_flags_are_set_32(parallel_state->subplan_state[plan], CASS_Included))
			worker_state->subplans_after_startup =
				bms_add_member(worker_state->subplans_after_startup, plan);
	}

	worker_state->lock = chunk_append_get_lock_pointer();
	worker_state->choose_next_subplan = choose_next_subplan_in_worker;
	worker_state->current = INVALID_SUBPLAN_INDEX;
	worker_state->parallel_state = parallel_state;

	init_subplanstates(worker_state, worker_state->estate, worker_state->eflags);
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
List *
ts_constify_restrictinfos(PlannerInfo *root, List *restrictinfos)
{
	List *additional_list = NIL;

	ListCell *lc;

	foreach (lc, restrictinfos)
	{
		RestrictInfo *rinfo = lfirst(lc);
		Expr *constified = (Expr *) estimate_expression_value(root, (Node *) rinfo->clause);

		/*
		 * Note that we have to use equal() here, because the expression mutators
		 * always return a deep copy of the expression tree, even if nothing was
		 * modified.
		 */
		if (!equal(rinfo->clause, constified))
		{
			/*
			 * We have constified something, so try applying the time_bucket
			 * transformations again. This might allow us to exclude chunks
			 * based on a parameterized time_bucket expression.
			 */
			Expr *additional_clause = ts_transform_time_bucket_comparison(constified);
			if (additional_clause != NULL)
			{
				/*
				 * We successfully added a filter clause based on a
				 * parameterized time_bucket comparison, but it might contain
				 * stable operators like comparison of timestamp to timestamptz,
				 * so we have to evaluate them as well.
				 */
				additional_clause = ts_transform_cross_datatype_comparison(additional_clause);
				additional_clause =
					(Expr *) estimate_expression_value(root, (Node *) additional_clause);
				additional_list =
					lappend(additional_list, make_simple_restrictinfo(root, additional_clause));
			}
		}
		rinfo->clause = constified;
	}

	return list_concat(restrictinfos, additional_list);
}

List *
ts_constify_restrictinfo_params(PlannerInfo *root, EState *state, List *restrictinfos)
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
				// reload prm as it may have been changed by ExecSetParamPlan call above.
				prm = estate->es_param_exec_vals[param->paramid];
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

		if (ts_guc_enable_chunk_skipping)
		{
			/* Add column range min/max ranges in 'CHECK CONSTRAINT' form */
			result = list_concat(result,
								 ts_chunk_column_stats_construct_check_constraints(relation,
																				   relationObjectId,
																				   varno));
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
	 * Detect constant-FALSE-or-NULL restriction clauses. If we have such a
	 * clause, no rows from the chunk are going to match. Unlike the postgres
	 * analog of this code in relation_excluded_by_constraints, we can't expect
	 * a single const false restrictinfo in this case, because we don't try to
	 * fold the restrictinfos after evaluating the mutable functions.
	 * We have to check this separately from the subsequent predicate_refuted_by.
	 * That function can also work with the normal CHECK constraints, and they
	 * don't fail if the constraint evaluates to null given the restriction info.
	 * That's why it has to prove that the CHECK constraint evaluates to false,
	 * and this doesn't follow from having a const null restrictinfo.
	 */
	ListCell *lc;
	foreach (lc, baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
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
	List *constraints = NIL;
	EState *estate = state->csstate.ss.ps.state;

	if (initial_rt_indexes == NIL)
		return;

	Assert(list_length(state->initial_subplans) == list_length(state->initial_ri_clauses));
	Assert(list_length(state->initial_subplans) == list_length(initial_rt_indexes));

	for (int i = 0; i < list_length(state->initial_subplans); i++)
	{
		Plan *subplan = list_nth(state->initial_subplans, i);
		Scan *scan = ts_chunk_append_get_scan_plan(subplan);
		Index initial_index = list_nth_oid(initial_rt_indexes, i);
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
				ChangeVarNodes(list_nth(state->initial_ri_clauses, i),
							   initial_index,
							   scan->scanrelid,
							   0);
		}
		constraints = lappend(constraints, relation_constraints);
	}
	state->initial_constraints = constraints;
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
		ExplainPropertyBool("Runtime Exclusion",
							(state->runtime_exclusion_parent || state->runtime_exclusion_children),
							es);

	if (state->startup_exclusion)
		ExplainPropertyInteger("Chunks excluded during startup",
							   NULL,
							   list_length(state->initial_subplans) - list_length(node->custom_ps),
							   es);

	if (state->runtime_exclusion_parent && state->runtime_number_loops > 0)
	{
		int avg_excluded = state->runtime_number_exclusions_parent / state->runtime_number_loops;
		ExplainPropertyInteger("Hypertables excluded during runtime", NULL, avg_excluded, es);
	}

	if (state->runtime_exclusion_children && state->runtime_number_loops > 0)
	{
		int avg_excluded = state->runtime_number_exclusions_children / state->runtime_number_loops;
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
	context = set_deparse_context_plan(es->deparse_cxt, plan, ancestors);
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
