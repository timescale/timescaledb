/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <utils/snapmgr.h>

#include "compat/compat.h"
#include "chunk_tuple_routing.h"
#include "cross_module_fn.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "indexing.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/modify_hypertable.h"

#if PG18_GE
#include <commands/explain_format.h>
#endif

static bool
should_use_direct_compress(ModifyHypertableState *state)
{
	if (!ts_guc_enable_optimizations)
	{
		return false;
	}

	ModifyTableState *mtstate = linitial_node(ModifyTableState, state->cscan_state.custom_ps);
	ResultRelInfo *resultRelInfo = mtstate->resultRelInfo;
	Hypertable *ht = state->ctr->hypertable;

	if (!ts_guc_enable_direct_compress_insert && !TS_HYPERTABLE_HAS_DIRECT_COMPRESS_ENABLED(ht))
	{
		return false;
	}

	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		return false;
	}

	if (resultRelInfo->ri_TrigDesc)
	{
		ereport(WARNING,
				(errmsg("disabling direct compress because the destination table has triggers")));
		return false;
	}

	if (ts_indexing_relation_has_primary_or_unique_index(state->ctr->root_rel))
	{
		ereport(WARNING,
				(errmsg("disabling direct compress because the destination table has unique "
						"constraints")));
		return false;
	}

	if (ts_indexing_relation_has_exclusion_constraint(state->ctr->root_rel))
	{
		ereport(WARNING,
				(errmsg("disabling direct compress because the destination table has exclusion "
						"constraints")));
		return false;
	}

	Plan *subplan = mtstate->ps.plan->lefttree;
	if (subplan->plan_rows < 10)
	{
		ereport(WARNING, (errmsg("disabling direct compress because of too small batch size")));
		return false;
	}

	return true;
}

static void modify_hypertable_init_child_plan_states(CustomScanState *node);

/*
 * ModifyHypertable is a plan node that implements DML for hypertables.
 * It is a wrapper around the ModifyTable plan node that calls the wrapped ModifyTable
 * plan.
 */
static void
modify_hypertable_begin(CustomScanState *node, EState *estate, int eflags)
{
	ModifyHypertableState *modify_hypertable_state = (ModifyHypertableState *) node;
	ModifyTable *modify_table_plan = castNode(ModifyTable, &modify_hypertable_state->mt->plan);

	/*
	 * To make statement trigger defined on the hypertable work
	 * we need to set the hypertable as the rootRelation otherwise
	 * statement trigger defined only on the hypertable will not fire.
	 */
	if (modify_table_plan->operation == CMD_DELETE || modify_table_plan->operation == CMD_UPDATE ||
		modify_table_plan->operation == CMD_MERGE)
	{
		modify_table_plan->rootRelation = modify_table_plan->nominalRelation;
	}

	Oid result_relid = rt_fetch(modify_table_plan->nominalRelation, estate->es_range_table)->relid;
	modify_hypertable_state->ht =
		ts_hypertable_cache_get_cache_and_entry(result_relid,
												CACHE_FLAG_MISSING_OK,
												&modify_hypertable_state->ht_cache);

	/*
	 * If we are inserting into a chunk directly, rri will point to the chunk
	 * itself, so we need to get the hypertable from the chunk.
	 */
	if (!modify_hypertable_state->ht)
	{
		Chunk *chunk = ts_chunk_get_by_relid(result_relid, true);
		modify_hypertable_state->ht =
			ts_hypertable_cache_get_entry(modify_hypertable_state->ht_cache,
										  chunk->hypertable_relid,
										  CACHE_FLAG_NONE);
	}
	modify_hypertable_state->has_continuous_aggregate =
		ts_hypertable_has_continuous_aggregates(modify_hypertable_state->ht->fd.id);

	/*
	 * The ModifyTable node itself must be initialized now, so that it's properly
	 * added to the es_auxmodifytables list. For secondary data-modifying CTEs,
	 * this can be the last time our code is called before ExecPostprocessPlan(),
	 * if the CTE is not referenced by the main query.
	 *
	 * The actual initialization of the child plan states is deferred until after
	 * we decompress the data that might potentially be involved in DML operations.
	 * We substitute them with a dummy Result here, so that the Postgres code
	 * can work.
	 */
	modify_hypertable_state->deferred_eflags = eflags;
	modify_hypertable_state->deferred_modify_table_subplan = outerPlan(modify_table_plan);

	Plan *dummy_child = (Plan *) makeNode(Result);
	castNode(Result, dummy_child)->resconstantqual =
		(Node *) list_make1(makeBoolConst(false, false));

	/*
	 * The child targetlist can contain Aggrefs which are not allowed on a Result
	 * targetlist. Just replace every expression with a null constant of the
	 * same type.
	 */
	dummy_child->targetlist =
		copyObject(modify_hypertable_state->deferred_modify_table_subplan->targetlist);
	ListCell *lc;
	foreach (lc, dummy_child->targetlist)
	{
		TargetEntry *entry = lfirst(lc);
		Node *expr = (Node *) entry->expr;
		entry->expr = (Expr *) makeNullConst(exprType(expr), exprTypmod(expr), exprCollation(expr));
	}

	/*
	 * Initialize the Postgres ModifyTableState with dummy Result plan as a
	 * child. The plan nodes here might come from the plan cache for prepared
	 * statements, and they outlive a single query. We shouldn't change them
	 * directly, so make a copy.
	 */
	ModifyTable *modify_table_plan_copy = makeNode(ModifyTable);
	memcpy(modify_table_plan_copy, modify_table_plan, sizeof(ModifyTable));
	outerPlan(modify_table_plan_copy) = dummy_child;
	PlanState *modify_table_state = ExecInitNode((Plan *) modify_table_plan_copy, estate, eflags);

	node->custom_ps = list_make1(modify_table_state);

	/*
	 * If Postgres adds our node to the secondary data-modifying CTE list, it
	 * adds just the Postgres ModifyTableState. Make it point to our
	 * ModifyHypertableState instead, so that our custom code is called.
	 */
	if (list_length(estate->es_auxmodifytables) > 0 &&
		linitial(estate->es_auxmodifytables) == modify_table_state)
	{
		linitial(estate->es_auxmodifytables) = node;
	}

	/*
	 * In some cases, the plain deferred initialization from exec doesn't work,
	 * we handle these below.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		/*
		 * With plain EXPLAIN, the node is not actually executed, so we have to
		 * finish the initialization now.
		 */
		modify_hypertable_init_child_plan_states(node);
	}
}

/*
 * Initialize the child plan states after we have decompressed the data that can
 * potentially be involved in DML operations. This is done to delay the
 * initialization of scans over uncompressed chunk tables until after
 * decompression, so that they properly pick up the decompressed data.
 */
static void
modify_hypertable_init_child_plan_states(CustomScanState *node)
{
	EState *estate = node->ss.ps.state;

	ModifyHypertableState *modify_hypertable_state = (ModifyHypertableState *) node;

	Assert(modify_hypertable_state->deferred_modify_table_subplan != NULL);

	PlanState *subplan_state = ExecInitNode(modify_hypertable_state->deferred_modify_table_subplan,
											estate,
											modify_hypertable_state->deferred_eflags);

	ModifyTableState *modify_table_state = castNode(ModifyTableState, linitial(node->custom_ps));

	outerPlanState(modify_table_state) = subplan_state;
	outerPlan(modify_table_state->ps.plan) = subplan_state->plan;

	modify_hypertable_state->deferred_modify_table_subplan = NULL;

	if (modify_table_state->operation == CMD_INSERT || modify_table_state->operation == CMD_MERGE)
	{
		/* setup chunk tuple routing state for INSERT/MERGE */
		modify_hypertable_state->ctr =
			ts_chunk_tuple_routing_create(estate,
										  modify_hypertable_state->ht,
										  modify_table_state->resultRelInfo);
		modify_hypertable_state->ctr->mht_state = modify_hypertable_state;

		if (modify_table_state->operation == CMD_INSERT &&
			should_use_direct_compress(modify_hypertable_state))
		{
			modify_hypertable_state->columnstore_insert = true;
			modify_hypertable_state->ctr->create_compressed_chunk = true;
		}

		/* setup per tuple exprcontext for tuple routing */
		if (!estate->es_per_tuple_exprcontext)
		{
			estate->es_per_tuple_exprcontext = CreateExprContext(estate);
		}
	}
}

static TupleTableSlot *
modify_hypertable_exec(CustomScanState *node)
{
	ModifyHypertableState *modify_hypertable_state = (ModifyHypertableState *) node;

	if (modify_hypertable_state->deferred_modify_table_subplan != NULL)
	{
		EState *estate = node->ss.ps.state;
		CmdType op = modify_hypertable_state->mt->operation;

		/*
		 * For UPDATE/DELETE/MERGE on compressed hypertable, decompress chunks and
		 * move rows to uncompressed chunks. For MERGE, decompression is needed
		 * even for DO NOTHING or INSERT-only actions because the join evaluation
		 * must see the actual rows to correctly determine matched vs not-matched.
		 */
		if (op == CMD_DELETE || op == CMD_UPDATE || op == CMD_MERGE)
		{
			/* Modify snapshot only if something got decompressed */
			if (ts_cm_functions->decompress_target_segments &&
				ts_cm_functions->decompress_target_segments(modify_hypertable_state))
			{
				modify_hypertable_state->comp_chunks_processed = true;
				/*
				 * save snapshot set during ExecutorStart(), since this is the same
				 * snapshot used to SeqScan of uncompressed chunks
				 */
				modify_hypertable_state->snapshot = estate->es_snapshot;
				CommandCounterIncrement();
				/* use a static copy of current transaction snapshot
				 * this needs to be a copy so we don't read trigger updates
				 */
				estate->es_snapshot = RegisterSnapshot(GetTransactionSnapshot());
				/* mark rows visible */
				estate->es_output_cid = GetCurrentCommandId(true);

				if (ts_guc_max_tuples_decompressed_per_dml > 0 &&
					modify_hypertable_state->tuples_decompressed >
						ts_guc_max_tuples_decompressed_per_dml)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
							 errmsg("tuple decompression limit exceeded by operation"),
							 errdetail("current limit: %d, tuples decompressed: %lld",
									   ts_guc_max_tuples_decompressed_per_dml,
									   (long long int)
										   modify_hypertable_state->tuples_decompressed),
							 errhint("Consider increasing "
									 "timescaledb.max_tuples_decompressed_per_dml_transaction "
									 "or set to 0 (unlimited).")));
				}
			}
			/* Account for tuples deleted via batch DELETE in compressed chunks */
			if (op == CMD_DELETE && modify_hypertable_state->tuples_deleted > 0)
			{
				estate->es_processed += modify_hypertable_state->tuples_deleted;
			}
		}

		modify_hypertable_init_child_plan_states(node);
	}
	Assert(modify_hypertable_state->deferred_modify_table_subplan == NULL);

	ModifyTableState *modify_table_state = linitial_node(ModifyTableState, node->custom_ps);

	/*
	 * The wrapped ModifyTable is not reached through ExecProcNode, so its
	 * instrumentation is not ticked automatically. Bracket the call manually
	 * so the node accumulates its own timings/row counts. Keeping a separate
	 * Instrumentation struct here (instead of aliasing the CustomScan's) is
	 * what makes it safe for extensions that call ExplainPrintPlan at
	 * arbitrary points (see issues #7583 and #8531).
	 */
	if (modify_table_state->ps.instrument)
	{
		InstrStartNode(modify_table_state->ps.instrument);
	}

	TupleTableSlot *result = ExecModifyTable(node, &modify_table_state->ps);

	if (modify_table_state->ps.instrument)
	{
		InstrStopNode(modify_table_state->ps.instrument, TupIsNull(result) ? 0.0 : 1.0);
	}

	return result;
}

static void
modify_hypertable_end(CustomScanState *node)
{
	ModifyHypertableState *state = (ModifyHypertableState *) node;

	/*
	 * Restore targetlists that were temporarily nullified during EXPLAIN
	 * VERBOSE (see modify_hypertable_explain). This prevents corruption of
	 * cached plans for prepared statements.
	 */
	if (state->explain_saved_tlist)
	{
		ModifyTableState *mtstate = linitial_node(ModifyTableState, node->custom_ps);
		Plan *lefttree = mtstate->ps.plan->lefttree;
		lefttree->targetlist = state->explain_saved_tlist;
		if (IsA(lefttree, CustomScan) && state->explain_saved_custom_scan_tlist)
		{
			castNode(CustomScan, lefttree)->custom_scan_tlist =
				state->explain_saved_custom_scan_tlist;
		}
		state->explain_saved_tlist = NULL;
		state->explain_saved_custom_scan_tlist = NULL;
	}

	/* compressor is flushed in ExecModifyTable */
	Assert(!state->compressor);

	ExecEndNode(linitial(node->custom_ps));

	if (state->ctr)
	{
		ts_chunk_tuple_routing_destroy(state->ctr);
	}

	ts_cache_release(&state->ht_cache);
}

static void
modify_hypertable_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

/*
 * Check if the plan is a ChunkAppend, possibly wrapped in one or more
 * Result nodes (for projection and/or pseudoconstant gating quals like EXISTS).
 */
static bool
is_chunk_append_or_projection(Plan *plan)
{
	while (IsA(plan, Result) && plan->lefttree != NULL)
	{
		plan = plan->lefttree;
	}
	return ts_is_chunk_append_plan(plan);
}

static void
modify_hypertable_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ModifyHypertableState *state = (ModifyHypertableState *) node;
	ModifyTableState *mtstate = linitial_node(ModifyTableState, node->custom_ps);

	/*
	 * The targetlist for this node will have references that cannot be resolved by
	 * EXPLAIN. So for EXPLAIN VERBOSE we clear the targetlist so that EXPLAIN does not
	 * complain. PostgreSQL does something equivalent and does not print the targetlist
	 * for ModifyTable for EXPLAIN VERBOSE.
	 *
	 * We save the original pointers and restore them in modify_hypertable_end
	 * to avoid corrupting cached Plan trees (e.g. for prepared statements).
	 */
	const CmdType operation = ((ModifyTable *) mtstate->ps.plan)->operation;
	if ((operation == CMD_MERGE || operation == CMD_DELETE) && es->verbose &&
		is_chunk_append_or_projection(mtstate->ps.plan->lefttree))
	{
		Plan *lefttree = mtstate->ps.plan->lefttree;
		state->explain_saved_tlist = lefttree->targetlist;
		lefttree->targetlist = NULL;

		if (IsA(lefttree, CustomScan))
		{
			state->explain_saved_custom_scan_tlist =
				castNode(CustomScan, lefttree)->custom_scan_tlist;
			castNode(CustomScan, lefttree)->custom_scan_tlist = NULL;
		}
	}

	/*
	 * INSERT .. ON CONFLICT statements record a couple of metrics on the
	 * wrapped ModifyTable node. Surface them on the ModifyHypertable
	 * CustomScan so they show up in EXPLAIN output for this node too.
	 *
	 * We intentionally do NOT alias mtstate->ps.instrument to the CustomScan's
	 * instrument: sharing the Instrumentation struct caused InstrStartNode to
	 * be invoked twice on the same struct when an extension calls
	 * ExplainPrintPlan before execution finishes (issues #7583 and #8531).
	 * mtstate's own instrument is ticked by modify_hypertable_exec.
	 */
	if (mtstate->ps.instrument && node->ss.ps.instrument)
	{
		node->ss.ps.instrument->ntuples2 = mtstate->ps.instrument->ntuples2;
		node->ss.ps.instrument->nfiltered1 = mtstate->ps.instrument->nfiltered1;
	}

	/*
	 * For INSERT we have to read the number of decompressed batches and
	 * tuples from the ChunkTupleRouting state below the ModifyTable.
	 */
	if ((mtstate->operation == CMD_INSERT || mtstate->operation == CMD_MERGE) &&
		outerPlanState(mtstate))
	{
		SharedCounters *counters = state->ctr->counters;

		state->batches_deleted += counters->batches_deleted;
		state->batches_filtered_decompressed += counters->batches_filtered_decompressed;
		state->batches_decompressed += counters->batches_decompressed;
		state->tuples_decompressed += counters->tuples_decompressed;
		state->batches_scanned += counters->batches_scanned;
		state->batches_checked_by_bloom += counters->batches_checked_by_bloom;
		state->batches_pruned_by_bloom += counters->batches_pruned_by_bloom;
		state->batches_without_bloom += counters->batches_without_bloom;
		state->batches_bloom_false_positives += counters->batches_bloom_false_positives;
	}
	if (state->batches_scanned > 0)
	{
		ExplainPropertyInteger("Batches scanned", NULL, state->batches_scanned, es);
	}
	if (state->batches_filtered_compressed > 0)
	{
		ExplainPropertyInteger("Compressed batches filtered",
							   NULL,
							   state->batches_filtered_compressed,
							   es);
	}
	if (state->batches_filtered_decompressed > 0)
	{
		ExplainPropertyInteger("Batches filtered after decompression",
							   NULL,
							   state->batches_filtered_decompressed,
							   es);
	}
	if (state->batches_decompressed > 0)
	{
		ExplainPropertyInteger("Batches decompressed", NULL, state->batches_decompressed, es);
	}
	if (state->tuples_decompressed > 0)
	{
		ExplainPropertyInteger("Tuples decompressed", NULL, state->tuples_decompressed, es);
	}
	if (state->batches_deleted > 0)
	{
		ExplainPropertyInteger("Batches deleted", NULL, state->batches_deleted, es);
	}
	if (state->batches_checked_by_bloom > 0)
	{
		ExplainPropertyInteger("Batches checked by bloom",
							   NULL,
							   state->batches_checked_by_bloom,
							   es);
	}
	if (state->batches_pruned_by_bloom > 0)
	{
		ExplainPropertyInteger("Batches pruned by bloom", NULL, state->batches_pruned_by_bloom, es);
	}
	if (state->batches_without_bloom > 0)
	{
		ExplainPropertyInteger("Batches without bloom", NULL, state->batches_without_bloom, es);
	}
	if (state->batches_bloom_false_positives > 0)
	{
		ExplainPropertyInteger("Batches bloom false positives",
							   NULL,
							   state->batches_bloom_false_positives,
							   es);
	}
	if (ts_guc_enable_direct_compress_insert && state->mt->operation == CMD_INSERT)
	{
		ExplainPropertyBool("Direct Compress", state->columnstore_insert, es);
	}
}

static CustomExecMethods modify_hypertable_state_methods = {
	.CustomName = "ModifyHypertableState",
	.BeginCustomScan = modify_hypertable_begin,
	.EndCustomScan = modify_hypertable_end,
	.ExecCustomScan = modify_hypertable_exec,
	.ReScanCustomScan = modify_hypertable_rescan,
	.ExplainCustomScan = modify_hypertable_explain,
};

static Node *
modify_hypertable_state_create(CustomScan *cscan)
{
	ModifyHypertableState *state;
	ModifyTable *mt = castNode(ModifyTable, linitial(cscan->custom_plans));

	state = (ModifyHypertableState *) newNode(sizeof(ModifyHypertableState), T_CustomScanState);
	state->cscan_state.methods = &modify_hypertable_state_methods;
	state->mt = mt;
	state->mt->arbiterIndexes = linitial(cscan->custom_private);

	return (Node *) state;
}

static CustomScanMethods modify_hypertable_plan_methods = {
	.CustomName = "ModifyHypertable",
	.CreateCustomScanState = modify_hypertable_state_create,
};

bool
ts_is_modify_hypertable_plan(Plan *plan)
{
	return IsA(plan, CustomScan) &&
		   castNode(CustomScan, plan)->methods == &modify_hypertable_plan_methods;
}

/*
 * Make a targetlist to meet CustomScan expectations.
 *
 * When a CustomScan isn't scanning a real relation (scanrelid=0), it will build
 * a virtual TupleDesc for the scan "input" based on custom_scan_tlist. The
 * "output" targetlist is then expected to reference the attributes of the
 * input's TupleDesc. Without projection, the targetlist will be only Vars with
 * varno set to INDEX_VAR (to indicate reference to the TupleDesc instead of a
 * real relation) and matching the order of the attributes in the TupleDesc.
 *
 * Any other order, or non-Vars, will lead to the CustomScan performing
 * projection.
 *
 * Since the CustomScan for hypertable insert just wraps ModifyTable, no
 * projection is needed, so we'll build a targetlist to avoid this.
 */
static List *
make_var_targetlist(const List *tlist)
{
	List *new_tlist = NIL;
	ListCell *lc;
	int resno = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var *var = makeVarFromTargetEntry(INDEX_VAR, tle);

		var->varattno = resno;
		new_tlist = lappend(new_tlist, makeTargetEntry(&var->xpr, resno, tle->resname, false));
		resno++;
	}

	return new_tlist;
}

/*
 * Construct the HypertableInsert's target list based on the ModifyTable's
 * target list, which now exists after having been created by
 * set_plan_references().
 */
void
ts_modify_hypertable_fixup_tlist(Plan *plan)
{
	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;

		if (cscan->methods == &modify_hypertable_plan_methods)
		{
			ModifyTable *mt = linitial_node(ModifyTable, cscan->custom_plans);

			if (mt->plan.targetlist == NIL)
			{
				cscan->custom_scan_tlist = NIL;
				cscan->scan.plan.targetlist = NIL;
			}
			else
			{
				/* The input is the output of the child ModifyTable node */
				cscan->custom_scan_tlist = mt->plan.targetlist;

				/* The output is a direct mapping of the input */
				cscan->scan.plan.targetlist = make_var_targetlist(mt->plan.targetlist);
			}
		}
	}
}

List *
ts_replace_rowid_vars(PlannerInfo *root, List *tlist, int varno)
{
	ListCell *lc;
	tlist = list_copy(tlist);
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (IsA(tle->expr, Var) && castNode(Var, tle->expr)->varno == ROWID_VAR)
		{
			tle = copyObject(tle);
			Var *var = castNode(Var, copyObject(tle->expr));
			RowIdentityVarInfo *ridinfo =
				(RowIdentityVarInfo *) list_nth(root->row_identity_vars, var->varattno - 1);
			var = copyObject(ridinfo->rowidvar);
			var->varno = varno;
			var->varnosyn = 0;
			var->varattnosyn = 0;
			tle->expr = (Expr *) var;
			lfirst(lc) = tle;
		}
	}
	return tlist;
}

static Plan *
modify_hypertable_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							  List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	ModifyTable *mt = linitial_node(ModifyTable, custom_plans);

	cscan->methods = &modify_hypertable_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;

	/* The tlist is always NIL since the ModifyTable subplan doesn't have its
	 * targetlist set until set_plan_references (setrefs.c) is run */
	Assert(tlist == NIL);

	/* Target list handling here needs special attention. Intuitively, we'd like
	 * to adopt the target list of the ModifyTable subplan we wrap without
	 * further projection. For a CustomScan this means setting the "input"
	 * custom_scan_tlist to the ModifyTable's target list and having an "output"
	 * targetlist that references the TupleDesc that is created from the
	 * custom_scan_tlist at execution time. Now, while this seems
	 * straight-forward, there are several things with how ModifyTable nodes are
	 * handled in the planner that complicates this:
	 *
	 * - First, ModifyTable doesn't have a targetlist set at this point, and
	 *   it is only set later in set_plan_references (setrefs.c) if there's a
	 *	 RETURNING clause.
	 *
	 * - Second, top-level plan nodes, except for ModifyTable nodes, need to
	 *	 have a targetlist matching root->processed_tlist. This is asserted in
	 *	 apply_tlist_labeling, which is called in create_plan (createplan.c)
	 *	 immediately after this function returns. ModifyTable is exempted
	 *	 because it doesn't always have a targetlist that matches
	 *	 processed_tlist. So, even if we had access to ModifyTable's
	 *	 targetlist here we wouldn't be able to use it since we're a
	 *	 CustomScan and thus not exempted.
	 *
	 * - Third, a CustomScan's targetlist should reference the attributes of the
	 *   TupleDesc that gets created from the custom_scan_tlist at the start of
	 *   execution. This means we need to make the targetlist into all Vars with
	 *   attribute numbers that correspond to the TupleDesc instead of result
	 *   relation in the ModifyTable.
	 *
	 * To get around these issues, we set the targetlist here to
	 * root->processed_tlist, and at the end of planning when the ModifyTable's
	 * targetlist is set, we go back and fix up the CustomScan's targetlist.
	 */
	cscan->scan.plan.targetlist = copyObject(root->processed_tlist);

	/*
	 * For UPDATE/DELETE/MERGE processed_tlist will have ROWID_VAR. We
	 * need to remove those because set_customscan_references will bail
	 * if it sees ROWID_VAR entries in the targetlist.
	 */
	if (mt->operation == CMD_UPDATE || mt->operation == CMD_DELETE || mt->operation == CMD_MERGE)
	{
		cscan->scan.plan.targetlist =
			ts_replace_rowid_vars(root, cscan->scan.plan.targetlist, mt->nominalRelation);

		/*
		 * When the ModifyTable's lefttree contains a ChunkAppend (possibly
		 * wrapped in one or more Result nodes for projection and/or
		 * pseudoconstant gating quals like EXISTS), ChunkAppend will have
		 * already replaced ROWID_VAR entries in its own targetlist to avoid
		 * assertions in set_customscan_references. However, the wrapping
		 * Result nodes' targetlists still contain the original ROWID_VAR
		 * entries. When set_plan_references later calls set_upper_references
		 * on these Result nodes, it tries to resolve the ROWID_VAR entries
		 * against the child's (already replaced) targetlist so we have to
		 * replace ROWID_VAR entries in all Result nodes' targetlists between
		 * ModifyTable and ChunkAppend.
		 */
		if (is_chunk_append_or_projection(mt->plan.lefttree))
		{
			Plan *plan = mt->plan.lefttree;
			while (IsA(plan, Result) && plan->lefttree != NULL)
			{
				plan->targetlist =
					ts_replace_rowid_vars(root, plan->targetlist, mt->nominalRelation);
				plan = plan->lefttree;
			}
		}
	}
	cscan->custom_scan_tlist = cscan->scan.plan.targetlist;

	/*
	 * we save the original list of arbiter indexes here
	 * because we modify that list during execution and
	 * we still need the original list in case that plan
	 * gets reused.
	 */
	cscan->custom_private = list_make1(mt->arbiterIndexes);

	return &cscan->scan.plan;
}

static CustomPathMethods modify_hypertable_path_methods = {
	.CustomName = "ModifyHypertablePath",
	.PlanCustomPath = modify_hypertable_plan_create,
};

Path *
ts_modify_hypertable_path_create(PlannerInfo *root, ModifyTablePath *mtpath, RelOptInfo *rel)
{
	ModifyHypertablePath *mht_path = palloc0(sizeof(ModifyHypertablePath));

	/* Copy costs, etc. */
	memcpy(&mht_path->cpath.path, &mtpath->path, sizeof(Path));
	mht_path->cpath.path.type = T_CustomPath;
	mht_path->cpath.path.pathtype = T_CustomScan;
	mht_path->cpath.custom_paths = list_make1(mtpath);
	mht_path->cpath.methods = &modify_hypertable_path_methods;

	return &mht_path->cpath.path;
}
