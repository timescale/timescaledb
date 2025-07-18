/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk_tuple_routing.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/modify_hypertable.h"

#if PG18_GE
#include <commands/explain_format.h>
#endif

static AttrNumber
rel_get_natts(Oid relid)
{
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	AttrNumber natts = ((Form_pg_class) GETSTRUCT(tp))->relnatts;
	ReleaseSysCache(tp);
	return natts;
}

static bool
attr_is_dropped_or_missing(Oid relid, AttrNumber attno)
{
	HeapTuple tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attno));
	if (!HeapTupleIsValid(tp))
		return false;
	Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	bool result = att_tup->attisdropped || att_tup->atthasmissing;
	ReleaseSysCache(tp);
	return result;
}

/*
 * ModifyHypertable is a plan node that implements DML for hypertables.
 * It is a wrapper around the ModifyTable plan node that calls the wrapped ModifyTable
 * plan.
 *
 * The wrapping is needed to setup state in the execution phase, and give access
 * to the ModifyTableState node to sub-plan states in the PlanState tree. For
 * instance, the ChunkDispatchState node needs to set the arbiter index list in
 * the ModifyTableState node whenever it inserts into a new chunk.
 */
static void
modify_hypertable_begin(CustomScanState *node, EState *estate, int eflags)
{
	ModifyHypertableState *state = (ModifyHypertableState *) node;
	ModifyTableState *mtstate;
	PlanState *ps;

	ModifyTable *mt = castNode(ModifyTable, &state->mt->plan);
	/*
	 * To make statement trigger defined on the hypertable work
	 * we need to set the hypertable as the rootRelation otherwise
	 * statement trigger defined only on the hypertable will not fire.
	 */
	if (mt->operation == CMD_DELETE || mt->operation == CMD_UPDATE || mt->operation == CMD_MERGE)
		mt->rootRelation = mt->nominalRelation;
	ps = ExecInitNode(&mt->plan, estate, eflags);
	node->custom_ps = list_make1(ps);
	mtstate = castNode(ModifyTableState, ps);
	state->mt_state = mtstate;

	/*
	 * If this is not the primary ModifyTable node, postgres added it to the
	 * beginning of es_auxmodifytables, to be executed by ExecPostprocessPlan.
	 * Unfortunately that strips off the HypertableInsert node leading to
	 * tuple routing not working in INSERTs inside CTEs. To make INSERTs
	 * inside CTEs work we have to fix es_auxmodifytables and add back the
	 * ModifyHypertableState.
	 */
	if (estate->es_auxmodifytables && linitial(estate->es_auxmodifytables) == mtstate)
		linitial(estate->es_auxmodifytables) = node;

	if (mtstate->operation == CMD_INSERT || mtstate->operation == CMD_MERGE)
	{
		/* setup chunk dispatch state only for INSERTs */
		state->ctr = ts_chunk_tuple_routing_create(estate, mtstate->resultRelInfo);
		state->ctr->counters->onConflictAction = mt->onConflictAction;
		state->ctr->mht_state = state;
	}
	if (mtstate->operation == CMD_MERGE)
	{
		const AttrNumber natts = rel_get_natts(state->ctr->hypertable->main_table_relid);
		for (AttrNumber attno = 1; attno <= natts; attno++)
		{
			if (attr_is_dropped_or_missing(state->ctr->hypertable->main_table_relid, attno))
			{
				state->ctr->has_dropped_attrs = true;
				break;
			}
		}
	}
}

static TupleTableSlot *
modify_hypertable_exec(CustomScanState *node)
{
	ModifyTableState *mtstate = linitial_node(ModifyTableState, node->custom_ps);
	return ExecModifyTable(node, &mtstate->ps);
}

static void
modify_hypertable_end(CustomScanState *node)
{
	ModifyHypertableState *state = (ModifyHypertableState *) node;
	ExecEndNode(linitial(node->custom_ps));
	if (state->ctr)
		ts_chunk_tuple_routing_destroy(state->ctr);
}

static void
modify_hypertable_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
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
	 */
	if (((ModifyTable *) mtstate->ps.plan)->operation == CMD_DELETE && es->verbose &&
		ts_is_chunk_append_plan(mtstate->ps.plan->lefttree))
	{
		mtstate->ps.plan->lefttree->targetlist = NULL;
		((CustomScan *) mtstate->ps.plan->lefttree)->custom_scan_tlist = NULL;
	}
	if (((ModifyTable *) mtstate->ps.plan)->operation == CMD_MERGE && es->verbose)
	{
		mtstate->ps.plan->lefttree->targetlist = NULL;
		((CustomScan *) mtstate->ps.plan->lefttree)->custom_scan_tlist = NULL;
	}
	/*
	 * Since we hijack the ModifyTable node, instrumentation on ModifyTable will
	 * be missing so we set it to instrumentation of ModifyHypertable node.
	 */
	if (mtstate->ps.instrument)
	{
		/*
		 * INSERT .. ON CONFLICT statements record few metrics in the ModifyTable node.
		 * So, copy them into ModifyHypertable node before replacing them.
		 */
		node->ss.ps.instrument->ntuples2 = mtstate->ps.instrument->ntuples2;
		node->ss.ps.instrument->nfiltered1 = mtstate->ps.instrument->nfiltered1;
	}
	mtstate->ps.instrument = node->ss.ps.instrument;

	/*
	 * For INSERT we have to read the number of decompressed batches and
	 * tuples from the ChunkDispatchState below the ModifyTable.
	 */
	if ((mtstate->operation == CMD_INSERT || mtstate->operation == CMD_MERGE) &&
		outerPlanState(mtstate))
	{
		ChunkTupleRouting *ctr = state->ctr;

		state->batches_deleted += ctr->counters->batches_deleted;
		state->batches_filtered += ctr->counters->batches_filtered;
		state->batches_decompressed += ctr->counters->batches_decompressed;
		state->tuples_decompressed += ctr->counters->tuples_decompressed;
	}
	if (state->batches_filtered > 0)
		ExplainPropertyInteger("Batches filtered", NULL, state->batches_filtered, es);
	if (state->batches_decompressed > 0)
		ExplainPropertyInteger("Batches decompressed", NULL, state->batches_decompressed, es);
	if (state->tuples_decompressed > 0)
		ExplainPropertyInteger("Tuples decompressed", NULL, state->tuples_decompressed, es);
	if (state->batches_deleted > 0)
		ExplainPropertyInteger("Batches deleted", NULL, state->batches_deleted, es);
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

		if (mt->operation == CMD_UPDATE && ts_is_chunk_append_plan(mt->plan.lefttree))
		{
			mt->plan.lefttree->targetlist =
				ts_replace_rowid_vars(root, mt->plan.lefttree->targetlist, mt->nominalRelation);
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
ts_modify_hypertable_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Hypertable *ht,
								 RelOptInfo *rel)
{
	Path *path = &mtpath->path;
	ModifyHypertablePath *hmpath;

	hmpath = palloc0(sizeof(ModifyHypertablePath));

	/* Copy costs, etc. */
	memcpy(&hmpath->cpath.path, path, sizeof(Path));
	hmpath->cpath.path.type = T_CustomPath;
	hmpath->cpath.path.pathtype = T_CustomScan;
	hmpath->cpath.custom_paths = list_make1(mtpath);
	hmpath->cpath.methods = &modify_hypertable_path_methods;
	path = &hmpath->cpath.path;

	return path;
}
