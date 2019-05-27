/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <executor/executor.h>
#include <executor/nodeSubplan.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/relation.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/plancat.h>
#include <optimizer/predtest.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "chunk_append/chunk_append.h"
#include "chunk_append/exec.h"
#include "chunk_append/explain.h"
#include "chunk_append/planner.h"
#include "compat.h"

static TupleTableSlot *chunk_append_exec(CustomScanState *node);
static void chunk_append_begin(CustomScanState *node, EState *estate, int eflags);
static void chunk_append_end(CustomScanState *node);
static void chunk_append_rescan(CustomScanState *node);

static CustomExecMethods chunk_append_state_methods = {
	.BeginCustomScan = chunk_append_begin,
	.ExecCustomScan = chunk_append_exec,
	.EndCustomScan = chunk_append_end,
	.ReScanCustomScan = chunk_append_rescan,
	.ExplainCustomScan = chunk_append_explain,
};

static List *constify_restrictinfos(PlannerInfo *root, List *restrictinfos);
static bool can_exclude_chunk(List *constraints, List *restrictinfos);
static void do_startup_exclusion(ChunkAppendState *state);
static Node *constify_param_mutator(Node *node, void *context);
static List *constify_restrictinfo_params(PlannerInfo *root, EState *state, List *restrictinfos);

static void initialize_constraints(ChunkAppendState *state, List *initial_rt_indexes);

Node *
chunk_append_state_create(CustomScan *cscan)
{
	ChunkAppendState *state;

	state = (ChunkAppendState *) newNode(sizeof(ChunkAppendState), T_CustomScanState);

	state->csstate.methods = &chunk_append_state_methods;

	state->initial_subplans = cscan->custom_plans;
	state->initial_ri_clauses = lsecond(cscan->custom_private);
	state->sort_options = lfourth(cscan->custom_private);

	state->startup_exclusion = (bool) linitial_oid(linitial(cscan->custom_private));
	state->runtime_exclusion = (bool) lsecond_oid(linitial(cscan->custom_private));
	state->limit = lthird_oid(linitial(cscan->custom_private));

	state->filtered_subplans = state->initial_subplans;
	state->filtered_ri_clauses = state->initial_ri_clauses;

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

			if (can_exclude_chunk(lfirst(lc_constraints), restrictinfos))
				continue;
		}

		filtered_children = lappend(filtered_children, lfirst(lc_plan));
		filtered_ri_clauses = lappend(filtered_ri_clauses, ri_clauses);
		filtered_constraints = lappend(filtered_constraints, lfirst(lc_constraints));
	}

	state->filtered_subplans = filtered_children;
	state->filtered_ri_clauses = filtered_ri_clauses;
	state->filtered_constraints = filtered_constraints;
}

static void
chunk_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	ChunkAppendState *state = (ChunkAppendState *) node;
	ListCell *lc;
	int i;

	Assert(list_length(cscan->custom_plans) == list_length(state->initial_subplans));
	initialize_constraints(state, lthird(cscan->custom_private));

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

	if (state->runtime_exclusion)
	{
		state->params = state->subplanstates[0]->plan->allParam;
		/*
		 * make sure all params are initialized for runtime exclusion
		 */
		node->ss.ps.chgParam = state->subplanstates[0]->plan->allParam;
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

	state->runtime_number_loops++;
	/*
	 * mark subplans as active/inactive in valid_subplans
	 */
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

			if (!can_exclude_chunk(lfirst(lc_constraints), restrictinfos))
				state->valid_subplans = bms_add_member(state->valid_subplans, i);
			else
				state->runtime_number_exclusions++;
		}

		lc_clauses = lnext(lc_clauses);
		lc_constraints = lnext(lc_constraints);
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

		if (bms_is_empty(state->valid_subplans))
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
			 * If the subplan gave us something check if we need
			 * to do projection otherwise return as is.
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
		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(state->subplanstates[i], node->ss.ps.chgParam);

		ExecReScan(state->subplanstates[i]);
	}
	state->current = 0;

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
	relation = heap_open(relationObjectId, NoLock);

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

#if (PG96 && PG_VERSION_NUM < 90609) || (PG10 && PG_VERSION_NUM < 100004)
			cexpr = (Node *) canonicalize_qual((Expr *) cexpr);
#elif PG96 || PG10
			cexpr = (Node *) canonicalize_qual_ext((Expr *) cexpr, true);
#else
			cexpr = (Node *) canonicalize_qual((Expr *) cexpr, true);
#endif

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

	heap_close(relation, NoLock);

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
#if PG96
	if (predicate_refuted_by(constraints, baserestrictinfo))
#else
	if (predicate_refuted_by(constraints, baserestrictinfo, false))
#endif
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
		Scan *scan = chunk_append_get_scan_plan(lfirst(lc_plan));
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
