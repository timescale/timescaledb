/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <funcapi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/planner.h>
#include <parser/parsetree.h>
#include <utils/fmgroids.h>

#include "plan.h"

#include "exec.h"
#include "expression_utils.h"
#include "import/list.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/columnar_scan/vector_quals.h"
#include "nodes/modify_hypertable.h"
#include "nodes/vector_agg.h"
#include "utils.h"

static struct CustomScanMethods scan_methods = { .CustomName = VECTOR_AGG_NODE_NAME,
												 .CreateCustomScanState = vector_agg_state_create };

void
_vector_agg_init(void)
{
	TryRegisterCustomScanMethods(&scan_methods);
}

bool
ts_is_vector_agg_plan(Plan *plan)
{
	return IsA(plan, CustomScan) && castNode(CustomScan, plan)->methods == &scan_methods;
}

/*
 * Create a vectorized aggregation node to replace the given partial aggregation
 * node.
 */
static Plan *
vector_agg_plan_create(Plan *childplan, Agg *agg, List *resolved_targetlist,
					   VectorAggGroupingType grouping_type)
{
	CustomScan *vector_agg = (CustomScan *) makeNode(CustomScan);
	vector_agg->custom_plans = list_make1(childplan);
	vector_agg->methods = &scan_methods;

	vector_agg->custom_scan_tlist = resolved_targetlist;

	/*
	 * Note that this is being called from the post-planning hook, and therefore
	 * after set_plan_refs(). The meaning of output targetlists is different from
	 * the previous planning stages, and they contain special varnos referencing
	 * the scan targetlists.
	 */
	vector_agg->scan.plan.targetlist =
		ts_build_trivial_custom_output_targetlist(vector_agg->custom_scan_tlist);

	/*
	 * Copy the costs from the normal aggregation node, so that they show up in
	 * the EXPLAIN output. They are not used for any other purposes, because
	 * this hook is called after the planning is finished.
	 */
	vector_agg->scan.plan.plan_rows = agg->plan.plan_rows;
	vector_agg->scan.plan.plan_width = agg->plan.plan_width;
	vector_agg->scan.plan.startup_cost = agg->plan.startup_cost;
	vector_agg->scan.plan.total_cost = agg->plan.total_cost;

	vector_agg->scan.plan.parallel_aware = false;
	vector_agg->scan.plan.parallel_safe = childplan->parallel_safe;
	vector_agg->scan.plan.async_capable = false;

	vector_agg->scan.plan.plan_node_id = agg->plan.plan_node_id;

	Assert(agg->plan.qual == NIL);

	vector_agg->scan.plan.initPlan = agg->plan.initPlan;

	vector_agg->scan.plan.extParam = bms_copy(agg->plan.extParam);
	vector_agg->scan.plan.allParam = bms_copy(agg->plan.allParam);

	vector_agg->custom_private = ts_new_list(T_List, VASI_Count);
	lfirst(list_nth_cell(vector_agg->custom_private, VASI_GroupingType)) =
		makeInteger(grouping_type);

	return (Plan *) vector_agg;
}

/*
 * Whether we have an in-memory columnar representation for a given type.
 */
static bool
is_vector_type(Oid typeoid)
{
	switch (typeoid)
	{
		case BOOLOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TEXTOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
		case UUIDOID:
		case INTERVALOID:
			return true;
		default:
			return false;
	}
}

static bool is_vector_expr(const VectorQualInfo *vqinfo, Expr *expr);

/*
 * Whether we can evaluate this function as part of the columnar pipeline.
 */
static bool
is_vector_function(const VectorQualInfo *vqinfo, List *args, Oid funcoid, Oid resulttype,
				   Oid inputcollid)
{
	if (!is_vector_type(resulttype))
	{
		return false;
	}

	ListCell *lc;
	foreach (lc, args)
	{
		if (!is_vector_expr(vqinfo, (Expr *) lfirst(lc)))
		{
			return false;
		}
	}

	if (!func_strict(funcoid))
	{
		return false;
	}

	if (func_volatile(funcoid) == PROVOLATILE_VOLATILE)
	{
		return false;
	}

	return true;
}

/*
 * Whether the expression can be used for vectorized processing: must be a Var
 * that refers to either a bulk-decompressed or a segmentby column.
 */
static bool
is_vector_expr(const VectorQualInfo *vqinfo, Expr *expr)
{
	/*
	 * Skip NULLs for uniform handling of the optional nodes.
	 */
	if (expr == NULL)
	{
		return true;
	}

	switch (((Node *) expr)->type)
	{
		case T_Const:
		{
			Const *c = (Const *) expr;
			return is_vector_type(c->consttype);
		}

		case T_FuncExpr:
		{
			/* Can vectorize some functions! */
			FuncExpr *f = castNode(FuncExpr, expr);
			return is_vector_function(vqinfo,
									  f->args,
									  f->funcid,
									  f->funcresulttype,
									  f->inputcollid);
		}

		case T_OpExpr:
		{
			OpExpr *o = castNode(OpExpr, expr);
			return is_vector_function(vqinfo,
									  o->args,
									  o->opfuncid,
									  o->opresulttype,
									  o->inputcollid);
		}

		case T_Var:
		{
			Var *var = castNode(Var, expr);

			if (var->varattno <= 0)
			{
				/* Can't work with special attributes like tableoid. */
				return false;
			}

			Assert(var->varattno <= vqinfo->maxattno);

			const bool is_vector = vqinfo->vector_attrs && vqinfo->vector_attrs[var->varattno];

			/*
			 * The segmentby columns are considered vectorizable, but their type might not actually
			 * have a columnar representation. Theoretically this can work because they are always
			 * represented as DT_Scalar, but in practice this is poorly tested and of limited
			 * utility, so we consider such columns not to be vectorizable at the moment.
			 */
			return is_vector && is_vector_type(var->vartype);
		}

		case T_CaseExpr:
		{
			CaseExpr *c = castNode(CaseExpr, expr);
			if (c->arg != NULL)
			{
				/*
				 * We don't handle the "CASE testexpr WHEN comexpr ..." form at
				 * the moment.
				 */
			}

			ListCell *lc;
			foreach (lc, c->args)
			{
				Node *when = lfirst(lc);
				if (!is_vector_expr(vqinfo, (Expr *) when))
				{
					return false;
				}
			}

			if (!is_vector_expr(vqinfo, c->defresult))
			{
				return false;
			}

			return true;
		}

		case T_CaseWhen:
		{
			CaseWhen *when = castNode(CaseWhen, expr);

			if (!is_vector_expr(vqinfo, when->result))
			{
				return false;
			}

			Node *condition_vectorized = vector_qual_make((Node *) when->expr, vqinfo);
			if (condition_vectorized == NULL)
			{
				return false;
			}

			when->expr = (Expr *) condition_vectorized;
			return true;
		}

		default:
			return false;
	}
}

/*
 * Whether we can vectorize this particular aggregate.
 */
static bool
can_vectorize_aggref(const VectorQualInfo *vqi, Aggref *aggref)
{
	if (aggref->aggdirectargs != NIL)
	{
		/* Can't process ordered-set aggregates with direct arguments. */
		return false;
	}

	if (aggref->aggorder != NIL)
	{
		/* Can't process aggregates with an ORDER BY clause. */
		return false;
	}

	if (aggref->aggdistinct != NIL)
	{
		/* Can't process aggregates with DISTINCT clause. */
		return false;
	}

	if (aggref->aggfilter != NULL)
	{
		/* Can process aggregates with filter clause if it's vectorizable. */
		Node *aggfilter_vectorized = vector_qual_make((Node *) aggref->aggfilter, vqi);
		if (aggfilter_vectorized == NULL)
		{
			return false;
		}
		aggref->aggfilter = (Expr *) aggfilter_vectorized;
	}

	if (get_vector_aggregate(aggref->aggfnoid) == NULL)
	{
		/*
		 * We don't have a vectorized implementation for this particular
		 * aggregate function.
		 */
		return false;
	}

	if (aggref->args == NIL)
	{
		/* This must be count(*), we can vectorize it. */
		return true;
	}

	/* The function must have one argument, check it. */
	Assert(list_length(aggref->args) == 1);
	TargetEntry *argument = castNode(TargetEntry, linitial(aggref->args));

	return is_vector_expr(vqi, argument->expr);
}

/*
 * What vectorized grouping strategy we can use for the given grouping columns.
 */
static VectorAggGroupingType
get_vectorized_grouping_type(const VectorQualInfo *vqinfo, Agg *agg, List *resolved_targetlist)
{
	/*
	 * The Agg->numCols value can be less than the number of the non-aggregated
	 * vars in the aggregated targetlist, if some of them are equated to a
	 * constant. This behavior started with PG 16. This case is not very
	 * important, so we treat all non-aggregated columns as grouping columns to
	 * keep the vectorized aggregation node simple.
	 */
	int num_grouping_columns = 0;
	bool all_segmentby = true;

	Oid single_grouping_var_type = InvalidOid;
	int16 typlen = 0;
	bool typbyval = false;

	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			continue;
		}

		num_grouping_columns++;

		if (!is_vector_expr(vqinfo, target_entry->expr))
		{
			return VAGT_Invalid;
		}

		/*
		 * Detect whether we're only grouping by segmentby columns, in which
		 * case we can use the whole-batch grouping strategy. Probably this
		 * could be extended to allow arbitrary expressions referencing only the
		 * segmentby columns.
		 */
		if (IsA(target_entry->expr, Var))
		{
			Var *var = castNode(Var, target_entry->expr);
			all_segmentby &= vqinfo->segmentby_attrs[var->varattno];
		}
		else
		{
			all_segmentby = false;
		}

		/*
		 * If we have a single grouping column, record it for the additional
		 * checks later.
		 */
		if (num_grouping_columns != 1)
		{
			continue;
		}

		TupleDesc tdesc = NULL;
		TypeFuncClass type_class =
			get_expr_result_type((Node *) target_entry->expr, &single_grouping_var_type, &tdesc);
		if (type_class != TYPEFUNC_SCALAR)
		{
			continue;
		}

		get_typlenbyval(single_grouping_var_type, &typlen, &typbyval);
		Ensure(typlen != 0, "invalid zero typlen for type %d", single_grouping_var_type);
	}

	Assert(num_grouping_columns >= agg->numCols);

	/*
	 * We support vectorized aggregation without grouping.
	 */
	if (num_grouping_columns == 0)
	{
		return VAGT_Batch;
	}

	/*
	 * We support grouping by any number of columns if all of them are segmentby.
	 */
	if (all_segmentby)
	{
		return VAGT_Batch;
	}

	/*
	 * We support hashed vectorized grouping by one fixed-size by-value
	 * compressed column.
	 * We can use our hash table for GroupAggregate as well, because it preserves
	 * the input order of the keys, but only for the direct order, not reverse.
	 */
	if (num_grouping_columns == 1 && typlen != 0)
	{
		if (typbyval)
		{
			switch (typlen)
			{
				case 1:
#ifdef TS_USE_UMASH
					Assert(single_grouping_var_type == BOOLOID);
					return VAGT_HashSerialized;
#else
					return VAGT_Invalid;
#endif
				case 2:
					return VAGT_HashSingleFixed2;
				case 4:
					return VAGT_HashSingleFixed4;
				case 8:
					return VAGT_HashSingleFixed8;
				default:
					Ensure(false, "invalid fixed size %d of a vector type", typlen);
					break;
			}
		}
#ifdef TS_USE_UMASH
		/*
		 * We also have the UUID type which is by-reference and has a
		 * columnar in-memory representation, but no specialized single-column
		 * vectorized grouping support. It can use the serialized grouping
		 * strategy.
		 */
		else if (single_grouping_var_type == TEXTOID)
		{
			return VAGT_HashSingleText;
		}
#endif
	}

#ifdef TS_USE_UMASH
	/*
	 * Use hashing of serialized keys when we have many grouping columns.
	 */
	return VAGT_HashSerialized;
#else
	return VAGT_Invalid;
#endif
}

typedef struct HasVectorAggContext
{
	bool has_agg;
	bool has_vector_agg;
} HasVectorAggContext;

static Plan *
has_vector_agg(Plan *plan, void *context)
{
	HasVectorAggContext *ctx = (HasVectorAggContext *) context;
	if (IsA(plan, Agg))
	{
		ctx->has_agg = true;
	}
	else if (ts_is_vector_agg_plan(plan))
	{
		ctx->has_vector_agg = true;
	}
	return plan;
}

/*
 * Whether we have a vectorized aggregation node and any aggregate node at all
 * in the plan tree. This is used for testing.
 */
bool
has_vector_agg_node(Plan *plan, bool *has_some_agg)
{
	HasVectorAggContext context = { .has_agg = false, .has_vector_agg = false };
	ts_plan_tree_walker(plan, has_vector_agg, &context);
	*has_some_agg = context.has_agg;
	return context.has_vector_agg;
}

/*
 * Check if a VectorAgg is possible on top of the given child plan.
 *
 * If the child plan is compatible, also initialize the VectorQualInfo struct
 * for aggregation FILTER clauses.
 *
 * Returns true if the scan node is a supported child, otherwise false.
 */
static bool
vectoragg_plan_possible(Plan *childplan, VectorQualInfo *vqi)
{
	if (!IsA(childplan, CustomScan))
		return false;

	if (childplan->qual != NIL)
	{
		/* Can't do vectorized aggregation if we have Postgres quals. */
		return false;
	}

	if (ts_is_columnar_scan_plan(childplan))
	{
		vectoragg_plan_columnar_scan(childplan, vqi);
		return true;
	}

	return false;
}

static Node *
mark_partial_aggref_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		mark_partial_aggref(castNode(Aggref, node), AGGSPLIT_INITIAL_SERIAL);
		return node;
	}

	return expression_tree_mutator(node, mark_partial_aggref_mutator, context);
}

typedef struct MakeFinalizeAggContext
{
	Agg *agg;
	List *vector_agg_targetlist;
} MakeFinalizeAggContext;

static Node *
make_finalize_agg_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, TargetEntry))
	{
		TargetEntry *tle = castNode(TargetEntry, node);
		MakeFinalizeAggContext *ctx = (MakeFinalizeAggContext *) context;

		if (IsA(tle->expr, Var))
		{
			Var *var = castNode(Var, tle->expr);
			Assert(var->varno == OUTER_VAR);
			AttrNumber old_attno = var->varattno;
			var->varattno = tle->resno;
			for (int k = 0; k < ctx->agg->numCols; k++)
			{
				if (ctx->agg->grpColIdx[k] == old_attno)
					ctx->agg->grpColIdx[k] = tle->resno;
			}
			return node;
		}

		if (IsA(tle->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, tle->expr);

			/*
			 * Look up the VectorAgg output type for this column, which is
			 * the transition type set by mark_partial_aggref above.
			 */
			TargetEntry *vag_tle = list_nth(ctx->vector_agg_targetlist, tle->resno - 1);
			Oid var_type = exprType((Node *) vag_tle->expr);

			mark_partial_aggref(aggref, AGGSPLIT_FINAL_DESERIAL);

			Var *var = makeVar(OUTER_VAR, tle->resno, var_type, -1, aggref->aggcollid, 0);
			aggref->args = list_make1(makeTargetEntry((Expr *) var, 1, NULL, false));
			return node;
		}
	}

	return expression_tree_mutator(node, make_finalize_agg_mutator, context);
}

static Plan *insert_vector_agg(Plan *plan, void *context);

Plan *
try_insert_vector_agg_node(Plan *plan)
{
	return ts_plan_tree_walker(plan, insert_vector_agg, NULL);
}

static Plan *
insert_vector_agg(Plan *plan, void *context)
{
	if (!IsA(plan, Agg))
	{
		return plan;
	}

	Agg *agg = castNode(Agg, plan);

	if (agg->aggsplit != AGGSPLIT_INITIAL_SERIAL && agg->aggsplit != AGGSPLIT_SIMPLE)
	{
		/* Can only vectorize partial or non-partial aggregation node. */
		return plan;
	}

	if (agg->groupingSets != NIL)
	{
		/* No GROUPING SETS support. */
		return plan;
	}

	if (agg->plan.qual != NIL)
	{
		/*
		 * No HAVING support. Probably we can't have it in this node in any case,
		 * because we only replace the partial aggregation nodes which can't
		 * check the HAVING clause.
		 */
		return plan;
	}
	if (agg->plan.lefttree == NULL)
	{
		/*
		 * Not sure what this would mean, but check for it just to be on the
		 * safe side because we can effectively see any possible plan here.
		 */
		return plan;
	}

	Plan *childplan = agg->plan.lefttree;
	VectorQualInfo vqi;
	MemSet(&vqi, 0, sizeof(VectorQualInfo));

	/*
	 * Build supplementary info to determine whether we can vectorize the
	 * aggregate FILTER clauses.
	 */
	if (!vectoragg_plan_possible(childplan, &vqi))
	{
		/* Not a compatible vectoragg child node */
		return plan;
	}

	/*
	 * To make it easier to examine the variables participating in the aggregation,
	 * the subsequent checks are performed on the aggregated targetlist with
	 * all variables resolved to uncompressed chunk variables.
	 */
	List *resolved_targetlist = ts_resolve_outer_special_vars(agg->plan.targetlist, childplan);

	const VectorAggGroupingType grouping_type =
		get_vectorized_grouping_type(&vqi, agg, resolved_targetlist);
	if (grouping_type == VAGT_Invalid)
	{
		/* The grouping is not vectorizable. */
		return plan;
	}

	/*
	 * The hash grouping strategies do not preserve the input key order when the
	 * reverse ordering is requested, so in this case they cannot work in
	 * GroupAggregate mode.
	 */
	if (grouping_type != VAGT_Batch && agg->aggstrategy != AGG_HASHED)
	{
		if (vqi.reverse)
		{
			return plan;
		}
	}

	/* Now check the output targetlist. */
	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, target_entry->expr);
			if (!can_vectorize_aggref(&vqi, aggref))
			{
				/* Aggregate function not vectorizable. */
				return plan;
			}
		}
	}

	/*
	 * Finally, all requirements are satisfied and we can vectorize this
	 * aggregation node.
	 */
	Plan *vector_agg_plan =
		vector_agg_plan_create(childplan, agg, resolved_targetlist, grouping_type);

	if (agg->aggsplit == AGGSPLIT_SIMPLE)
	{
		/*
		 * Convert a non-partial aggregation into a two-phase partial + finalize
		 * aggregation with VectorAgg performing the partial step.
		 */
		CustomScan *vector_agg = castNode(CustomScan, vector_agg_plan);
		vector_agg->custom_scan_tlist =
			(List *) expression_tree_mutator((Node *) vector_agg->custom_scan_tlist,
											 mark_partial_aggref_mutator,
											 NULL);

		/*
		 * Rebuild the plan output targetlist to reflect the updated types.
		 * VectorAgg returns ps_ResultTupleSlot whose TupleDesc is derived from
		 * plan.targetlist, so it must match the actual partial aggregate output
		 * types for correct tuple materialization on all platforms.
		 */
		vector_agg->scan.plan.targetlist =
			ts_build_trivial_custom_output_targetlist(vector_agg->custom_scan_tlist);

		/*
		 * Set up the parent Agg to finalize the partial results from VectorAgg.
		 */
		agg->aggsplit = AGGSPLIT_FINAL_DESERIAL;
		agg->plan.lefttree = vector_agg_plan;

		MakeFinalizeAggContext finalize_ctx = {
			.agg = agg,
			.vector_agg_targetlist = vector_agg->scan.plan.targetlist,
		};
		agg->plan.targetlist = (List *) expression_tree_mutator((Node *) agg->plan.targetlist,
																make_finalize_agg_mutator,
																&finalize_ctx);
		agg->plan.qual = (List *) expression_tree_mutator((Node *) agg->plan.qual,
														  make_finalize_agg_mutator,
														  &finalize_ctx);

		return (Plan *) agg;
	}

	return vector_agg_plan;
}
