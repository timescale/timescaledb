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
#include <optimizer/optimizer.h>
#include <optimizer/planner.h>
#include <optimizer/tlist.h>
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
vector_agg_plan_create(Plan *aggregation_input, Agg *agg, List *resolved_targetlist,
					   List *resolved_postgres_quals, VectorAggGroupingType grouping_type)
{
	CustomScan *vector_agg = (CustomScan *) makeNode(CustomScan);
	vector_agg->custom_plans = list_make1(aggregation_input);
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
	vector_agg->scan.plan.parallel_safe = aggregation_input->parallel_safe;
	vector_agg->scan.plan.async_capable = false;

	vector_agg->scan.plan.plan_node_id = agg->plan.plan_node_id;

	Assert(agg->plan.qual == NIL);

	vector_agg->scan.plan.initPlan = agg->plan.initPlan;

	vector_agg->scan.plan.extParam = bms_copy(agg->plan.extParam);
	vector_agg->scan.plan.allParam = bms_copy(agg->plan.allParam);

	vector_agg->custom_private = ts_new_list(T_List, VASI_Count);
	lfirst(list_nth_cell(vector_agg->custom_private, VASI_GroupingType)) =
		makeInteger(grouping_type);
	lfirst(list_nth_cell(vector_agg->custom_private, VASI_PostgresQuals)) = resolved_postgres_quals;

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
				return false;
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
can_vectorize_aggref(const VectorQualInfo *vqinfo, Aggref *aggref)
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
		Node *aggfilter_vectorized = vector_qual_make((Node *) aggref->aggfilter, vqinfo);
		if (aggfilter_vectorized == NULL)
		{
			return false;
		}
		aggref->aggfilter = (Expr *) aggfilter_vectorized;
	}

	if (get_vector_aggregate(aggref->aggfnoid, aggref->inputcollid) == NULL)
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

	return is_vector_expr(vqinfo, argument->expr);
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
 * Check if a VectorAgg is possible on top of the given aggregation input.
 *
 * If the aggregation input is compatible, also initialize the VectorQualInfo struct
 * for aggregation FILTER clauses.
 *
 * Returns true if the given plan is a supported aggregation input, otherwise false.
 */
static bool
vectoragg_plan_possible(Plan *aggregation_input, VectorQualInfo *vqinfo)
{
	if (!ts_is_columnar_scan_plan(aggregation_input))
	{
		return false;
	}

	vectoragg_plan_columnar_scan(aggregation_input, vqinfo);
	return true;
}

/*
 * Build a finalize Agg targetlist expression from a grouping targetlist
 * expression: change Var and Aggref references to point to the targetlist items
 * of partial aggregate. Note that we're working after set_plan_references here.
 */
static Node *
finalize_agg_reference_mutator(Node *node, void *context)
{
	List *partial_agg_targetlist = (List *) context;

	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Var))
	{
		Var *grouping_tlist_var = castNode(Var, node);
		Assert(grouping_tlist_var->varno == OUTER_VAR);
		TargetEntry *partial_agg_tle =
			tlist_member((Expr *) grouping_tlist_var, partial_agg_targetlist);
		Assert(partial_agg_tle != NULL);
		Var *partial_column_var = castNode(Var, copyObject(grouping_tlist_var));
		partial_column_var->varattno = partial_agg_tle->resno;
		return (Node *) partial_column_var;
	}

	if (IsA(node, Aggref))
	{
		Aggref *aggref = castNode(Aggref, node);

		TargetEntry *partial_agg_tle = tlist_member((Expr *) aggref, partial_agg_targetlist);
		Assert(partial_agg_tle != NULL);

		Var *partial_state_var = makeVar(OUTER_VAR,
										 partial_agg_tle->resno,
										 exprType((Node *) partial_agg_tle->expr),
										 -1,
										 aggref->aggcollid,
										 0);

		Aggref *combining_aggref = makeNode(Aggref);
		memcpy(combining_aggref, aggref, sizeof(Aggref));
		/* The partial stage applies the FILTER. */
		combining_aggref->aggfilter = NULL;
		combining_aggref->args =
			list_make1(makeTargetEntry((Expr *) partial_state_var, 1, NULL, false));
		return (Node *) combining_aggref;
	}

	return expression_tree_mutator(node, finalize_agg_reference_mutator, context);
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

	Plan *aggregation_input = agg->plan.lefttree;
	VectorQualInfo vqinfo;
	MemSet(&vqinfo, 0, sizeof(VectorQualInfo));

	/*
	 * Build supplementary info to determine whether we can vectorize the
	 * aggregate FILTER clauses.
	 */
	if (!vectoragg_plan_possible(aggregation_input, &vqinfo))
	{
		/* Not a compatible VectorAgg aggregation input */
		return plan;
	}

	/*
	 * Can't do vectorized aggregation if we have Postgres quals that we
	 * cannot evaluate in the columnar pipeline.
	 */
	List *resolved_postgres_quals =
		(List *) ts_resolve_outer_special_vars((Node *) aggregation_input->qual, aggregation_input);
	ListCell *lc;
	foreach (lc, resolved_postgres_quals)
	{
		if (!is_vector_expr(&vqinfo, (Expr *) lfirst(lc)))
		{
			return plan;
		}
	}

	/*
	 * VectorAgg can only replaces a partial aggregation node. Normally these
	 * are created by chunkwise aggregation, but it is not applied when we have
	 * only one chunk. To handle this case, we split a single-chunk Agg node
	 * here into a final and partial aggregate nodes, if we find out that we can
	 * replace the partial aggregate with VectorAgg. First, we have to prepare
	 * the targetlists for both the final and partial aggregate nodes, to be
	 * able to perform the remaining checks.
	 *
	 * The grouping targetlist is output after projection: an item can be an
	 * expression mixing aggregates and columns, and a grouping key does not
	 * have to appear there at all. This makes it complicated to build the
	 * partial agg targetlist by following the grouping targetlist layout.
	 * Instead, we pull all aggregates and variables from grouping targetlist,
	 * In addition, we consult the list of grouping columns in the aggregation
	 * node, because some grouping columns might not be present in the final
	 * output, but they still must be produced by the partial aggregation node.
	 *
	 * The targetlist of the finalize aggregation node follows the structure of
	 * the grouping targetlist, but the aggregates and variables there have to
	 * be replaced to reference the partial aggregation output.
	 */
	List *partial_agg_targetlist = NIL;
	List *finalize_agg_targetlist = NIL;
	AttrNumber *finalize_agg_grpcolidx = NULL;
	if (agg->aggsplit == AGGSPLIT_SIMPLE)
	{
		finalize_agg_grpcolidx = palloc(sizeof(AttrNumber) * agg->numCols);
		for (int k = 0; k < agg->numCols; k++)
		{
			const AttrNumber resno = k + 1;
			TargetEntry *aggregation_input_tle =
				list_nth_node(TargetEntry,
							  aggregation_input->targetlist,
							  AttrNumberGetAttrOffset(agg->grpColIdx[k]));
			Var *grouping_var = makeVar(OUTER_VAR,
										agg->grpColIdx[k],
										exprType((Node *) aggregation_input_tle->expr),
										exprTypmod((Node *) aggregation_input_tle->expr),
										exprCollation((Node *) aggregation_input_tle->expr),
										0);
			partial_agg_targetlist =
				lappend(partial_agg_targetlist,
						makeTargetEntry((Expr *) grouping_var, resno, NULL, false));
			finalize_agg_grpcolidx[k] = resno;
		}

		List *vars_and_aggrefs =
			pull_var_clause((Node *) agg->plan.targetlist, PVC_INCLUDE_AGGREGATES);
		partial_agg_targetlist = add_to_flat_tlist(partial_agg_targetlist, vars_and_aggrefs);

		finalize_agg_targetlist = (List *) expression_tree_mutator((Node *) agg->plan.targetlist,
																   finalize_agg_reference_mutator,
																   partial_agg_targetlist);

		/*
		 * The aggregates were matched across the two targetlists by
		 * equality, in the original single-stage form. Now mark them for
		 * the two-stage aggregation.
		 */
		foreach (lc, partial_agg_targetlist)
		{
			TargetEntry *partial_agg_tle = lfirst_node(TargetEntry, lc);
			if (IsA(partial_agg_tle->expr, Aggref))
			{
				mark_partial_aggref(castNode(Aggref, partial_agg_tle->expr),
									AGGSPLIT_INITIAL_SERIAL);
			}
		}

		List *finalize_agg_aggrefs =
			pull_var_clause((Node *) finalize_agg_targetlist, PVC_INCLUDE_AGGREGATES);
		foreach (lc, finalize_agg_aggrefs)
		{
			if (!IsA(lfirst(lc), Aggref))
			{
				continue;
			}
			Aggref *combining_aggref = castNode(Aggref, lfirst(lc));
			mark_partial_aggref(combining_aggref, AGGSPLIT_FINAL_DESERIAL);

			/*
			 * The partial state variable gets the result type of the
			 * partial aggregate, known only after the marking above.
			 */
			TargetEntry *argument_tle = linitial_node(TargetEntry, combining_aggref->args);
			Var *partial_state_var = castNode(Var, argument_tle->expr);
			TargetEntry *partial_agg_tle =
				list_nth_node(TargetEntry,
							  partial_agg_targetlist,
							  AttrNumberGetAttrOffset(partial_state_var->varattno));
			partial_state_var->vartype = exprType((Node *) partial_agg_tle->expr);
		}
	}
	else
	{
		partial_agg_targetlist = list_copy(agg->plan.targetlist);
	}

	/*
	 * To make it easier to examine the variables participating in the aggregation,
	 * the subsequent checks are performed on the aggregated targetlist with
	 * all variables resolved to uncompressed chunk variables.
	 */
	List *resolved_targetlist =
		castNode(List,
				 ts_resolve_outer_special_vars((Node *) partial_agg_targetlist, aggregation_input));

	const VectorAggGroupingType grouping_type =
		get_vectorized_grouping_type(&vqinfo, agg, resolved_targetlist);
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
		if (vqinfo.reverse)
		{
			return plan;
		}
	}

	/* Now check the output targetlist. */
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, target_entry->expr);
			if (!can_vectorize_aggref(&vqinfo, aggref))
			{
				/* Aggregate function not vectorizable. */
				return plan;
			}
		}
	}

	/*
	 * Finally, all requirements are satisfied and we can vectorize this
	 * aggregation node.
	 *
	 * The Postgres quals stay on aggregation_input->qual for EXPLAIN
	 * display. VectorAgg evaluates their resolved copies itself,
	 * reading compressed batches from the ColumnarScan directly
	 * instead of running it as a plan node.
	 */
	Plan *vector_agg_plan = vector_agg_plan_create(aggregation_input,
												   agg,
												   resolved_targetlist,
												   resolved_postgres_quals,
												   grouping_type);

	if (agg->aggsplit == AGGSPLIT_SIMPLE)
	{
		/*
		 * We have to additionally split the single chunk aggregation node into
		 * partial and final aggregation, as prepared above.
		 */
		agg->aggsplit = AGGSPLIT_FINAL_DESERIAL;
		agg->plan.lefttree = vector_agg_plan;
		agg->plan.targetlist = finalize_agg_targetlist;
		agg->grpColIdx = finalize_agg_grpcolidx;

		return (Plan *) agg;
	}

	return vector_agg_plan;
}
