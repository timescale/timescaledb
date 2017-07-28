#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/makefuncs.h>
#include <nodes/plannodes.h>
#include <nodes/params.h>
#include <nodes/print.h>
#include <nodes/extensible.h>
#include <parser/parsetree.h>
#include <parser/parse_func.h>
#include <parser/parse_oper.h>
#include <utils/guc.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <optimizer/paths.h>
#include <utils/lsyscache.h>
#include <parser/parse_coerce.h>
#include <parser/parse_collate.h>
#include <miscadmin.h>

#include "hypertable_cache.h"
#include "partitioning.h"
#include "extension.h"
#include "utils.h"
#include "guc.h"
#include "dimension.h"
#include "chunk_dispatch_plan.h"
#include "planner_utils.h"
#include "hypertable_insert.h"

void		_planner_init(void);
void		_planner_fini(void);

static planner_hook_type prev_planner_hook;
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;

typedef struct GlobalPlannerCtx
{
	bool		extension_is_loaded;
	Cache	   *hcache;
	bool		has_hypertables;
} GlobalPlannerCtx;

typedef struct HypertableQueryCtx
{
	Query	   *parse;
	Query	   *parent;
	CmdType		cmdtype;
	Hypertable *hentry;
} HypertableQueryCtx;

typedef struct AddPartFuncQualCtx
{
	Query	   *parse;
	Hypertable *hentry;
} AddPartFuncQualCtx;


static GlobalPlannerCtx gpc = {0};

/*
 * Identify queries on a hypertable by walking the query tree. If the query is
 * indeed on a hypertable, setup the necessary state and/or make modifications
 * to the query tree.
 */
static bool
hypertable_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;
		HypertableQueryCtx *ctx = (HypertableQueryCtx *) context;

		if (rte->rtekind == RTE_RELATION && rte->inh)
		{
			Hypertable *hentry = hypertable_cache_get_entry(gpc.hcache, rte->relid);

			if (hentry != NULL)
				ctx->hentry = hentry;
		}

		return false;
	}

	if (IsA(node, Query))
	{
		bool		result;
		HypertableQueryCtx *ctx = (HypertableQueryCtx *) context;
		CmdType		old = ctx->cmdtype;
		Query	   *query = (Query *) node;
		Query	   *oldparent = ctx->parent;

		/* adjust context */
		ctx->cmdtype = query->commandType;
		ctx->parent = query;

		result = query_tree_walker(ctx->parent, hypertable_query_walker,
								   context, QTW_EXAMINE_RTES);

		/* restore context */
		ctx->cmdtype = old;
		ctx->parent = oldparent;

		return result;
	}

	return expression_tree_walker(node, hypertable_query_walker, context);
}

/* Returns the partitioning info for a var if the var is a partitioning
 * column. If the var is not a partitioning column return NULL */
static PartitioningInfo *
get_partitioning_info_for_partition_column_var(Var *var_expr, Query *parse, Cache *hcache, Hypertable *hentry)
{
	RangeTblEntry *rte = rt_fetch(var_expr->varno, parse->rtable);
	char	   *varname = get_rte_attribute_name(rte, var_expr->varattno);

	if (rte->relid == hentry->main_table_relid)
	{
		Dimension  *closed_dim = hyperspace_get_closed_dimension(hentry->space, 0);

		if (closed_dim != NULL &&
		 strncmp(closed_dim->fd.column_name.data, varname, NAMEDATALEN) == 0)
			return closed_dim->partitioning;
	}
	return NULL;
}

/* Creates an expression for partioning_func(var_expr, partitioning_mod) =
 * partioning_func(const_expr, partitioning_mod).  This function makes a copy of
 * all nodes given in input. */
static Expr *
create_partition_func_equals_const(Var *var_expr, Const *const_expr, char *partitioning_func_schema,
								   char *partitioning_func)
{
	Expr	   *op_expr;
	List	   *func_name = list_make2(makeString(partitioning_func_schema), makeString(partitioning_func));
	Var		   *var_for_fn_call;
	Node	   *var_node_for_fn_call;
	Const	   *const_for_fn_call;
	Node	   *const_node_for_fn_call;
	List	   *args_func_var;
	List	   *args_func_const;
	FuncCall   *fc_var;
	FuncCall   *fc_const;
	Node	   *f_var;
	Node	   *f_const;

	const_for_fn_call = (Const *) palloc(sizeof(Const));
	memcpy(const_for_fn_call, const_expr, sizeof(Const));

	var_for_fn_call = (Var *) palloc(sizeof(Var));
	memcpy(var_for_fn_call, var_expr, sizeof(Var));

	if (var_for_fn_call->vartype == TEXTOID)
	{
		var_node_for_fn_call = (Node *) var_for_fn_call;
		const_node_for_fn_call = (Node *) const_for_fn_call;
	}
	else
	{
		var_node_for_fn_call =
			coerce_to_target_type(NULL, (Node *) var_for_fn_call,
								  var_for_fn_call->vartype,
								  TEXTOID, -1, COERCION_EXPLICIT,
								  COERCE_EXPLICIT_CAST, -1);
		const_node_for_fn_call =
			coerce_to_target_type(NULL, (Node *) const_for_fn_call,
								  const_for_fn_call->consttype,
								  TEXTOID, -1, COERCION_EXPLICIT,
								  COERCE_EXPLICIT_CAST, -1);
	}

	args_func_var = list_make1(var_node_for_fn_call);
	args_func_const = list_make1(const_node_for_fn_call);

	fc_var = makeFuncCall(func_name, args_func_var, -1);
	fc_const = makeFuncCall(func_name, args_func_const, -1);

	f_var = ParseFuncOrColumn(NULL, func_name, args_func_var, fc_var, -1);
	assign_expr_collations(NULL, f_var);

	f_const = ParseFuncOrColumn(NULL, func_name, args_func_const, fc_const, -1);

	op_expr = make_op(NULL, list_make2(makeString("pg_catalog"), makeString("=")), f_var, f_const, -1);

	return op_expr;
}

static Node *
add_partitioning_func_qual_mutator(Node *node, AddPartFuncQualCtx *context)
{
	if (node == NULL)
		return NULL;

	/*
	 * Detect partitioning_column = const. If not fall-thru. If detected,
	 * replace with partitioning_column = const AND
	 * partitioning_func(partition_column) = partitioning_func(const)
	 */
	if (IsA(node, OpExpr))
	{
		OpExpr	   *exp = (OpExpr *) node;

		if (list_length(exp->args) == 2)
		{
			/* only look at var op const or const op var; */
			Node	   *left = (Node *) linitial(exp->args);
			Node	   *right = (Node *) lsecond(exp->args);
			Var		   *var_expr = NULL;
			Node	   *other_expr = NULL;

			if (IsA(left, Var))
			{
				var_expr = (Var *) left;
				other_expr = right;
			}
			else if (IsA(right, Var))
			{
				var_expr = (Var *) right;
				other_expr = left;
			}

			if (var_expr != NULL)
			{
				if (!IsA(other_expr, Const))
				{
					/* try to simplify the non-var expression */
					other_expr = eval_const_expressions(NULL, other_expr);
				}
				if (IsA(other_expr, Const))
				{
					/* have a var and const, make sure the op is = */
					Const	   *const_expr = (Const *) other_expr;
					Oid			eq_oid = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString("=")), exprType(left), exprType(right));

					if (eq_oid == exp->opno)
					{
						/*
						 * I now have a var = const. Make sure var is a
						 * partitioning column
						 */
						PartitioningInfo *pi =
						get_partitioning_info_for_partition_column_var(var_expr,
															  context->parse,
																  gpc.hcache,
															context->hentry);

						if (pi != NULL)
						{
							/* The var is a partitioning column */
							Expr	   *partitioning_clause = create_partition_func_equals_const(var_expr, const_expr,
									 pi->partfunc.schema, pi->partfunc.name);

							return (Node *) make_andclause(list_make2(node, partitioning_clause));

						}
					}
				}
			}
		}
	}

	return expression_tree_mutator(node, add_partitioning_func_qual_mutator,
								   (void *) context);
}


/*
 * This function does a transformation that allows postgres's native constraint
 * exclusion to exclude space partititions when the query contains equivalence
 * qualifiers on the space partition key.
 *
 * This function goes through the upper-level qual of a parse tree and finds
 * quals of the form:
 *				partitioning_column = const
 * It transforms them into the qual:
 *				partitioning_column = const AND
 *				partitioning_func(partition_column, partitioning_mod) =
 *				partitioning_func(const, partitioning_mod)
 *
 * This tranformation helps because the check constraint on a table is of the
 * form CHECK(partitioning_func(partition_column, partitioning_mod) BETWEEN X
 * AND Y).
 */
static void
add_partitioning_func_qual(Query *parse, Hypertable *hentry)
{
	AddPartFuncQualCtx context = {
		.parse = parse,
		.hentry = hentry,
	};

	parse->jointree->quals = add_partitioning_func_qual_mutator(parse->jointree->quals, &context);
}

typedef struct ModifyTableWalkerCtx
{
	Query	   *parse;
	List	   *rtable;
} ModifyTableWalkerCtx;

/*
 * Traverse the plan tree to find ModifyTable nodes that indicate an INSERT
 * operation. We'd like to modify these plans to redirect tuples to chunks
 * instead of the parent table.
 *
 * From the ModifyTable description: "Each ModifyTable node contains
 * a list of one or more subplans, much like an Append node.  There
 * is one subplan per result relation."
 *
 * The subplans produce the tuples for INSERT, while the result relation is the
 * table we'd like to insert into.
 *
 * The way we redirect tuples to chunks is to insert an intermediate "chunk
 * dispatch" plan node, inbetween the ModifyTable and its subplan that produces
 * the tuples. When the ModifyTable plan is executed, it tries to read a tuple
 * from the intermediate chunk dispatch plan instead of the original
 * subplan. The chunk plan reads the tuple from the original subplan, looks up
 * the chunk, sets the executor's resultRelation to the chunk table and finally
 * returns the tuple to the ModifyTable node.
 *
 * We also need to wrap the ModifyTable plan node with a HypertableInsert node
 * to give the ChunkDispatchState node access to the ModifyTableState node in
 * the execution phase.
 *
 * Conceptually, the plan modification looks like this:
 *
 * Original plan:
 *
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 *
 *
 * Modified plan:
 *
 *	[ HypertableInsert ]
 *		  ^
 *		  |
 *	[ ModifyTable ] -> resultRelation
 *		  ^			   ^
 *		  | Tuple	  / <Set resultRelation to the matching chunk table>
 *		  |			 /
 * [ ChunkDispatch ]
 *		  ^
 *		  | Tuple
 *		  |
 *	  [ subplan ]
 */
static void
modifytable_plan_walker(Plan **planptr, void *pctx)
{
	ModifyTableWalkerCtx *ctx = (ModifyTableWalkerCtx *) pctx;
	Plan	   *plan = *planptr;

	if (IsA(plan, ModifyTable))
	{
		ModifyTable *mt = (ModifyTable *) plan;

		if (mt->operation == CMD_INSERT)
		{
			bool        hypertable_found = false;
			ListCell   *lc_plan,
					   *lc_rel;

			if (ctx->parse->onConflict != NULL &&
				ctx->parse->onConflict->constraint != InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Hypertables do not support ON CONFLICT statements that reference constraints"),
					 errhint("Use column names to infer indexes instead.")));

			/*
			 * To match up tuple-producing subplans with result relations, we
			 * simultaneously loop over subplans and resultRelations, although
			 * for INSERTs we expect only one of each.
			 */
			forboth(lc_plan, mt->plans, lc_rel, mt->resultRelations)
			{
				RangeTblEntry *rte = rt_fetch(lfirst_int(lc_rel), ctx->rtable);
				Hypertable *ht = hypertable_cache_get_entry(gpc.hcache, rte->relid);

				if (ht != NULL)
				{
					void	  **subplan_ptr = &lfirst(lc_plan);
					Plan	   *subplan = *subplan_ptr;

					/*
					 * We replace the plan with our custom chunk dispatch
					 * plan.
					 */
					*subplan_ptr = chunk_dispatch_plan_create(subplan, rte->relid, ctx->parse);
					hypertable_found = true;
				}
			}

			if (hypertable_found)
				*planptr = hypertable_insert_plan_create(mt);
		}
	}
}

static void
global_planner_context_init()
{
	memset(&gpc, 0, sizeof(GlobalPlannerCtx));
	gpc.extension_is_loaded = extension_is_loaded();

	if (gpc.extension_is_loaded)
		gpc.hcache = hypertable_cache_pin();
}

static void
global_planner_context_cleanup()
{
	if (gpc.hcache)
		cache_release(gpc.hcache);
}

static PlannedStmt *
timescaledb_planner(Query *parse, int cursor_opts, ParamListInfo bound_params)
{
	PlannedStmt *plan_stmt = NULL;

	global_planner_context_init();

	if (gpc.extension_is_loaded)
	{
		HypertableQueryCtx context;

		/* replace call to main table with call to the replica table */
		context.parse = parse;
		context.parent = parse;
		context.cmdtype = parse->commandType;
		context.hentry = NULL;
		hypertable_query_walker((Node *) parse, &context);

		/* note assumes 1 hypertable per query */
		if (context.hentry != NULL)
		{
			add_partitioning_func_qual(parse, context.hentry);
			gpc.has_hypertables = true;
		}
	}

	if (prev_planner_hook != NULL)
	{
		/* Call any earlier hooks */
		plan_stmt = (prev_planner_hook) (parse, cursor_opts, bound_params);
	}
	else
	{
		/* Call the standard planner */
		plan_stmt = standard_planner(parse, cursor_opts, bound_params);
	}

	if (gpc.extension_is_loaded)
	{
		ModifyTableWalkerCtx ctx = {
			.parse = parse,
			.rtable = plan_stmt->rtable,
		};

		planned_stmt_walker(plan_stmt, modifytable_plan_walker, &ctx);
	}
	global_planner_context_cleanup();

	return plan_stmt;
}


static inline bool
should_optimize_query()
{
	return !guc_disable_optimizations &&
		(guc_optimize_non_hypertables || (gpc.extension_is_loaded && gpc.has_hypertables));
}


extern void sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

static void
timescaledb_set_rel_pathlist(PlannerInfo *root,
							 RelOptInfo *rel,
							 Index rti,
							 RangeTblEntry *rte)
{
	if (extension_is_loaded() && !IS_DUMMY_REL(rel) && should_optimize_query())
	{
		if (OidIsValid(rte->relid))
		{
			sort_transform_optimization(root, rel);
		}
	}

	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook) (root, rel, rti, rte);
}

void
_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = timescaledb_planner;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = timescaledb_set_rel_pathlist;

}

void
_planner_fini(void)
{
	planner_hook = prev_planner_hook;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
}
