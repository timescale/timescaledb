#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>
#include <parser/parse_coerce.h>
#include <parser/parse_func.h>
#include <parser/parse_collate.h>
#include <parser/parse_oper.h>
#include <optimizer/clauses.h>
#include <catalog/pg_type.h>

#include "cache.h"
#include "hypertable.h"
#include "partitioning.h"
#include "parse_rewrite.h"
#include "compat.h"

typedef struct AddPartFuncQualCtx
{
	ParseState *pstate;
	Query	   *parse;
	Hypertable *hentry;
} AddPartFuncQualCtx;


/*
 * Returns the partitioning info for a var if the var is a partitioning
 * column. If the var is not a partitioning column return NULL.
 */
static inline PartitioningInfo *
get_partitioning_info_for_partition_column_var(Var *var_expr, AddPartFuncQualCtx *context)
{
	Hypertable *ht = context->hentry;
	RangeTblEntry *rte = rt_fetch(var_expr->varno, context->parse->rtable);
	char	   *varname;
	Dimension  *dim;

	if (rte->relid != ht->main_table_relid)
		return NULL;

	varname = get_rte_attribute_name(rte, var_expr->varattno);

	dim = hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_CLOSED, varname);

	if (dim != NULL)
		return dim->partitioning;

	return NULL;
}

/*
 * Creates an expression for partioning_func(var_expr, partitioning_mod) =
 * partitioning_func(const_expr, partitioning_mod).  This function makes a copy
 * of all nodes given in input.
 */
static Expr *
create_partition_func_equals_const(ParseState *pstate, PartitioningInfo *pi, Var *var_expr, Const *const_expr)
{
	Expr	   *op_expr;
	List	   *func_name = partitioning_func_qualified_name(&pi->partfunc);
	Node	   *var_node;
	Node	   *const_node;
	List	   *args_func_var;
	List	   *args_func_const;
	FuncCall   *fc_var;
	FuncCall   *fc_const;
	Node	   *f_var;
	Node	   *f_const;

	var_node = (Node *) copyObject(var_expr);
	const_node = (Node *) copyObject(const_expr);

	args_func_var = list_make1(var_node);
	args_func_const = list_make1(const_node);

	fc_var = makeFuncCall(func_name, args_func_var, -1);
	fc_const = makeFuncCall(func_name, args_func_const, -1);

	f_var = ParseFuncOrColumnCompat(pstate,
									func_name,
									args_func_var,
									fc_var,
									-1);

	assign_expr_collations(pstate, f_var);

	f_const = ParseFuncOrColumnCompat(pstate,
									  func_name,
									  args_func_const,
									  fc_const,
									  -1);

	op_expr = make_op_compat(pstate,
							 list_make2(makeString("pg_catalog"), makeString("=")),
							 f_var,
							 f_const,
							 -1);

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
																	   context);

						if (pi != NULL)
						{
							/* The var is a partitioning column */
							Expr	   *partitioning_clause =
							create_partition_func_equals_const(context->pstate, pi, var_expr, const_expr);

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
void
parse_rewrite_query(ParseState *pstate, Query *parse, Hypertable *ht)
{
	AddPartFuncQualCtx context = {
		.pstate = pstate,
		.parse = parse,
		.hentry = ht,
	};

	parse->jointree->quals = add_partitioning_func_qual_mutator(parse->jointree->quals, &context);
}
