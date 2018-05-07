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
#include <optimizer/paths.h>

#include "cache.h"
#include "hypertable.h"
#include "partitioning.h"
#include "compat.h"

#include "hypertable_restrict_info.h"
#include "chunk.h"
#include "hypercube.h"
#include "hypertable.h"

static inline Dimension *
get_dimension_for_partition_column_var(Var *var_expr, Hypertable *ht, Query *parse)
{
	RangeTblEntry *rte = rt_fetch(var_expr->varno, parse->rtable);
	char	   *varname;

	if (rte->relid != ht->main_table_relid)
		return NULL;

	varname = get_rte_attribute_name(rte, var_expr->varattno);

	return hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_CLOSED, varname);
}

static Const *
get_const_node(Node *node)
{
	if (IsA(node, Const))
		return (Const *) node;
	/* try to simplify the non-var expression */
	node = eval_const_expressions(NULL, node);

	if (IsA(node, Const))
		return (Const *) node;

	return NULL;
}

/* Return true if expr is var = const or const = var. If true, set
 * var_node and const_node to the var and const. */
static bool
is_var_equal_const_opexpr(OpExpr *opexpr, Var **var_node, Const **const_node)
{
	Node	   *left;
	Node	   *right;
	Oid			eq_oid;

	if (list_length(opexpr->args) != 2)
		return false;


	left = linitial(opexpr->args);
	right = lsecond(opexpr->args);


	if (IsA(left, Var))
	{
		*var_node = (Var *) left;
		*const_node = get_const_node(right);
	}
	else if (IsA(right, Var))
	{
		*var_node = (Var *) right;
		*const_node = get_const_node(left);
	}
	else
	{
		return false;
	}

	if (*const_node == NULL)
		return false;


	eq_oid = OpernameGetOprid(list_make2(makeString("pg_catalog"), makeString("=")), exprType(left), exprType(right));

	if (eq_oid != opexpr->opno)
		return false;

	return true;
}

static HypertableRestrictInfo *
hypertable_restrict_info_create(Dimension *dim, int32 hash_value)
{
	HypertableRestrictInfo *hre = palloc(sizeof(HypertableRestrictInfo));

	*hre = (HypertableRestrictInfo)
	{
		.dim = dim,
			.value = hash_value,
	};
	return hre;
}


HypertableRestrictInfo *
hypertable_restrict_info_get(RestrictInfo *base_restrict_info, Query *query, Hypertable *hentry)
{
	Var		   *var_expr;
	Const	   *const_expr;
	Expr	   *node = base_restrict_info->clause;
	OpExpr	   *opexpr;

	if (!IsA(node, OpExpr))
		return NULL;

	opexpr = (OpExpr *) node;

	if (is_var_equal_const_opexpr(opexpr, &var_expr, &const_expr))
	{
		/*
		 * I now have a var = const. Make sure var is a partitioning column
		 */
		Dimension  *dim = get_dimension_for_partition_column_var(var_expr, hentry, query);

		if (dim != NULL)
			return hypertable_restrict_info_create(dim,
												   partitioning_func_apply(dim->partitioning, const_expr->constvalue));
	}
	return NULL;
}


void
hypertable_restrict_info_apply(HypertableRestrictInfo *spe,
							   PlannerInfo *root,
							   Hypertable *ht,
							   Index main_table_child_index)
{
	ListCell   *lsib;

	/*
	 * Go through all the append rel list and find all children that are
	 * chunks. Then check the chunk for exclusion and make dummy if necessary
	 */
	foreach(lsib, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(lsib);
		RelOptInfo *siblingrel;
		RangeTblEntry *siblingrte;
		Chunk	   *chunk;
		DimensionSlice *ds;

		/* find all chunks: children that are not the main table */
		if (appinfo->parent_reloid != ht->main_table_relid || appinfo->child_relid == main_table_child_index)
			continue;

		siblingrel = root->simple_rel_array[appinfo->child_relid];
		siblingrte = root->simple_rte_array[appinfo->child_relid];
		chunk = chunk_get_by_relid(siblingrte->relid, ht->space->num_dimensions, true);
		ds = hypercube_get_slice_by_dimension_id(chunk->cube, spe->dim->fd.id);

		/* If the hash value is not inside the ds, exclude the chunk */
		if (dimension_slice_cmp_coordinate(ds, (int64) spe->value) != 0)
			set_dummy_rel_pathlist(siblingrel);
	}
}
