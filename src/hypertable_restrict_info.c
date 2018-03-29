#include <postgres.h>
#include <nodes/relation.h>
#include <utils/typcache.h>
#include <optimizer/clauses.h>
#include <utils/lsyscache.h>
#include <parser/parsetree.h>

#include "hypertable_restrict_info.h"
#include "dimension.h"
#include "utils.h"
#include "dimension_slice.h"
#include "chunk.h"
#include "dimension_vector.h"
#include "partitioning.h"

typedef struct DimensionRestrictInfo
{
	Dimension  *dimension;
} DimensionRestrictInfo;

typedef struct DimensionRestrictInfoOpen
{
	DimensionRestrictInfo base;
	int64		lower_bound;	/* internal time representation */
	StrategyNumber lower_strategy;
	int64		upper_bound;	/* internal time representation */
	StrategyNumber upper_strategy;
} DimensionRestrictInfoOpen;

typedef struct DimensionRestrictInfoClosed
{
	DimensionRestrictInfo base;
	int32		value;			/* hash value */
	StrategyNumber strategy;	/* either Invalid or equal */
} DimensionRestrictInfoClosed;

static DimensionRestrictInfoOpen *
dimension_restrict_info_open_create(Dimension *d)
{
	DimensionRestrictInfoOpen *new = palloc(sizeof(DimensionRestrictInfoOpen));

	new->base.dimension = d;
	new->lower_strategy = InvalidStrategy;
	new->upper_strategy = InvalidStrategy;
	return new;
}

static DimensionRestrictInfoClosed *
dimension_restrict_info_closed_create(Dimension *d)
{
	DimensionRestrictInfoClosed *new = palloc(sizeof(DimensionRestrictInfoClosed));

	new->base.dimension = d;
	new->strategy = InvalidStrategy;
	return new;
}

static DimensionRestrictInfo *
dimension_restrict_info_create(Dimension *d)
{
	switch (d->type)
	{
		case DIMENSION_TYPE_OPEN:
			return &dimension_restrict_info_open_create(d)->base;
		case DIMENSION_TYPE_CLOSED:
			return &dimension_restrict_info_closed_create(d)->base;
		default:
			elog(ERROR, "unknown dimension type");
	}
}

static bool
dimension_restrict_info_open_add(DimensionRestrictInfoOpen *dri, StrategyNumber strategy, Const *c)
{
	int64		value = time_value_to_internal(c->constvalue, c->consttype, false);

	switch (strategy)
	{
		case BTLessEqualStrategyNumber:
		case BTLessStrategyNumber:
			if (dri->upper_strategy == InvalidStrategy || value < dri->upper_bound)
			{
				dri->upper_strategy = strategy;
				dri->upper_bound = value;
			}
			return true;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			if (dri->lower_strategy == InvalidStrategy || value > dri->lower_bound)
			{
				dri->lower_strategy = strategy;
				dri->lower_bound = value;
			}
			return true;
		case BTEqualStrategyNumber:
			dri->lower_bound = value;
			dri->upper_bound = value;
			dri->lower_strategy = BTGreaterEqualStrategyNumber;
			dri->upper_strategy = BTLessEqualStrategyNumber;
			return true;
		default:
			return false;
	}
}

static bool
dimension_restrict_info_closed_add(DimensionRestrictInfoClosed *dri, StrategyNumber strategy, Const *c)
{
	int64		value = partitioning_func_apply(dri->base.dimension->partitioning, c->constvalue);

	switch (strategy)
	{
		case BTEqualStrategyNumber:
			dri->value = value;
			dri->strategy = strategy;
			return true;
		default:
			return false;
	}
}


static bool
dimension_restrict_info_add(DimensionRestrictInfo *dri, int strategy, Const *c)
{
	switch (dri->dimension->type)
	{
		case DIMENSION_TYPE_OPEN:
			return dimension_restrict_info_open_add((DimensionRestrictInfoOpen *) dri, strategy, c);
		case DIMENSION_TYPE_CLOSED:
			return dimension_restrict_info_closed_add((DimensionRestrictInfoClosed *) dri, strategy, c);
		default:
			elog(ERROR, "unknown dimension type");
	}
}

static DimensionVec *
dimension_restrict_info_open_slices(DimensionRestrictInfoOpen *dri)
{
	/* basic idea: slice_end > lower_bound && slice_start < upper_bound */
	return dimension_slice_scan_range_limit(dri->base.dimension->fd.id, dri->upper_strategy, dri->upper_bound, dri->lower_strategy, dri->lower_bound, 0);
}

static DimensionVec *
dimension_restrict_info_closed_slices(DimensionRestrictInfoClosed *dri)
{
	if (dri->strategy == BTEqualStrategyNumber)
		/* slice_end >= value && slice_start <= value */
		return dimension_slice_scan_range_limit(dri->base.dimension->fd.id,
												BTLessEqualStrategyNumber,
												dri->value,
												BTGreaterEqualStrategyNumber,
												dri->value,
												0);
	/* get all slices */
	return dimension_slice_scan_range_limit(dri->base.dimension->fd.id,
											InvalidStrategy,
											-1,
											InvalidStrategy,
											-1,
											0);
}

static DimensionVec *
dimension_restrict_info_slices(DimensionRestrictInfo *dri)
{
	switch (dri->dimension->type)
	{
		case DIMENSION_TYPE_OPEN:
			return dimension_restrict_info_open_slices((DimensionRestrictInfoOpen *) dri);
		case DIMENSION_TYPE_CLOSED:
			return dimension_restrict_info_closed_slices((DimensionRestrictInfoClosed *) dri);
		default:
			elog(ERROR, "unknown dimension type");
	}
}

typedef struct HypertableRestrictInfo
{
	int			num_base_restrictions;	/* number of base restrictions
										 * successfully added */
	int			num_dimensions;
	DimensionRestrictInfo *dimension_restriction[FLEXIBLE_ARRAY_MEMBER];	/* array of dimension
																			 * restrictions */
} HypertableRestrictInfo;


HypertableRestrictInfo *
hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht)
{
	int			num_dimensions = ht->space->num_dimensions;
	HypertableRestrictInfo *res = palloc0(sizeof(HypertableRestrictInfo) + sizeof(DimensionRestrictInfo *) * num_dimensions);
	int			i;

	res->num_dimensions = num_dimensions;

	for (i = 0; i < num_dimensions; i++)
	{
		DimensionRestrictInfo *dri = dimension_restrict_info_create(&ht->space->dimensions[i]);

		res->dimension_restriction[i] = dri;
	}

	return res;
}

static DimensionRestrictInfo *
hypertable_restrict_info_get(HypertableRestrictInfo *hri, AttrNumber attno)
{
	int			i;

	for (i = 0; i < hri->num_dimensions; i++)
	{
		if (hri->dimension_restriction[i]->dimension->column_attno == attno)
			return hri->dimension_restriction[i];
	}
	return NULL;
}

static bool
hypertable_restrict_info_add_op_expr(HypertableRestrictInfo *hri, PlannerInfo *root, OpExpr *clause)
{
	Expr	   *leftop,
			   *rightop,
			   *expr;
	DimensionRestrictInfo *dri;
	Var		   *v;
	Const	   *c;
	Oid			op_oid;
	RangeTblEntry *rte;
	Oid			columntype;
	TypeCacheEntry *tce;
	int			strategy;
	Oid			lefttype,
				righttype;

	if (list_length(clause->args) != 2)
		return false;

	/* Same as constraint_exclusion */
	if (contain_mutable_functions((Node *) clause))
		return false;

	leftop = (Expr *) get_leftop((Expr *) clause);
	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	rightop = (Expr *) get_rightop((Expr *) clause);
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	if (IsA(leftop, Var))
	{
		v = (Var *) leftop;
		expr = rightop;
		op_oid = clause->opno;
	}
	else if (IsA(rightop, Var))
	{
		v = (Var *) rightop;
		expr = leftop;
		op_oid = get_commutator(clause->opno);
	}
	else
		return false;

	dri = hypertable_restrict_info_get(hri, v->varattno);
	/* the attribute is not a dimension */
	if (dri == NULL)
		return false;

	expr = (Expr *) eval_const_expressions(root, (Node *) expr);

	if (!IsA(expr, Const) ||!OidIsValid(op_oid) || !op_strict(op_oid))
		return false;

	c = (Const *) expr;

	rte = rt_fetch(v->varno, root->parse->rtable);

	columntype = get_atttype(rte->relid, dri->dimension->column_attno);
	tce = lookup_type_cache(columntype, TYPECACHE_BTREE_OPFAMILY);

	if (!op_in_opfamily(op_oid, tce->btree_opf))
		return false;

	get_op_opfamily_properties(op_oid,
							   tce->btree_opf,
							   false,
							   &strategy,
							   &lefttype,
							   &righttype);

	return dimension_restrict_info_add(dri, strategy, c);
}

static void
hypertable_restrict_info_add_restrict_info(HypertableRestrictInfo *hri, PlannerInfo *root, RestrictInfo *ri)
{
	Expr	   *e = ri->clause;

	if (!IsA(e, OpExpr))
		return;

	if (hypertable_restrict_info_add_op_expr(hri, root, (OpExpr *) e))
		hri->num_base_restrictions++;
}

void
hypertable_restrict_info_add(HypertableRestrictInfo *hri,
							 PlannerInfo *root,
							 List *base_restrict_infos)
{
	ListCell   *lc;

	foreach(lc, base_restrict_infos)
	{
		RestrictInfo *ri = lfirst(lc);

		hypertable_restrict_info_add_restrict_info(hri, root, ri);
	}
}

bool
hypertable_restrict_info_has_restrictions(HypertableRestrictInfo *hri)
{
	return hri->num_base_restrictions > 0;
}

List *
hypertable_restrict_info_get_chunk_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode)
{
	int			i;
	List	   *dimension_vecs = NIL;

	for (i = 0; i < hri->num_dimensions; i++)
	{
		DimensionRestrictInfo *dri = hri->dimension_restriction[i];
		DimensionVec *dv;

		Assert(NULL != dri);

		dv = dimension_restrict_info_slices(dri);

		Assert(dv->num_slices >= 0);

		/*
		 * If there are no matching slices in any single dimension, the result
		 * will be empty
		 */
		if (dv->num_slices == 0)
			return NIL;

		dimension_vecs = lappend(dimension_vecs, dv);

	}

	Assert(list_length(dimension_vecs) == ht->space->num_dimensions);
	return chunk_find_all_oids(ht->space, dimension_vecs, lockmode);
}
