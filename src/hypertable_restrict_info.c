/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/relation.h>
#include <utils/typcache.h>
#include <optimizer/clauses.h>
#include <utils/lsyscache.h>
#include <parser/parsetree.h>
#include <utils/array.h>

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
	List	   *partitions;		/* hash values */
	StrategyNumber strategy;	/* either Invalid or equal */
} DimensionRestrictInfoClosed;

typedef struct DimensionValues
{
	List	   *values;
	bool		use_or;			/* ORed or ANDed values */
	Oid type;					/* Oid type for values */
} DimensionValues;

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

	new->partitions = NIL;
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
			return NULL;
	}
}

static bool
dimension_restrict_info_open_add(DimensionRestrictInfoOpen *dri, StrategyNumber strategy, DimensionValues *dimvalues)
{
	ListCell   *item;
	bool		restriction_added = false;

	/* can't handle IN/ANY with multiple values */
	if (dimvalues->use_or && list_length(dimvalues->values) > 1)
		return false;

	foreach(item, dimvalues->values)
	{
		Oid			restype;
		Datum		datum = ts_dimension_transform_value(dri->base.dimension, PointerGetDatum(lfirst(item)), &restype);
		int64		value = ts_time_value_to_internal(datum, restype, false);

		switch (strategy)
		{
			case BTLessEqualStrategyNumber:
			case BTLessStrategyNumber:
				if (dri->upper_strategy == InvalidStrategy || value < dri->upper_bound)
				{
					dri->upper_strategy = strategy;
					dri->upper_bound = value;
					restriction_added = true;
				}
				break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
				if (dri->lower_strategy == InvalidStrategy || value > dri->lower_bound)
				{
					dri->lower_strategy = strategy;
					dri->lower_bound = value;
					restriction_added = true;
				}
				break;
			case BTEqualStrategyNumber:
				dri->lower_bound = value;
				dri->upper_bound = value;
				dri->lower_strategy = BTGreaterEqualStrategyNumber;
				dri->upper_strategy = BTLessEqualStrategyNumber;
				restriction_added = true;
				break;
			default:
				/* unsupported strategy */
				break;

		}
	}
	return restriction_added;
}

static List *
dimension_restrict_info_get_partitions(DimensionRestrictInfoClosed *dri, List *values)
{
	List	   *partitions = NIL;
	ListCell   *item;

	foreach(item, values)
	{
		Datum		value = ts_dimension_transform_value(dri->base.dimension, PointerGetDatum(lfirst(item)), NULL);

		partitions = list_append_unique_int(partitions, DatumGetInt32(value));
	}

	return partitions;
}

static bool
dimension_restrict_info_closed_add(DimensionRestrictInfoClosed *dri, StrategyNumber strategy, DimensionValues *dimvalues)
{
	List	   *partitions;
	bool		restriction_added = false;

	if (strategy != BTEqualStrategyNumber)
	{
		return false;
	}

	partitions = dimension_restrict_info_get_partitions(dri, dimvalues->values);

	/* the intersection is empty when using ALL operator (ANDing values)  */
	if (list_length(partitions) > 1 && !dimvalues->use_or)
	{
		dri->strategy = strategy;
		dri->partitions = NIL;
		return true;
	}

	if (dri->strategy == InvalidStrategy)
		/* first time through */
	{
		dri->partitions = partitions;
		dri->strategy = strategy;
		restriction_added = true;
	}
	else
	{
		/* intersection with NULL is NULL */
		if (dri->partitions == NIL)
			return true;

		/*
		 * We are always ANDing the expressions thus intersection is used.
		 */
		dri->partitions = list_intersection_int(dri->partitions, partitions);

		/* no intersection is also a restriction  */
		restriction_added = true;
	}
	return restriction_added;
}

static bool
dimension_restrict_info_add(DimensionRestrictInfo *dri, int strategy, DimensionValues *values)
{
	switch (dri->dimension->type)
	{
		case DIMENSION_TYPE_OPEN:
			return dimension_restrict_info_open_add((DimensionRestrictInfoOpen *) dri, strategy, values);
		case DIMENSION_TYPE_CLOSED:
			return dimension_restrict_info_closed_add((DimensionRestrictInfoClosed *) dri, strategy, values);
		default:
			elog(ERROR, "unknown dimension type: %d", dri->dimension->type);
			/* supress compiler warning on MSVC */
			return false;
	}
}

static DimensionVec *
dimension_restrict_info_open_slices(DimensionRestrictInfoOpen *dri)
{
	/* basic idea: slice_end > lower_bound && slice_start < upper_bound */
	return ts_dimension_slice_scan_range_limit(dri->base.dimension->fd.id, dri->upper_strategy, dri->upper_bound, dri->lower_strategy, dri->lower_bound, 0);
}

static DimensionVec *
dimension_restrict_info_closed_slices(DimensionRestrictInfoClosed *dri)
{
	if (dri->strategy == BTEqualStrategyNumber)
	{
		/* slice_end >= value && slice_start <= value */
		ListCell   *cell;
		DimensionVec *dim_vec = ts_dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);

		foreach(cell, dri->partitions)
		{
			int			i;
			int32		partition = lfirst_int(cell);
			DimensionVec *tmp = ts_dimension_slice_scan_range_limit(dri->base.dimension->fd.id,
																	BTLessEqualStrategyNumber,
																	partition,
																	BTGreaterEqualStrategyNumber,
																	partition,
																	0);

			for (i = 0; i < tmp->num_slices; i++)
				dim_vec = ts_dimension_vec_add_unique_slice(&dim_vec, tmp->slices[i]);
		}
		return dim_vec;
	}

	/* get all slices */
	return ts_dimension_slice_scan_range_limit(dri->base.dimension->fd.id,
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
			return NULL;
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
ts_hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht)
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

typedef DimensionValues *(*get_dimension_values) (Const *c, bool use_or);

static bool
hypertable_restrict_info_add_expr(HypertableRestrictInfo *hri, PlannerInfo *root, List *expr_args, Oid op_oid, get_dimension_values func_get_dim_values, bool use_or)
{
	Expr	   *leftop,
			   *rightop,
			   *expr;
	DimensionRestrictInfo *dri;
	Var		   *v;
	Const	   *c;
	RangeTblEntry *rte;
	Oid			columntype;
	TypeCacheEntry *tce;
	int			strategy;
	Oid			lefttype,
				righttype;
	DimensionValues *dimvalues;

	if (list_length(expr_args) != 2)
		return false;

	leftop = linitial(expr_args);
	rightop = lsecond(expr_args);

	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	if (IsA(leftop, Var))
	{
		v = (Var *) leftop;
		expr = rightop;
	}
	else if (IsA(rightop, Var))
	{
		v = (Var *) rightop;
		expr = leftop;
		op_oid = get_commutator(op_oid);
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

	dimvalues = func_get_dim_values(c, use_or);
	return dimension_restrict_info_add(dri, strategy, dimvalues);
}

static DimensionValues *
dimension_values_create(List *values, Oid type, bool use_or)
{
	DimensionValues *dimvalues;

	dimvalues = palloc(sizeof(DimensionValues));
	dimvalues->values = values;
	dimvalues->use_or = use_or;
	dimvalues->type = type;

	return dimvalues;
}

static DimensionValues *
dimension_values_create_from_array(Const *c, bool user_or)
{
	ArrayIterator iterator = array_create_iterator(DatumGetArrayTypeP(c->constvalue), 0, NULL);
	Datum		elem = (Datum) NULL;
	bool		isnull;
	List	   *values = NIL;
	Oid			base_el_type;

	while (array_iterate(iterator, &elem, &isnull))
	{
		if (!isnull)
			values = lappend(values, DatumGetPointer(elem));
	}

	/* it's an array type, lets get the base element type */
	base_el_type = get_element_type(c->consttype);
	if (base_el_type == InvalidOid)
		elog(ERROR, "Couldn't get base element type from array type: %d", c->consttype);

	return dimension_values_create(values, base_el_type, user_or);
}

static DimensionValues *
dimension_values_create_from_single_element(Const *c, bool user_or)
{
	return dimension_values_create(list_make1(DatumGetPointer(c->constvalue)), c->consttype, user_or);
}

static void
hypertable_restrict_info_add_restrict_info(HypertableRestrictInfo *hri, PlannerInfo *root, RestrictInfo *ri)
{
	bool		added = false;

	Expr	   *e = ri->clause;

	/* Same as constraint_exclusion */
	if (contain_mutable_functions((Node *) e))
		return;

	switch (nodeTag(e))
	{
		case T_OpExpr:
			{
				OpExpr	   *op_expr = (OpExpr *) e;

				added = hypertable_restrict_info_add_expr(hri, root, op_expr->args, op_expr->opno, dimension_values_create_from_single_element, false);
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *scalar_expr = (ScalarArrayOpExpr *) e;

				added = hypertable_restrict_info_add_expr(hri, root, scalar_expr->args, scalar_expr->opno, dimension_values_create_from_array, scalar_expr->useOr);
				break;
			}
		default:
			/* we don't support other node types */
			break;
	}

	if (added)
		hri->num_base_restrictions++;
}

void
ts_hypertable_restrict_info_add(HypertableRestrictInfo *hri,
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
ts_hypertable_restrict_info_has_restrictions(HypertableRestrictInfo *hri)
{
	return hri->num_base_restrictions > 0;
}

List *
ts_hypertable_restrict_info_get_chunk_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode)
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

	return ts_chunk_find_all_oids(ht->space, dimension_vecs, lockmode);
}

List *
ts_hypertable_restrict_info_get_chunk_oids_ordered(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode, bool reverse)
{
	/*
	 * we only support ordered append for partitioning by 1 dimension
	 * initially
	 *
	 * to support space partitioning as well here this function needs to be
	 * adjusted
	 */
	DimensionRestrictInfo *dri;
	DimensionVec *dv;
	List	   *chunk_oids = NIL;
	int			i;

	/*
	 * ordered append is the only user of this function which checks for
	 * number of dimensions as well so we only assert here
	 */
	Assert(hri->num_dimensions == 1);

	dri = hri->dimension_restriction[0];

	Assert(NULL != dri);

	dv = dimension_restrict_info_slices(dri);

	Assert(dv->num_slices >= 0);

	/*
	 * If there are no matching slices in any single dimension, the result
	 * will be empty
	 */
	if (dv->num_slices == 0)
		return NIL;

	if (reverse)
		ts_dimension_vec_sort_reverse(&dv);
	else
		ts_dimension_vec_sort(&dv);

	for (i = 0; i < dv->num_slices; i++)
	{
		ListCell   *lc;
		List	   *chunk_ids = NIL;
		DimensionSlice *slice = dv->slices[i];

		ts_chunk_constraint_scan_by_dimension_slice_to_list(slice, &chunk_ids, CurrentMemoryContext);

		/*
		 * since we don't support space partitioning here atm, there should
		 * only be 1 chunk per dimension slice
		 */
		Assert(list_length(chunk_ids) <= 1);

		foreach(lc, chunk_ids)
		{
			Chunk	   *chunk = ts_chunk_get_by_id(lfirst_int(lc), 0, true);

			chunk_oids = lappend_oid(chunk_oids, chunk->table_id);
		}
	}

	return chunk_oids;
}
