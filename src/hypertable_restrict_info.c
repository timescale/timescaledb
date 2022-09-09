/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <catalog/pg_inherits.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>

#include "hypertable_restrict_info.h"

#include "chunk.h"
#include "chunk_scan.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "partitioning.h"
#include "scan_iterator.h"
#include "utils.h"

#include <inttypes.h>
#include <tcop/tcopprot.h>

typedef struct DimensionRestrictInfo
{
	const Dimension *dimension;
} DimensionRestrictInfo;

typedef struct DimensionRestrictInfoOpen
{
	DimensionRestrictInfo base;
	int64 lower_bound; /* internal time representation */
	StrategyNumber lower_strategy;
	int64 upper_bound; /* internal time representation */
	StrategyNumber upper_strategy;
} DimensionRestrictInfoOpen;

typedef struct DimensionRestrictInfoClosed
{
	DimensionRestrictInfo base;
	List *partitions;		 /* hash values */
	StrategyNumber strategy; /* either Invalid or equal */
} DimensionRestrictInfoClosed;

typedef struct DimensionValues
{
	List *values;
	bool use_or; /* ORed or ANDed values */
	Oid type;	/* Oid type for values */
} DimensionValues;

static DimensionRestrictInfoOpen *
dimension_restrict_info_open_create(const Dimension *d)
{
	DimensionRestrictInfoOpen *new = palloc(sizeof(DimensionRestrictInfoOpen));

	new->base.dimension = d;
	new->lower_strategy = InvalidStrategy;
	new->upper_strategy = InvalidStrategy;
	return new;
}

static DimensionRestrictInfoClosed *
dimension_restrict_info_closed_create(const Dimension *d)
{
	DimensionRestrictInfoClosed *new = palloc(sizeof(DimensionRestrictInfoClosed));

	new->partitions = NIL;
	new->base.dimension = d;
	new->strategy = InvalidStrategy;
	return new;
}

static DimensionRestrictInfo *
dimension_restrict_info_create(const Dimension *d)
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

/*
 * Check if the restriction on this dimension is trivial, that is, the entire
 * range of the dimension matches.
 */
static bool
dimension_restrict_info_is_trivial(const DimensionRestrictInfo *dri)
{
	switch (dri->dimension->type)
	{
		case DIMENSION_TYPE_OPEN:
		{
			DimensionRestrictInfoOpen *open = (DimensionRestrictInfoOpen *) dri;
			return open->lower_strategy == InvalidStrategy &&
				   open->upper_strategy == InvalidStrategy;
		}
		case DIMENSION_TYPE_CLOSED:
			return ((DimensionRestrictInfoClosed *) dri)->strategy == InvalidStrategy;
		default:
			Assert(false);
			return false;
	}
}

static bool
dimension_restrict_info_open_add(DimensionRestrictInfoOpen *dri, StrategyNumber strategy,
								 Oid collation, DimensionValues *dimvalues)
{
	ListCell *item;
	bool restriction_added = false;

	/* can't handle IN/ANY with multiple values */
	if (dimvalues->use_or && list_length(dimvalues->values) > 1)
		return false;

	foreach (item, dimvalues->values)
	{
		Oid restype;
		Datum datum = ts_dimension_transform_value(dri->base.dimension,
												   collation,
												   PointerGetDatum(lfirst(item)),
												   dimvalues->type,
												   &restype);
		int64 value = ts_time_value_to_internal_or_infinite(datum, restype, NULL);

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
dimension_restrict_info_get_partitions(DimensionRestrictInfoClosed *dri, Oid collation,
									   List *values)
{
	List *partitions = NIL;
	ListCell *item;

	foreach (item, values)
	{
		Datum value = ts_dimension_transform_value(dri->base.dimension,
												   collation,
												   PointerGetDatum(lfirst(item)),
												   InvalidOid,
												   NULL);

		partitions = list_append_unique_int(partitions, DatumGetInt32(value));
	}

	return partitions;
}

static bool
dimension_restrict_info_closed_add(DimensionRestrictInfoClosed *dri, StrategyNumber strategy,
								   Oid collation, DimensionValues *dimvalues)
{
	List *partitions;
	bool restriction_added = false;

	if (strategy != BTEqualStrategyNumber)
	{
		return false;
	}

	partitions = dimension_restrict_info_get_partitions(dri, collation, dimvalues->values);

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
dimension_restrict_info_add(DimensionRestrictInfo *dri, int strategy, Oid collation,
							DimensionValues *values)
{
	switch (dri->dimension->type)
	{
		case DIMENSION_TYPE_OPEN:
			return dimension_restrict_info_open_add((DimensionRestrictInfoOpen *) dri,
													strategy,
													collation,
													values);
		case DIMENSION_TYPE_CLOSED:
			return dimension_restrict_info_closed_add((DimensionRestrictInfoClosed *) dri,
													  strategy,
													  collation,
													  values);
		default:
			elog(ERROR, "unknown dimension type: %d", dri->dimension->type);
			/* suppress compiler warning on MSVC */
			return false;
	}
}

typedef struct HypertableRestrictInfo
{
	int num_base_restrictions; /* number of base restrictions
								* successfully added */
	int num_dimensions;
	DimensionRestrictInfo *dimension_restriction[FLEXIBLE_ARRAY_MEMBER]; /* array of dimension
																		  * restrictions */
} HypertableRestrictInfo;

HypertableRestrictInfo *
ts_hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht)
{
	int num_dimensions = ht->space->num_dimensions;
	HypertableRestrictInfo *res =
		palloc0(sizeof(HypertableRestrictInfo) + sizeof(DimensionRestrictInfo *) * num_dimensions);
	int i;

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
	int i;

	for (i = 0; i < hri->num_dimensions; i++)
	{
		if (hri->dimension_restriction[i]->dimension->column_attno == attno)
			return hri->dimension_restriction[i];
	}
	return NULL;
}

typedef DimensionValues *(*get_dimension_values)(Const *c, bool use_or);

static bool
hypertable_restrict_info_add_expr(HypertableRestrictInfo *hri, PlannerInfo *root, List *expr_args,
								  Oid op_oid, get_dimension_values func_get_dim_values, bool use_or)
{
	Expr *leftop, *rightop, *expr;
	DimensionRestrictInfo *dri;
	Var *v;
	Const *c;
	RangeTblEntry *rte;
	Oid columntype;
	TypeCacheEntry *tce;
	int strategy;
	Oid lefttype, righttype;
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

	if (!IsA(expr, Const) || !OidIsValid(op_oid) || !op_strict(op_oid))
		return false;

	c = (Const *) expr;

	/* quick check for a NULL constant */
	if (c->constisnull)
		return false;

	rte = rt_fetch(v->varno, root->parse->rtable);

	columntype = get_atttype(rte->relid, dri->dimension->column_attno);
	tce = lookup_type_cache(columntype, TYPECACHE_BTREE_OPFAMILY);

	if (!op_in_opfamily(op_oid, tce->btree_opf))
		return false;

	get_op_opfamily_properties(op_oid, tce->btree_opf, false, &strategy, &lefttype, &righttype);
	dimvalues = func_get_dim_values(c, use_or);
	return dimension_restrict_info_add(dri, strategy, c->constcollid, dimvalues);
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
	Datum elem = (Datum) NULL;
	bool isnull;
	List *values = NIL;
	Oid base_el_type;

	while (array_iterate(iterator, &elem, &isnull))
	{
		if (!isnull)
			values = lappend(values, DatumGetPointer(elem));
	}

	/* it's an array type, lets get the base element type */
	base_el_type = get_element_type(c->consttype);
	if (base_el_type == InvalidOid)
		elog(ERROR,
			 "invalid base element type for array type: \"%s\"",
			 format_type_be(c->consttype));

	return dimension_values_create(values, base_el_type, user_or);
}

static DimensionValues *
dimension_values_create_from_single_element(Const *c, bool user_or)
{
	return dimension_values_create(list_make1(DatumGetPointer(c->constvalue)),
								   c->consttype,
								   user_or);
}

static void
hypertable_restrict_info_add_restrict_info(HypertableRestrictInfo *hri, PlannerInfo *root,
										   RestrictInfo *ri)
{
	bool added = false;

	Expr *e = ri->clause;

	/* Same as constraint_exclusion */
	if (contain_mutable_functions((Node *) e))
		return;

	switch (nodeTag(e))
	{
		case T_OpExpr:
		{
			OpExpr *op_expr = (OpExpr *) e;

			added = hypertable_restrict_info_add_expr(hri,
													  root,
													  op_expr->args,
													  op_expr->opno,
													  dimension_values_create_from_single_element,
													  false);
			break;
		}

		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *scalar_expr = (ScalarArrayOpExpr *) e;

			added = hypertable_restrict_info_add_expr(hri,
													  root,
													  scalar_expr->args,
													  scalar_expr->opno,
													  dimension_values_create_from_array,
													  scalar_expr->useOr);
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
ts_hypertable_restrict_info_add(HypertableRestrictInfo *hri, PlannerInfo *root,
								List *base_restrict_infos)
{
	ListCell *lc;

	foreach (lc, base_restrict_infos)
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

/*
 * Scan for dimension slices matching query constraints.
 *
 * Matching slices are appended to to the given dimension vector. Note that we
 * keep the table and index open as long as we do not change the number of
 * scan keys. If the keys change, but the number of keys is the same, we can
 * simply "rescan". If the number of keys change, however, we need to end the
 * scan and start again.
 */
static DimensionVec *
scan_and_append_slices(ScanIterator *it, int old_nkeys, DimensionVec **dv, bool unique)
{
	if (old_nkeys != -1 && old_nkeys != it->ctx.nkeys)
		ts_scan_iterator_end(it);

	ts_scan_iterator_start_or_restart_scan(it);

	while (ts_scan_iterator_next(it))
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(it);
		DimensionSlice *slice = ts_dimension_slice_from_tuple(ti);

		if (NULL != slice)
		{
			if (unique)
				*dv = ts_dimension_vec_add_unique_slice(dv, slice);
			else
				*dv = ts_dimension_vec_add_slice(dv, slice);
		}
	}

	return *dv;
}

/* search dimension_slice catalog table for slices that meet hri restriction
 */
static List *
gather_restriction_dimension_vectors(const HypertableRestrictInfo *hri)
{
	List *dimension_vecs = NIL;
	ScanIterator it;
	int i;
	int old_nkeys = -1;

	it = ts_dimension_slice_scan_iterator_create(NULL, CurrentMemoryContext);

	for (i = 0; i < hri->num_dimensions; i++)
	{
		const DimensionRestrictInfo *dri = hri->dimension_restriction[i];
		DimensionVec *dv = ts_dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);

		Assert(NULL != dri);

		switch (dri->dimension->type)
		{
			case DIMENSION_TYPE_OPEN:
			{
				const DimensionRestrictInfoOpen *open = (const DimensionRestrictInfoOpen *) dri;

				ts_dimension_slice_scan_iterator_set_range(&it,
														   open->base.dimension->fd.id,
														   open->upper_strategy,
														   open->upper_bound,
														   open->lower_strategy,
														   open->lower_bound);

				/*
				 * If we have a condition on the second index column
				 * range_start, use a backward scan direction, so that the index
				 * is able to use the second column as well to choose the
				 * starting point for the scan.
				 * If not, prefer forward direction, because backwards scan is
				 * slightly slower for some reason.
				 * Ideally we need some other index type than btree for this,
				 * because the btree index is not so suited for queries like
				 * "find an interval that contains a given point", which is what
				 * we're doing here.
				 * There is a comment in the Postgres code (_bt_start()) that
				 * explains the logic of selecting a starting point for a btree
				 * index scan in more detail.
				 */
				it.ctx.scandirection = open->upper_strategy != InvalidStrategy ?
										   BackwardScanDirection :
										   ForwardScanDirection;

				dv = scan_and_append_slices(&it, old_nkeys, &dv, false);
				break;
			}
			case DIMENSION_TYPE_CLOSED:
			{
				const DimensionRestrictInfoClosed *closed =
					(const DimensionRestrictInfoClosed *) dri;

				/* Shouldn't have trivial restriction infos here. */
				Assert(closed->strategy == BTEqualStrategyNumber);

				ListCell *cell;
				foreach (cell, closed->partitions)
				{
					int32 partition = lfirst_int(cell);

					/*
					 * slice_end >= value && slice_start <= value.
					 * See the comment about scan direction above.
					 */
					it.ctx.scandirection = BackwardScanDirection;
					ts_dimension_slice_scan_iterator_set_range(&it,
															   dri->dimension->fd.id,
															   BTLessEqualStrategyNumber,
															   partition,
															   BTGreaterEqualStrategyNumber,
															   partition);

					dv = scan_and_append_slices(&it, old_nkeys, &dv, true);
				}
				break;
			}
			default:
				elog(ERROR, "unknown dimension type");
				return NULL;
		}

		Assert(dv->num_slices >= 0);

		/*
		 * If there is a dimension where no slices match, the result will be
		 * empty.
		 */
		if (dv->num_slices == 0)
		{
			ts_scan_iterator_close(&it);
			return NIL;
		}

		dv = ts_dimension_vec_sort(&dv);
		dimension_vecs = lappend(dimension_vecs, dv);
		old_nkeys = it.ctx.nkeys;
	}

	ts_scan_iterator_close(&it);

	Assert(list_length(dimension_vecs) == hri->num_dimensions);

	return dimension_vecs;
}

Chunk **
ts_hypertable_restrict_info_get_chunks(HypertableRestrictInfo *hri, Hypertable *ht,
									   unsigned int *num_chunks)
{
	/*
	 * Remove the dimensions for which we don't have a restriction, that is,
	 * the entire range of the dimension matches. Such dimensions do not
	 * influence the result set, because their every slice matches, so we can
	 * just ignore them when searching for the matching chunks.
	 */
	const int old_dimensions = hri->num_dimensions;
	hri->num_dimensions = 0;
	for (int i = 0; i < old_dimensions; i++)
	{
		DimensionRestrictInfo *dri = hri->dimension_restriction[i];
		if (!dimension_restrict_info_is_trivial(dri))
		{
			hri->dimension_restriction[hri->num_dimensions] = dri;
			hri->num_dimensions++;
		}
	}

	List *chunk_ids = NIL;
	if (hri->num_dimensions == 0)
	{
		/*
		 * No restrictions on hyperspace. Just enumerate all the chunks.
		 */
		chunk_ids = ts_chunk_get_chunk_ids_by_hypertable_id(ht->fd.id);
	}
	else
	{
		/*
		 * Have some restrictions, enumerate the matching dimension slices.
		 */
		List *dimension_vectors = gather_restriction_dimension_vectors(hri);
		if (list_length(dimension_vectors) == 0)
		{
			/*
			 * No dimension slices match for some dimension for which there is
			 * a restriction. This means that no chunks match.
			 */
			chunk_ids = NIL;
		}
		else
		{
			/* Find the chunks matching these dimension slices. */
			chunk_ids = ts_chunk_id_find_in_subspace(ht, dimension_vectors);
		}

		/*
		 * Always include the OSM chunk if we have one. It has some virtual
		 * dimension slices (at the moment, (+inf, +inf) slice for time, but it
		 * used to be different and might change again.) So sometimes it will
		 * match and sometimes it won't, so we have to check if it's already
		 * there not to add a duplicate.
		 */
		int32 osm_chunk_id = ts_chunk_get_osm_chunk_id(ht->fd.id);
		if (osm_chunk_id != 0 && !list_member_int(chunk_ids, osm_chunk_id))
		{
			chunk_ids = lappend_int(chunk_ids, osm_chunk_id);
		}
	}

	/*
	 * Sort the ids to have more favorable (closer to sequential) data access
	 * patterns to our catalog tables and indexes.
	 * We don't care about the locking order here, because this code uses
	 * AccessShareLock that doesn't conflict with itself.
	 */
	chunk_ids = list_sort_compat(chunk_ids, list_int_cmp_compat);

	return ts_chunk_scan_by_chunk_ids(ht->space, chunk_ids, num_chunks);
}

/*
 * Compare two chunks along first dimension and chunk ID (in that priority and
 * order).
 */
static int
chunk_cmp_impl(const Chunk *c1, const Chunk *c2)
{
	int cmp = ts_dimension_slice_cmp(c1->cube->slices[0], c2->cube->slices[0]);

	if (cmp == 0)
		cmp = VALUE_CMP(c1->fd.id, c2->fd.id);

	return cmp;
}

static int
chunk_cmp(const void *c1, const void *c2)
{
	return chunk_cmp_impl(*((const Chunk **) c1), *((const Chunk **) c2));
}

static int
chunk_cmp_reverse(const void *c1, const void *c2)
{
	return chunk_cmp_impl(*((const Chunk **) c2), *((const Chunk **) c1));
}

/*
 * get chunk oids ordered by time dimension
 *
 * if "chunks" is NULL, we get all the chunks from the catalog. Otherwise we
 * restrict ourselves to the passed in chunks list.
 *
 * nested_oids is a list of lists, chunks that occupy the same time slice will be
 * in the same list. In the list [[1,2,3],[4,5,6]] chunks 1, 2 and 3 are space partitions of
 * the same time slice and 4, 5 and 6 are space partitions of the next time slice.
 *
 */
Chunk **
ts_hypertable_restrict_info_get_chunks_ordered(HypertableRestrictInfo *hri, Hypertable *ht,
											   Chunk **chunks, bool reverse, List **nested_oids,
											   unsigned int *num_chunks)
{
	List *slot_chunk_oids = NIL;
	DimensionSlice *slice = NULL;
	unsigned int i;

	if (chunks == NULL)
	{
		chunks = ts_hypertable_restrict_info_get_chunks(hri, ht, num_chunks);
	}

	if (*num_chunks == 0)
		return NULL;

	Assert(ht->space->num_dimensions > 0);
	Assert(IS_OPEN_DIMENSION(&ht->space->dimensions[0]));

	if (reverse)
		qsort(chunks, *num_chunks, sizeof(Chunk *), chunk_cmp_reverse);
	else
		qsort(chunks, *num_chunks, sizeof(Chunk *), chunk_cmp);

	for (i = 0; i < *num_chunks; i++)
	{
		Chunk *chunk = chunks[i];

		if (NULL != slice && ts_dimension_slice_cmp(slice, chunk->cube->slices[0]) != 0 &&
			slot_chunk_oids != NIL)
		{
			*nested_oids = lappend(*nested_oids, slot_chunk_oids);
			slot_chunk_oids = NIL;
		}

		if (NULL != nested_oids)
			slot_chunk_oids = lappend_oid(slot_chunk_oids, chunk->table_id);

		slice = chunk->cube->slices[0];
	}

	if (slot_chunk_oids != NIL)
		*nested_oids = lappend(*nested_oids, slot_chunk_oids);

	return chunks;
}
