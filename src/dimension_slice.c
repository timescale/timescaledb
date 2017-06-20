#include <stdlib.h>
#include <postgres.h>
#include <access/relscan.h>

#include "catalog.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "scanner.h"
#include "dimension.h"
#include "chunk_constraint.h"

static DimensionVec *dimension_vec_expand(DimensionVec *vec, int32 new_size);

static inline DimensionSlice *
dimension_slice_from_form_data(Form_dimension_slice fd)
{
	DimensionSlice *ds;
	ds = palloc0(sizeof(DimensionSlice));
	memcpy(&ds->fd, fd, sizeof(FormData_dimension_slice));
	ds->storage_free = NULL;
	ds->storage = NULL;
	return ds;
}

static inline DimensionSlice *
dimension_slice_from_tuple(HeapTuple tuple)
{
	return dimension_slice_from_form_data((Form_dimension_slice ) GETSTRUCT(tuple));
}

static inline Hypercube *
hypercube_alloc(int16 num_dimensions)
{
	Hypercube *hc = palloc0(HYPERCUBE_SIZE(num_dimensions));
	hc->capacity = num_dimensions;
	return hc;
}

static inline void
hypercube_free(Hypercube *hc)
{
	int i;

	for (i = 0; i < hc->num_slices; i++)
		pfree(hc->slices[i]);

	pfree(hc);
}

Hypercube *
hypercube_copy(Hypercube *hc)
{
	Hypercube *copy;
	size_t nbytes = HYPERCUBE_SIZE(hc->capacity);

	copy = palloc(nbytes);
	memcpy(copy, hc, nbytes);

	for(int i = 0; i < hc->num_slices; i++)
	{
		copy->slices[i] = dimension_slice_copy(hc->slices[i]);
	}

	return copy;
}

static bool
dimension_vec_tuple_found(TupleInfo *ti, void *data)
{
	DimensionVec **vecptr = data;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);
	dimension_vec_add_slice(vecptr, slice);
	return true;
}

DimensionVec *
dimension_slice_scan(int32 dimension_id, int64 coordinate)
{
	Catalog    *catalog = catalog_get();
	DimensionVec *vec = dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
	ScanKeyData scankey[3];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.index = catalog->tables[DIMENSION_SLICE].index_ids[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 3,
		.scankey = scankey,
		.data = &vec,
		.tuple_found = dimension_vec_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan for slice matching the dimension's ID and which
	 * encloses the coordinate */
	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessEqualStrategyNumber, F_INT8LE, Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber, F_INT8GT, Int64GetDatum(coordinate));

	scanner_scan(&scanCtx);

	return vec;
}

static bool
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;
	*slice = dimension_slice_from_tuple(ti->tuple);
	return false;
}

static DimensionSlice *
dimension_slice_scan_by_id(int32 dimension_slice_id)
{
	Catalog    *catalog = catalog_get();
	DimensionSlice *slice = NULL;
	ScanKeyData scankey[1];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.index = catalog->tables[DIMENSION_SLICE].index_ids[DIMENSION_SLICE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &slice,
		.tuple_found = dimension_slice_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_slice_id));
	scanner_scan(&scanCtx);

	return slice;
}

DimensionSlice *
dimension_slice_copy(const DimensionSlice *original)
{
	DimensionSlice *new = palloc(sizeof(DimensionSlice));
	memcpy(new, original, sizeof(DimensionSlice));
	return new;
}

static int
cmp_slices_by_dimension_id(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	if (left_slice->fd.dimension_id == right_slice->fd.dimension_id)
		return 0;
	if (left_slice->fd.dimension_id < right_slice->fd.dimension_id)
		return -1;
	return 1;
}


static void
hypercube_slice_sort(Hypercube *hc)
{
	qsort(hc->slices, hc->num_slices, sizeof(DimensionSlice *), cmp_slices_by_dimension_id);
}

Hypercube *
hypercube_from_constraints(ChunkConstraint constraints[], int16 num_constraints)
{
	Hypercube *hc = hypercube_alloc(num_constraints);
	int i;

	for (i = 0; i < num_constraints; i++)
	{
		DimensionSlice *slice = dimension_slice_scan_by_id(constraints[i].fd.dimension_slice_id);
		Assert(slice != NULL);
		hc->slices[hc->num_slices++] = slice;
	}

	hypercube_slice_sort(hc);
	return hc;
}

void dimension_slice_free(DimensionSlice *slice)
{
	if (slice->storage_free != NULL)
		slice->storage_free(slice->storage);
	pfree(slice);
}

static int
cmp_slices(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	if (left_slice->fd.range_start == right_slice->fd.range_start)
	{
		if (left_slice->fd.range_end == right_slice->fd.range_end)
			return 0;

		if (left_slice->fd.range_end > right_slice->fd.range_end)
			return 1;

		return -1;
	}

	if (left_slice->fd.range_start > right_slice->fd.range_start)
		return 1;

	return -1;
}

static int
cmp_coordinate_and_slice(const void *left, const void *right)
{
	int64 coord = *((int64 *) left);
	const DimensionSlice *slice = *((DimensionSlice **) right);

	if (coord < slice->fd.range_start)
		return -1;

	if (coord >= slice->fd.range_end)
		return 1;

	return 0;
}

static DimensionVec *
dimension_vec_expand(DimensionVec *vec, int32 new_capacity)
{
	if (vec != NULL && vec->capacity >= new_capacity)
		return vec;

	if (NULL == vec)
		vec = palloc(DIMENSION_VEC_SIZE(new_capacity));
	else
		vec = repalloc(vec, DIMENSION_VEC_SIZE(new_capacity));

	vec->capacity = new_capacity;

	return vec;
}

DimensionVec *
dimension_vec_create(int32 initial_num_slices)
{
	DimensionVec *vec = dimension_vec_expand(NULL, initial_num_slices);
	vec->capacity = initial_num_slices;
	vec->num_slices = 0;
	return vec;
}

DimensionVec *
dimension_vec_add_slice(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec = *vecptr;

	if (vec->num_slices + 1 > vec->capacity)
		*vecptr = vec = dimension_vec_expand(vec, vec->capacity + 10);

	vec->slices[vec->num_slices++] = slice;

	return vec;
}

DimensionVec *
dimension_vec_add_slice_sort(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec = *vecptr;
	*vecptr = vec = dimension_vec_add_slice(vecptr, slice);
	qsort(vec->slices, vec->num_slices, sizeof(DimensionSlice *), cmp_slices);
	return vec;
}

DimensionSlice *
dimension_vec_find_slice(DimensionVec *vec, int64 coordinate)
{
  DimensionSlice **res = bsearch(&coordinate, vec->slices, vec->num_slices,
								 sizeof(DimensionSlice *), cmp_coordinate_and_slice);
  if (res == NULL)
	  return NULL;

  return *res;
}

void dimension_vec_free(DimensionVec *vec)
{
	int i;

	for (i = 0; i < vec->num_slices; i++)
		dimension_slice_free(vec->slices[i]);
	pfree(vec);
}
