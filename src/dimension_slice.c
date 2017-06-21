#include <stdlib.h>
#include <postgres.h>
#include <access/relscan.h>

#include "catalog.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "scanner.h"
#include "dimension.h"
#include "chunk_constraint.h"

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
	hc->num_dimensions = num_dimensions;
	return hc;
}

static inline void
hypercube_free(Hypercube *hc)
{
	int i;

	for (i = 0; i < hc->num_dimensions; i++)
		pfree(hc->slices[i]);

	pfree(hc);
}

static bool
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **ds = data;
	*ds = dimension_slice_from_tuple(ti->tuple);
	return false;
}

DimensionSlice *
dimension_slice_scan(int32 dimension_id, int64 coordinate)
{
	Catalog    *catalog = catalog_get();
	DimensionSlice *slice = NULL;
	ScanKeyData scankey[3];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.index = catalog->tables[DIMENSION_SLICE].index_ids[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 3,
		.scankey = scankey,
		.data = &slice,
		.tuple_found = dimension_slice_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan for slice matching the dimension's ID and which
	 * encloses the coordinate */
	/* FIXME  MAT: I don't think this is right BTGreaterEqualStrategyNumber searches for rows >= target 
	 * I think. Also I don't think BTLessStrategyNumber can be used with a forward scan
	 * */
	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTGreaterEqualStrategyNumber, F_INT8GE, Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTLessStrategyNumber, F_INT8LT, Int64GetDatum(coordinate));

	scanner_scan(&scanCtx);

	return slice;
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
	
	return hc;
}

static inline bool
scan_dimensions(Hypercube *hc, Dimension *dimensions[], int16 num_dimensions, int64 point[])
{
	int i;

	for (i = 0; i < num_dimensions; i++)
	{
		Dimension *d = dimensions[i];
		DimensionSlice *slice = dimension_slice_scan(d->fd.id, point[i]);

		if (slice == NULL)
			return false;

		hc->slices[hc->num_slices++] = slice;
	}
	return true;
}

Hypercube *
dimension_slice_point_scan(Hyperspace *space, int64 point[])
{
	Hypercube *cube = hypercube_alloc(HYPERSPACE_NUM_DIMENSIONS(space));

	if (!scan_dimensions(cube, space->open_dimensions, space->num_open_dimensions, point)) {
		hypercube_free(cube);
		return NULL;
	}

	if (!scan_dimensions(cube, space->closed_dimensions, space->num_closed_dimensions, point)) {
		hypercube_free(cube);
		return NULL;
	}
	return cube;
}

typedef struct PointScanCtx
{
	Hyperspace *hs;
	Hypercube *hc;
	int64 *point;
} PointScanCtx;

static inline
DimensionSlice *match_dimension_slice(Form_dimension_slice slice, int64 point[],
									  Dimension *dimensions[], int16 num_dimensions)
{
	int i;

	for (i = 0; i < num_dimensions; i++)
	{
		int32 dimension_id = dimensions[i]->fd.id;
		int64 coordinate = point[i];

		if (slice->dimension_id == dimension_id && point_coordinate_is_in_slice(slice, coordinate))
			return dimension_slice_from_form_data(slice);
	}

	return NULL;
}

static bool
point_filter(TupleInfo *ti, void *data)
{
	PointScanCtx *ctx = data;
	Hyperspace *hs = ctx->hs;
	Hypercube *hc = ctx->hc;
	DimensionSlice *slice;

	/* Match open dimension */
	slice = match_dimension_slice((Form_dimension_slice) GETSTRUCT(ti->tuple), ctx->point,
								  hs->open_dimensions, hs->num_open_dimensions);

	if (slice != NULL)
	{
		hc->slices[hc->num_slices++] = slice;
		return HYPERSPACE_NUM_DIMENSIONS(hs) == hc->num_slices;
	}
	
	/* Match closed dimension */
	slice = match_dimension_slice((Form_dimension_slice) GETSTRUCT(ti->tuple), ctx->point,
								  hs->closed_dimensions, hs->num_closed_dimensions);

	if (slice != NULL)
	{
		hc->slices[hc->num_slices++] = slice;
		return HYPERSPACE_NUM_DIMENSIONS(hs) == hc->num_slices;
	}

	return true;
}

/*
 * Given a N-dimensional point, scan for the hypercube that encloses it.
 *
 * NOTE: This assumes non-overlapping slices.
 */
Hypercube *
dimension_slice_point_scan_heap(Hyperspace *space, int64 point[])
{
	Catalog    *catalog = catalog_get();
	Hypercube *cube = hypercube_alloc(HYPERSPACE_NUM_DIMENSIONS(space));
	PointScanCtx ctx = {
		.hs = space,
		.hc = cube,
		.point = point,
	};
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.scantype = ScannerTypeHeap,
		.nkeys = 0,
		.data = &ctx,
		.filter = point_filter,
		.tuple_found = dimension_slice_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	cube->num_slices = 0;

	scanner_scan(&scanCtx);

	return cube;
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

static DimensionAxis *
dimension_axis_expand(DimensionAxis *axis, int32 new_size)
{
	if (axis != NULL && axis->num_slots >= new_size)
		return axis;

	if (axis == NULL) 
	{
		axis = palloc(sizeof(DimensionAxis) + sizeof(DimensionSlice *) * new_size);
	}
	else
	{
		axis = repalloc(axis, sizeof(DimensionAxis) + sizeof(DimensionSlice *) * new_size);
	}
	axis->num_slots = new_size;
	return axis;
}

DimensionAxis *
dimension_axis_create(DimensionType type, int32 num_slices)
{
	DimensionAxis *axis = dimension_axis_expand(NULL, num_slices);
	axis->type = type;
	axis->num_slices = 0;
	return axis;
}

int32
dimension_axis_add_slice(DimensionAxis **axis, DimensionSlice *slice)
{
	if ((*axis)->num_slices + 1 > (*axis)->num_slots)
		*axis = dimension_axis_expand(*axis, (*axis)->num_slots + 10);

	(*axis)->slices[(*axis)->num_slices++] = slice;

	return (*axis)->num_slices;
}

int32
dimension_axis_add_slice_sort(DimensionAxis **axis, DimensionSlice *slice)
{
	dimension_axis_add_slice(axis, slice);
	qsort((*axis)->slices, (*axis)->num_slices, sizeof(DimensionSlice *), cmp_slices);
	return (*axis)->num_slices;
}

DimensionSlice *
dimension_axis_find_slice(DimensionAxis *axis, int64 coordinate)
{
  DimensionSlice ** res = bsearch(&coordinate, axis->slices, axis->num_slices, sizeof(DimensionSlice *), cmp_coordinate_and_slice);
  if (res == NULL)
	  return NULL;
  return *res;
}

void dimension_axis_free(DimensionAxis *axis)
{
	int i;
	
	for (i = 0; i < axis->num_slices; i++)
		dimension_slice_free(axis->slices[i]);
	pfree(axis);
}
