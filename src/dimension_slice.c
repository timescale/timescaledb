#include <postgres.h>

#include <access/relscan.h>

#include "catalog.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "scanner.h"


static inline DimensionSlice *
dimension_slice_from_form_data(Form_dimension_slice fd)
{
	DimensionSlice *ds;
	ds = palloc0(sizeof(DimensionSlice));
	memcpy(&ds->fd, fd, sizeof(FormData_dimension_slice));   
	return ds;
}

static inline DimensionSlice *
dimension_slice_from_tuple(HeapTuple tuple)
{
	return dimension_slice_from_form_data((Form_dimension_slice ) GETSTRUCT(tuple));
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
	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTGreaterEqualStrategyNumber, F_INT8EQ, Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTLessStrategyNumber, F_INT8EQ, Int64GetDatum(coordinate));

	scanner_scan(&scanCtx);

	return slice;
}

Hypercube *
dimension_slice_point_scan(Hyperspace *space, int64 point[])
{
	Catalog    *catalog = catalog_get();
	Hypercube *cube = palloc0(sizeof(Hypercube));
	int i;

	// FIXME: this iteration does not work because time and space dimensions are
	// not back-to-back
	for (i = 0; i < HYPERSPACE_NUM_DIMENSIONS(space); i++)
	{
		Dimension *d = space->dimensions[i];
		DimensionSlice *slice = dimension_slice_scan(d->fd.id, point[i]);

		if (slice == NULL)
		{
			// TODO: free slices
			int j;

			// FIXME: this iteration does not work because slices are not back-to-back
			for (j = 0; j < HYPERCUBE_NUM_SLICES(cube); j++)
				pfree(cube->slices[j]);
			pfree(cube);
			return NULL;
		}

		if (IS_SPACE_DIMENSION(d))
			cube->time_slices[cube->num_time_slices++] = slice;
		else
			cube->space_slices[cube->num_space_slices++] = slice;
	}

	return cube;
}

typedef struct PointScanCtx
{
	Hyperspace *hs;
	Hypercube *hc;
	int64 *point;
} PointScanCtx;

static inline bool
point_in_slice(const Form_dimension_slice slice, const int64 coordinate)
{
	return coordinate >= slice->range_start && coordinate < slice->range_end;
}

static inline
DimensionSlice *match_dimension_slice(Form_dimension_slice slice, int64 point[],
									  Dimension *dimensions[], int16 num_dimensions)
{
	int i;
	
	for (i = 0; i < num_dimensions; i++)
	{
		int32 dimension_id = dimensions[i]->fd.id;
		int64 coordinate = point[i];
		
		if (slice->dimension_id == dimension_id && point_in_slice(slice, coordinate))
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

	/* Match space dimension */
	slice = match_dimension_slice((Form_dimension_slice) GETSTRUCT(ti->tuple), ctx->point,
								  hs->space_dimensions, hs->num_space_dimensions);

	if (slice != NULL)
	{
		hc->space_slices[hc->num_space_slices++] = slice;
		return hs->num_space_dimensions != hc->num_space_slices &&
			hs->num_time_dimensions != hc->num_space_slices;
	}

	/* Match time dimension */
	slice = match_dimension_slice((Form_dimension_slice) GETSTRUCT(ti->tuple), ctx->point,
								  hs->time_dimensions, hs->num_time_dimensions);

	if (slice != NULL)
	{
		hc->time_slices[hc->num_time_slices++] = slice;
		return hs->num_space_dimensions != hc->num_space_slices &&
			hs->num_time_dimensions != hc->num_space_slices;
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
	Hypercube *cube = palloc0(sizeof(Hypercube));
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

	cube->num_time_slices = cube->num_space_slices = 0;
	
	scanner_scan(&scanCtx);

	if (cube->num_time_slices != space->num_time_dimensions ||
		cube->num_space_slices != space->num_space_dimensions)
		return NULL;
	
	return cube;
}
