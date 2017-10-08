#include <stdlib.h>
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <access/heapam.h>
#include <utils/rel.h>
#include <catalog/indexing.h>

#include "catalog.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "scanner.h"
#include "dimension.h"
#include "chunk_constraint.h"
#include "dimension_vector.h"

static inline DimensionSlice *
dimension_slice_alloc(void)
{
	return palloc0(sizeof(DimensionSlice));
}

static inline DimensionSlice *
dimension_slice_from_form_data(Form_dimension_slice fd)
{
	DimensionSlice *slice = dimension_slice_alloc();

	memcpy(&slice->fd, fd, sizeof(FormData_dimension_slice));
	slice->storage_free = NULL;
	slice->storage = NULL;
	return slice;
}

static inline DimensionSlice *
dimension_slice_from_tuple(HeapTuple tuple)
{
	return dimension_slice_from_form_data((Form_dimension_slice) GETSTRUCT(tuple));
}

DimensionSlice *
dimension_slice_create(int dimension_id, int64 range_start, int64 range_end)
{
	DimensionSlice *slice = dimension_slice_alloc();

	slice->fd.dimension_id = dimension_id;
	slice->fd.range_start = range_start;
	slice->fd.range_end = range_end;

	return slice;
}

typedef struct DimensionSliceScanData
{
	DimensionVec *slices;
	int			limit;
}	DimensionSliceScanData;

static bool
dimension_vec_tuple_found(TupleInfo *ti, void *data)
{
	DimensionVec *slices = data;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);

	dimension_vec_add_slice(&slices, slice);

	return true;
}

static int
dimension_slice_scan_limit_internal(ScanKeyData *scankey,
									int num_scankeys,
									tuple_found_func on_tuple_found,
									void *scandata,
									int limit)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanCtx = {
		.table = catalog->tables[DIMENSION_SLICE].id,
		.index = catalog->tables[DIMENSION_SLICE].index_ids[DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = num_scankeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	return scanner_scan(&scanCtx);
}

/*
 * Scan for slices that enclose the coordinate in the given dimension.
 *
 * Returns a dimension vector of slices that enclose the coordinate.
 */
DimensionVec *
dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
			 BTLessEqualStrategyNumber, F_INT8LE, Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber, F_INT8GT, Int64GetDatum(coordinate));

	dimension_slice_scan_limit_internal(scankey, 3, dimension_vec_tuple_found, slices, limit);

	return dimension_vec_sort(&slices);
}

/*
 * Scan for slices that collide/overlap with the given range.
 *
 * Returns a dimension vector of colliding slices.
 */
DimensionVec *
dimension_slice_collision_scan_limit(int32 dimension_id, int64 range_start, int64 range_end, int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessStrategyNumber, F_INT8LT, Int64GetDatum(range_end));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
			  BTGreaterStrategyNumber, F_INT8GT, Int64GetDatum(range_start));

	dimension_slice_scan_limit_internal(scankey, 3, dimension_vec_tuple_found, slices, limit);

	return dimension_vec_sort(&slices);
}


static bool
dimension_slice_fill(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;

	memcpy(&(*slice)->fd, GETSTRUCT(ti->tuple), sizeof(FormData_dimension_slice));
	return false;
}

/*
 * Scan for an existing slice that exactly matches the given slice's dimension
 * and range. If a match is found, the given slice is updated with slice ID.
 */
DimensionSlice *
dimension_slice_scan_for_existing(DimensionSlice *slice)
{
	ScanKeyData scankey[3];

	ScanKeyInit(&scankey[0], Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
	 BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(slice->fd.dimension_id));
	ScanKeyInit(&scankey[1], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
	  BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(slice->fd.range_start));
	ScanKeyInit(&scankey[2], Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
		BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(slice->fd.range_end));

	dimension_slice_scan_limit_internal(scankey, 3, dimension_slice_fill, &slice, 1);

	return slice;
}

static bool
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;

	*slice = dimension_slice_from_tuple(ti->tuple);
	return false;
}

DimensionSlice *
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
		.limit = 1,
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

/*
 * Check if two dimensions slices overlap by doing collision detection in one
 * dimension.
 *
 * Returns true if the slices collide, otherwise false.
 */
bool
dimension_slices_collide(DimensionSlice *slice1, DimensionSlice *slice2)
{
	Assert(slice1->fd.dimension_id == slice2->fd.dimension_id);

	return (slice1->fd.range_start < slice2->fd.range_end &&
			slice1->fd.range_end > slice2->fd.range_start);
}

/*
 * Check whether two slices are identical.
 *
 * We require by assertion that the slices are in the same dimension and we only
 * compare the ranges (i.e., the slice ID is not important for equality).
 *
 * Returns true if the slices have identical ranges, otherwise false.
 */
bool
dimension_slices_equal(DimensionSlice *slice1, DimensionSlice *slice2)
{
	Assert(slice1->fd.dimension_id == slice2->fd.dimension_id);

	return slice1->fd.range_start == slice2->fd.range_start &&
		slice1->fd.range_end == slice2->fd.range_end;
}

/*-
 * Cut a slice that collides with another slice. The coordinate is the point of
 * insertion, and determines which end of the slice to cut.
 *
 * Case where we cut "after" the coordinate:
 *
 * ' [-x--------]
 * '      [--------]
 *
 * Case where we cut "before" the coordinate:
 *
 * '      [------x--]
 * ' [--------]
 *
 * Returns true if the slice was cut, otherwise false.
 */
bool
dimension_slice_cut(DimensionSlice *to_cut, DimensionSlice *other, int64 coord)
{
	Assert(to_cut->fd.dimension_id == other->fd.dimension_id);

	if (other->fd.range_end <= coord &&
		other->fd.range_end > to_cut->fd.range_start)
	{
		/* Cut "before" the coordinate */
		to_cut->fd.range_start = other->fd.range_end;
		return true;
	}
	else if (other->fd.range_start > coord &&
			 other->fd.range_start < to_cut->fd.range_end)
	{
		/* Cut "after" the coordinate */
		to_cut->fd.range_end = other->fd.range_start;
		return true;
	}

	return false;
}

void
dimension_slice_free(DimensionSlice *slice)
{
	if (slice->storage_free != NULL)
		slice->storage_free(slice->storage);
	pfree(slice);
}

static bool
dimension_slice_insert_relation(Relation rel, DimensionSlice *slice)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_dimension_slice];
	bool		nulls[Natts_dimension_slice] = {false};

	if (slice->fd.id > 0)
		/* Slice already exists in table */
		return false;

	memset(values, 0, sizeof(values));
	slice->fd.id = catalog_table_next_seq_id(catalog_get(), DIMENSION_SLICE);
	values[Anum_dimension_slice_id - 1] = slice->fd.id;
	values[Anum_dimension_slice_dimension_id - 1] = slice->fd.dimension_id;
	values[Anum_dimension_slice_range_start - 1] = slice->fd.range_start;
	values[Anum_dimension_slice_range_end - 1] = slice->fd.range_end;

	catalog_insert_values(rel, desc, values, nulls);

	return true;
}

/*
 * Insert slices into the catalog.
 */
void
dimension_slice_insert_multi(DimensionSlice **slices, Size num_slices)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	Size		i;

	rel = heap_open(catalog->tables[DIMENSION_SLICE].id, RowExclusiveLock);

	for (i = 0; i < num_slices; i++)
		dimension_slice_insert_relation(rel, slices[i]);

	heap_close(rel, RowExclusiveLock);
}
