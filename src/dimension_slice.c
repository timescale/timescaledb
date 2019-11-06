/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <stdlib.h>
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <access/heapam.h>
#include <utils/rel.h>
#include <catalog/indexing.h>
#include <funcapi.h>
#include <utils/lsyscache.h>
#include <catalog/pg_opfamily.h>
#include <catalog/pg_type.h>

#include "bgw_policy/chunk_stats.h"
#include "catalog.h"
#include "chunk.h"
#include "chunk_constraint.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypertable.h"
#include "scanner.h"

#include "compat.h"

/* Put DIMENSION_SLICE_MAXVALUE point in same slice as DIMENSION_SLICE_MAXVALUE-1, always */
/* This avoids the problem with coord < range_end where coord and range_end is an int64 */
#define REMAP_LAST_COORDINATE(coord)                                                               \
	(((coord) == DIMENSION_SLICE_MAXVALUE) ? DIMENSION_SLICE_MAXVALUE - 1 : (coord))

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
ts_dimension_slice_create(int dimension_id, int64 range_start, int64 range_end)
{
	DimensionSlice *slice = dimension_slice_alloc();

	slice->fd.dimension_id = dimension_id;
	slice->fd.range_start = range_start;
	slice->fd.range_end = range_end;

	return slice;
}

int
ts_dimension_slice_cmp(const DimensionSlice *left, const DimensionSlice *right)
{
	int res = DIMENSION_SLICE_RANGE_START_CMP(left, right);

	if (res == 0)
		res = DIMENSION_SLICE_RANGE_END_CMP(left, right);

	return res;
}

int
ts_dimension_slice_cmp_coordinate(const DimensionSlice *slice, int64 coord)
{
	coord = REMAP_LAST_COORDINATE(coord);
	if (coord < slice->fd.range_start)
		return -1;

	if (coord >= slice->fd.range_end)
		return 1;

	return 0;
}

typedef struct DimensionSliceScanData
{
	DimensionVec *slices;
	int limit;
} DimensionSliceScanData;

static ScanTupleResult
dimension_vec_tuple_found(TupleInfo *ti, void *data)
{
	DimensionVec **slices = data;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);

	*slices = ts_dimension_vec_add_slice(slices, slice);

	return SCAN_CONTINUE;
}

static int
dimension_slice_scan_limit_direction_internal(int indexid, ScanKeyData *scankey, int nkeys,
											  tuple_found_func on_tuple_found, void *scandata,
											  int limit, ScanDirection scandir, LOCKMODE lockmode,
											  MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, DIMENSION_SLICE),
		.index = catalog_get_index(catalog, DIMENSION_SLICE, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lockmode,
		.scandirection = scandir,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&scanctx);
}

static int
dimension_slice_scan_limit_internal(int indexid, ScanKeyData *scankey, int nkeys,
									tuple_found_func on_tuple_found, void *scandata, int limit,
									LOCKMODE lockmode, MemoryContext mctx)
{
	return dimension_slice_scan_limit_direction_internal(indexid,
														 scankey,
														 nkeys,
														 on_tuple_found,
														 scandata,
														 limit,
														 ForwardScanDirection,
														 lockmode,
														 mctx);
}

/*
 * Scan for slices that enclose the coordinate in the given dimension.
 *
 * Returns a dimension vector of slices that enclose the coordinate.
 */
DimensionVec *
ts_dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	coordinate = REMAP_LAST_COORDINATE(coordinate);

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessEqualStrategyNumber,
				F_INT8LE,
				Int64GetDatum(coordinate));
	ScanKeyInit(&scankey[2],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber,
				F_INT8GT,
				Int64GetDatum(coordinate));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock,
										CurrentMemoryContext);

	return ts_dimension_vec_sort(&slices);
}

static void
dimension_slice_scan_with_strategies(int32 dimension_id, StrategyNumber start_strategy,
									 int64 start_value, StrategyNumber end_strategy,
									 int64 end_value, void *data, tuple_found_func tuple_found,
									 int limit)
{
	ScanKeyData scankey[3];
	int nkeys = 1;

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));
	if (start_strategy != InvalidStrategy)
	{
		Oid opno = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT8OID, INT8OID, start_strategy);
		Oid proc = get_opcode(opno);

		Assert(OidIsValid(proc));

		ScanKeyInit(&scankey[nkeys++],
					Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
					start_strategy,
					proc,
					Int64GetDatum(start_value));
	}
	if (end_strategy != InvalidStrategy)
	{
		Oid opno = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT8OID, INT8OID, end_strategy);
		Oid proc = get_opcode(opno);

		Assert(OidIsValid(proc));

		/*
		 * range_end is stored as exclusive, so add 1 to the value being
		 * searched. Also avoid overflow
		 */
		if (end_value != PG_INT64_MAX)
		{
			end_value++;

			/*
			 * If getting as input INT64_MAX-1, need to remap the incremented
			 * value back to INT64_MAX-1
			 */
			end_value = REMAP_LAST_COORDINATE(end_value);
		}
		else
		{
			/*
			 * The point with INT64_MAX gets mapped to INT64_MAX-1 so
			 * incrementing that gets you to INT_64MAX
			 */
			end_value = PG_INT64_MAX;
		}

		ScanKeyInit(&scankey[nkeys++],
					Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
					end_strategy,
					proc,
					Int64GetDatum(end_value));
	}

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										nkeys,
										tuple_found,
										data,
										limit,
										AccessShareLock,
										CurrentMemoryContext);
}

/*
 * Look for all dimension slices where (lower_bound, upper_bound) of the dimension_slice contains
 * the given (start_value, end_value) range
 *
 */
DimensionVec *
ts_dimension_slice_scan_range_limit(int32 dimension_id, StrategyNumber start_strategy,
									int64 start_value, StrategyNumber end_strategy, int64 end_value,
									int limit)
{
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	dimension_slice_scan_with_strategies(dimension_id,
										 start_strategy,
										 start_value,
										 end_strategy,
										 end_value,
										 &slices,
										 dimension_vec_tuple_found,
										 limit);

	return ts_dimension_vec_sort(&slices);
}

/*
 * Scan for slices that collide/overlap with the given range.
 *
 * Returns a dimension vector of colliding slices.
 */
DimensionVec *
ts_dimension_slice_collision_scan_limit(int32 dimension_id, int64 range_start, int64 range_end,
										int limit)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessStrategyNumber,
				F_INT8LT,
				Int64GetDatum(range_end));
	ScanKeyInit(&scankey[2],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTGreaterStrategyNumber,
				F_INT8GT,
				Int64GetDatum(range_start));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock,
										CurrentMemoryContext);

	return ts_dimension_vec_sort(&slices);
}

DimensionVec *
ts_dimension_slice_scan_by_dimension(int32 dimension_id, int limit)
{
	ScanKeyData scankey[1];
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										1,
										dimension_vec_tuple_found,
										&slices,
										limit,
										AccessShareLock,
										CurrentMemoryContext);

	return ts_dimension_vec_sort(&slices);
}

/*
 * Return slices that occur "before" the given point.
 *
 * The slices will be allocated on the given memory context. Note, however, that
 * the returned dimension vector is allocated on the current memory context.
 */
DimensionVec *
ts_dimension_slice_scan_by_dimension_before_point(int32 dimension_id, int64 point, int limit,
												  ScanDirection scandir, MemoryContext mctx)
{
	ScanKeyData scankey[3];
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));
	ScanKeyInit(&scankey[1],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTLessStrategyNumber,
				F_INT8LT,
				Int64GetDatum(point));
	ScanKeyInit(&scankey[2],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTLessStrategyNumber,
				F_INT8LT,
				Int64GetDatum(point));

	dimension_slice_scan_limit_direction_internal(
		DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
		scankey,
		3,
		dimension_vec_tuple_found,
		&slices,
		limit,
		scandir,
		AccessShareLock,
		mctx);

	return ts_dimension_vec_sort(&slices);
}

static ScanTupleResult
dimension_slice_tuple_delete(TupleInfo *ti, void *data)
{
	bool isnull;
	Datum dimension_slice_id = heap_getattr(ti->tuple, Anum_dimension_slice_id, ti->desc, &isnull);
	bool *delete_constraints = data;
	CatalogSecurityContext sec_ctx;

	Assert(!isnull);

	/* delete chunk constraints */
	if (NULL != delete_constraints && *delete_constraints)
		ts_chunk_constraint_delete_by_dimension_slice_id(DatumGetInt32(dimension_slice_id));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_dimension_slice_delete_by_dimension_id(int32 dimension_id, bool delete_constraints)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	return dimension_slice_scan_limit_internal(
		DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
		scankey,
		1,
		dimension_slice_tuple_delete,
		&delete_constraints,
		0,
		RowExclusiveLock,
		CurrentMemoryContext);
}

int
ts_dimension_slice_delete_by_id(int32 dimension_slice_id, bool delete_constraints)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_slice_id));

	return dimension_slice_scan_limit_internal(DIMENSION_SLICE_ID_IDX,
											   scankey,
											   1,
											   dimension_slice_tuple_delete,
											   &delete_constraints,
											   1,
											   RowExclusiveLock,
											   CurrentMemoryContext);
}

static ScanTupleResult
dimension_slice_fill(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;

	memcpy(&(*slice)->fd, GETSTRUCT(ti->tuple), sizeof(FormData_dimension_slice));
	return SCAN_DONE;
}

/*
 * Scan for an existing slice that exactly matches the given slice's dimension
 * and range. If a match is found, the given slice is updated with slice ID.
 */
DimensionSlice *
ts_dimension_slice_scan_for_existing(DimensionSlice *slice)
{
	ScanKeyData scankey[3];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(slice->fd.dimension_id));
	ScanKeyInit(&scankey[1],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_start,
				BTEqualStrategyNumber,
				F_INT8EQ,
				Int64GetDatum(slice->fd.range_start));
	ScanKeyInit(&scankey[2],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
				BTEqualStrategyNumber,
				F_INT8EQ,
				Int64GetDatum(slice->fd.range_end));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_slice_fill,
										&slice,
										1,
										AccessShareLock,
										CurrentMemoryContext);

	return slice;
}

static ScanTupleResult
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;
	MemoryContext old = MemoryContextSwitchTo(ti->mctx);

	*slice = dimension_slice_from_tuple(ti->tuple);
	MemoryContextSwitchTo(old);
	return SCAN_DONE;
}

DimensionSlice *
ts_dimension_slice_scan_by_id(int32 dimension_slice_id, MemoryContext mctx)
{
	DimensionSlice *slice = NULL;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_id_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_slice_id));

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_ID_IDX,
										scankey,
										1,
										dimension_slice_tuple_found,
										&slice,
										1,
										AccessShareLock,
										mctx);

	return slice;
}

DimensionSlice *
ts_dimension_slice_copy(const DimensionSlice *original)
{
	DimensionSlice *new = palloc(sizeof(DimensionSlice));

	Assert(original->storage == NULL);
	Assert(original->storage_free == NULL);

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
ts_dimension_slices_collide(DimensionSlice *slice1, DimensionSlice *slice2)
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
ts_dimension_slices_equal(DimensionSlice *slice1, DimensionSlice *slice2)
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
ts_dimension_slice_cut(DimensionSlice *to_cut, DimensionSlice *other, int64 coord)
{
	Assert(to_cut->fd.dimension_id == other->fd.dimension_id);

	coord = REMAP_LAST_COORDINATE(coord);

	if (other->fd.range_end <= coord && other->fd.range_end > to_cut->fd.range_start)
	{
		/* Cut "before" the coordinate */
		to_cut->fd.range_start = other->fd.range_end;

		return true;
	}
	else if (other->fd.range_start > coord && other->fd.range_start < to_cut->fd.range_end)
	{
		/* Cut "after" the coordinate */
		to_cut->fd.range_end = other->fd.range_start;

		return true;
	}

	return false;
}

void
ts_dimension_slice_free(DimensionSlice *slice)
{
	if (slice->storage_free != NULL)
		slice->storage_free(slice->storage);
	pfree(slice);
}

static bool
dimension_slice_insert_relation(Relation rel, DimensionSlice *slice)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_dimension_slice];
	bool nulls[Natts_dimension_slice] = { false };
	CatalogSecurityContext sec_ctx;

	if (slice->fd.id > 0)
		/* Slice already exists in table */
		return false;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	memset(values, 0, sizeof(values));
	slice->fd.id = ts_catalog_table_next_seq_id(ts_catalog_get(), DIMENSION_SLICE);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_id)] = Int32GetDatum(slice->fd.id);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_dimension_id)] =
		Int32GetDatum(slice->fd.dimension_id);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_start)] =
		Int64GetDatum(slice->fd.range_start);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_end)] =
		Int64GetDatum(slice->fd.range_end);

	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	return true;
}

/*
 * Insert slices into the catalog.
 */
void
ts_dimension_slice_insert_multi(DimensionSlice **slices, Size num_slices)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	Size i;

	rel = table_open(catalog_get_table_id(catalog, DIMENSION_SLICE), RowExclusiveLock);

	for (i = 0; i < num_slices; i++)
		dimension_slice_insert_relation(rel, slices[i]);

	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
dimension_slice_nth_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;
	MemoryContext old = MemoryContextSwitchTo(ti->mctx);

	*slice = dimension_slice_from_tuple(ti->tuple);
	MemoryContextSwitchTo(old);
	return SCAN_CONTINUE;
}

DimensionSlice *
ts_dimension_slice_nth_latest_slice(int32 dimension_id, int n)
{
	ScanKeyData scankey[1];
	int num_tuples;
	DimensionSlice *ret = NULL;

	ScanKeyInit(&scankey[0],
				Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_id));

	num_tuples = dimension_slice_scan_limit_direction_internal(
		DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
		scankey,
		1,
		dimension_slice_nth_tuple_found,
		&ret,
		n,
		BackwardScanDirection,
		AccessShareLock,
		CurrentMemoryContext);
	if (num_tuples < n)
		return NULL;

	return ret;
}

typedef struct ChunkStatInfo
{
	int32 chunk_id;
	int32 job_id;
} ChunkStatInfo;

static ScanTupleResult
dimension_slice_check_chunk_stats_tuple_found(TupleInfo *ti, void *data)
{
	ListCell *lc;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);
	List *chunk_ids = NIL;
	ChunkStatInfo *info = data;

	ts_chunk_constraint_scan_by_dimension_slice_to_list(slice, &chunk_ids, CurrentMemoryContext);

	foreach (lc, chunk_ids)
	{
		BgwPolicyChunkStats *chunk_stat =
			ts_bgw_policy_chunk_stats_find(info->job_id, lfirst_int(lc));

		if (chunk_stat == NULL || chunk_stat->fd.num_times_job_run == 0)
		{
			/* Save the chunk_id */
			info->chunk_id = lfirst_int(lc);
			return SCAN_DONE;
		}
	}

	return SCAN_CONTINUE;
}

int
ts_dimension_slice_oldest_chunk_without_executed_job(int32 job_id, int32 dimension_id,
													 StrategyNumber start_strategy,
													 int64 start_value, StrategyNumber end_strategy,
													 int64 end_value)
{
	ChunkStatInfo info = {
		.job_id = job_id,
		.chunk_id = -1,
	};

	dimension_slice_scan_with_strategies(dimension_id,
										 start_strategy,
										 start_value,
										 end_strategy,
										 end_value,
										 &info,
										 dimension_slice_check_chunk_stats_tuple_found,
										 -1);

	return info.chunk_id;
}

static ScanTupleResult
dimension_slice_check_is_chunk_uncompressed_tuple_found(TupleInfo *ti, void *data)
{
	ListCell *lc;
	DimensionSlice *slice = dimension_slice_from_tuple(ti->tuple);
	List *chunk_ids = NIL;

	ts_chunk_constraint_scan_by_dimension_slice_to_list(slice, &chunk_ids, CurrentMemoryContext);

	foreach (lc, chunk_ids)
	{
		int32 chunk_id = lfirst_int(lc);
		if (ts_chunk_can_be_compressed(chunk_id))
		{
			/* found a chunk that has not yet been compressed */
			*((int32 *) data) = chunk_id;
			return SCAN_DONE;
		}
	}

	return SCAN_CONTINUE;
}

int32
ts_dimension_slice_get_chunkid_to_compress(int32 dimension_id, StrategyNumber start_strategy,
										   int64 start_value, StrategyNumber end_strategy,
										   int64 end_value)
{
	int32 chunk_id_ret = INVALID_CHUNK_ID;
	dimension_slice_scan_with_strategies(dimension_id,
										 start_strategy,
										 start_value,
										 end_strategy,
										 end_value,
										 &chunk_id_ret,
										 dimension_slice_check_is_chunk_uncompressed_tuple_found,
										 -1);

	return chunk_id_ret;
}
