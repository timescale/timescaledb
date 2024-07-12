/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/pg_opfamily.h>
#include <catalog/pg_type.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "bgw_policy/chunk_stats.h"
#include "chunk.h"
#include "chunk_constraint.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypertable.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"

#include "compat/compat.h"

static inline DimensionSlice *
dimension_slice_alloc(void)
{
	return palloc0(sizeof(DimensionSlice));
}

static inline DimensionSlice *
dimension_slice_from_form_data(const Form_dimension_slice fd)
{
	DimensionSlice *slice = dimension_slice_alloc();

	memcpy(&slice->fd, fd, sizeof(FormData_dimension_slice));
	slice->storage_free = NULL;
	slice->storage = NULL;
	return slice;
}

static inline DimensionSlice *
dimension_slice_from_slot(TupleTableSlot *slot)
{
	bool should_free;
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);
	DimensionSlice *slice;

	slice = dimension_slice_from_form_data((Form_dimension_slice) GETSTRUCT(tuple));

	if (should_free)
		heap_freetuple(tuple);

	return slice;
}

static HeapTuple
dimension_slice_formdata_make_tuple(const FormData_dimension_slice *fd, TupleDesc desc)
{
	Datum values[Natts_dimension_slice];
	bool nulls[Natts_dimension_slice] = { false };

	memset(values, 0, sizeof(Datum) * Natts_dimension_slice);

	values[AttrNumberGetAttrOffset(Anum_dimension_slice_id)] = Int32GetDatum(fd->id);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_dimension_id)] =
		Int32GetDatum(fd->dimension_id);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_start)] =
		Int64GetDatum(fd->range_start);
	values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_end)] = Int64GetDatum(fd->range_end);

	return heap_form_tuple(desc, values, nulls);
}

static inline void
dimension_slice_formdata_fill(FormData_dimension_slice *fd, const TupleInfo *ti)
{
	bool nulls[Natts_dimension_slice];
	Datum values[Natts_dimension_slice];
	bool should_free;
	HeapTuple tuple;

	tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	Assert(!nulls[AttrNumberGetAttrOffset(Anum_dimension_slice_id)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_dimension_slice_dimension_id)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_dimension_slice_range_start)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_dimension_slice_range_end)]);

	fd->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_dimension_slice_id)]);
	fd->dimension_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_dimension_slice_dimension_id)]);
	fd->range_start =
		DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_start)]);
	fd->range_end = DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_dimension_slice_range_end)]);

	if (should_free)
		heap_freetuple(tuple);
}

static bool
lock_dimension_slice_tuple(int32 dimension_slice_id, ItemPointer tid,
						   FormData_dimension_slice *form)
{
	bool success = false;
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	ScanIterator iterator =
		ts_scan_iterator_create(DIMENSION_SLICE, RowShareLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), DIMENSION_SLICE, DIMENSION_SLICE_ID_IDX);
	iterator.ctx.tuplock = &scantuplock;
	/* Keeping the lock since we presumably want to update the tuple */
	iterator.ctx.flags = SCANNER_F_KEEPLOCK;

	/* see table_tuple_lock for details about flags that are set in TupleExclusive mode */
	scantuplock.lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
	if (!IsolationUsesXactSnapshot())
	{
		/* in read committed mode, we follow all updates to this tuple */
		scantuplock.lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
	}

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_dimension_slice_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(dimension_slice_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		if (ti->lockresult != TM_Ok)
		{
			if (IsolationUsesXactSnapshot())
			{
				/* For Repeatable Read and Serializable isolation level report error
				 * if we cannot lock the tuple
				 */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to lock hypertable catalog tuple, lock result is %d for "
								"hypertable "
								"ID (%d)",
								ti->lockresult,
								dimension_slice_id)));
			}
		}
		dimension_slice_formdata_fill(form, ti);
		ItemPointer result_tid = ts_scanner_get_tuple_tid(ti);
		tid->ip_blkid = result_tid->ip_blkid;
		tid->ip_posid = result_tid->ip_posid;
		success = true;
		break;
	}
	ts_scan_iterator_close(&iterator);
	return success;
}

/* update the tuple at this tid. The assumption is that we already hold a
 * tuple exclusive lock and no other transaction can modify this tuple
 * The sequence of operations for any update is:
 * lock the tuple using lock_hypertable_tuple.
 * then update the required fields
 * call dimension_slice_update_catalog_tuple to complete the update.
 * This ensures correct tuple locking and tuple updates in the presence of
 * concurrent transactions. Failure to follow this results in catalog corruption
 */
static void
dimension_slice_update_catalog_tuple(ItemPointer tid, FormData_dimension_slice *update)
{
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;
	Catalog *catalog = ts_catalog_get();
	Oid table = catalog_get_table_id(catalog, DIMENSION_SLICE);
	Relation dimension_slice_rel = relation_open(table, RowExclusiveLock);

	new_tuple = dimension_slice_formdata_make_tuple(update, dimension_slice_rel->rd_att);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(dimension_slice_rel, tid, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	relation_close(dimension_slice_rel, NoLock);
}

/* delete the tuple at this tid. The assumption is that we already hold a
 * tuple exclusive lock and no other transaction can modify this tuple
 * The sequence of operations for any delete is:
 * lock the tuple using lock_hypertable_tuple.
 * call dimension_slice_delete_catalog_tuple to complete the delete.
 * This ensures correct tuple locking and tuple deletes in the presence of
 * concurrent transactions. Failure to follow this results in catalog corruption
 */
static void
dimension_slice_delete_catalog_tuple(ItemPointer tid)
{
	CatalogSecurityContext sec_ctx;
	Catalog *catalog = ts_catalog_get();
	Oid table = catalog_get_table_id(catalog, DIMENSION_SLICE);
	Relation dimension_slice_rel = relation_open(table, RowExclusiveLock);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(dimension_slice_rel, tid);
	ts_catalog_restore_user(&sec_ctx);
	relation_close(dimension_slice_rel, NoLock);
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

static bool
tuple_is_deleted(TupleInfo *ti)
{
#ifdef USE_ASSERT_CHECKING
	if (ti->lockresult == TM_Deleted)
		Assert(ItemPointerEquals(ts_scanner_get_tuple_tid(ti), &ti->lockfd.ctid));
#endif
	return ti->lockresult == TM_Deleted;
}

static void
lock_result_ok_or_abort(TupleInfo *ti)
{
	switch (ti->lockresult)
	{
		/* Updating a tuple in the same transaction before taking a lock is OK
		 * even though it is not expected in this case */
		case TM_SelfModified:
		case TM_Ok:
			break;
		case TM_Deleted:
		case TM_Updated:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("chunk %s by other transaction",
							tuple_is_deleted(ti) ? "deleted" : "updated"),
					 errhint("Retry the operation again.")));
			pg_unreachable();
			break;

		case TM_BeingModified:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("chunk updated by other transaction"),
					 errhint("Retry the operation again.")));
			pg_unreachable();
			break;
		case TM_Invisible:
			elog(ERROR, "attempt to lock invisible tuple");
			pg_unreachable();
			break;
		case TM_WouldBlock:
		default:
			elog(ERROR, "unexpected tuple lock status: %d", ti->lockresult);
			pg_unreachable();
			break;
	}
}

static ScanTupleResult
dimension_vec_tuple_found_list(TupleInfo *ti, void *data)
{
	List **slices = data;
	DimensionSlice *slice;
	MemoryContext old;

	switch (ti->lockresult)
	{
		case TM_SelfModified:
		case TM_Ok:
			break;
		case TM_Deleted:
		case TM_Updated:
			/* Treat as not found */
			return SCAN_CONTINUE;
		default:
			elog(ERROR, "unexpected tuple lock status: %d", ti->lockresult);
			pg_unreachable();
			break;
	}

	old = MemoryContextSwitchTo(ti->mctx);
	slice = dimension_slice_from_slot(ti->slot);
	Assert(NULL != slice);
	*slices = lappend(*slices, slice);
	MemoryContextSwitchTo(old);

	return SCAN_CONTINUE;
}

static ScanTupleResult
dimension_vec_tuple_found(TupleInfo *ti, void *data)
{
	DimensionVec **slices = data;
	DimensionSlice *slice;
	MemoryContext old;

	switch (ti->lockresult)
	{
		case TM_SelfModified:
		case TM_Ok:
			break;
		case TM_Deleted:
		case TM_Updated:
			/* Treat as not found */
			return SCAN_CONTINUE;
		default:
			elog(ERROR, "unexpected tuple lock status: %d", ti->lockresult);
			pg_unreachable();
			break;
	}

	old = MemoryContextSwitchTo(ti->mctx);
	slice = dimension_slice_from_slot(ti->slot);
	Assert(NULL != slice);
	*slices = ts_dimension_vec_add_slice(slices, slice);
	MemoryContextSwitchTo(old);

	return SCAN_CONTINUE;
}

static int
dimension_slice_scan_limit_direction_internal(int indexid, ScanKeyData *scankey, int nkeys,
											  tuple_found_func on_tuple_found, void *scandata,
											  int limit, ScanDirection scandir, LOCKMODE lockmode,
											  const ScanTupLock *tuplock, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, DIMENSION_SLICE),
		.index = catalog_get_index(catalog, DIMENSION_SLICE, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuplock = tuplock,
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
									LOCKMODE lockmode, const ScanTupLock *tuplock,
									MemoryContext mctx)
{
	/*
	 * We have =, <=, > ops for index columns, so backwards scan direction is
	 * more appropriate. Forward direction wouldn't be able to use the second
	 * column to find a starting point for the scan. Unfortunately we can't do
	 * anything about the third column, we'll be checking for it with a
	 * sequential scan over index pages. Ideally we need some other index type
	 * than btree for this.
	 */
	return dimension_slice_scan_limit_direction_internal(indexid,
														 scankey,
														 nkeys,
														 on_tuple_found,
														 scandata,
														 limit,
														 BackwardScanDirection,
														 lockmode,
														 tuplock,
														 mctx);
}

/*
 * Scan for slices that enclose the coordinate in the given dimension.
 *
 * Returns a dimension vector of slices that enclose the coordinate.
 */
DimensionVec *
ts_dimension_slice_scan_limit(int32 dimension_id, int64 coordinate, int limit,
							  const ScanTupLock *tuplock)
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
										tuplock,
										CurrentMemoryContext);

	return ts_dimension_vec_sort(&slices);
}

void
ts_dimension_slice_scan_list(int32 dimension_id, int64 coordinate, List **matching_dimension_slices)
{
	coordinate = REMAP_LAST_COORDINATE(coordinate);

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	ScanKeyData scankey[3];
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

	ScanTupLock tuplock = {
		.lockmode = LockTupleKeyShare,
		.waitpolicy = LockWaitBlock,
	};

	dimension_slice_scan_limit_internal(DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
										scankey,
										3,
										dimension_vec_tuple_found_list,
										matching_dimension_slices,
										/* limit = */ 0,
										AccessShareLock,
										&tuplock,
										CurrentMemoryContext);
}

int
ts_dimension_slice_scan_iterator_set_range(ScanIterator *it, int32 dimension_id,
										   StrategyNumber start_strategy, int64 start_value,
										   StrategyNumber end_strategy, int64 end_value)
{
	Catalog *catalog = ts_catalog_get();

	it->ctx.index = catalog_get_index(catalog,
									  DIMENSION_SLICE,
									  DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(
		it,
		Anum_dimension_slice_dimension_id_range_start_range_end_idx_dimension_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(dimension_id));

	/*
	 * Perform an index scan for slices matching the dimension's ID and which
	 * enclose the coordinate.
	 */
	if (start_strategy != InvalidStrategy)
	{
		Oid opno = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT8OID, INT8OID, start_strategy);
		Oid proc = get_opcode(opno);

		Assert(OidIsValid(proc));

		ts_scan_iterator_scan_key_init(
			it,
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

		ts_scan_iterator_scan_key_init(
			it,
			Anum_dimension_slice_dimension_id_range_start_range_end_idx_range_end,
			end_strategy,
			proc,
			Int64GetDatum(end_value));
	}

	return it->ctx.nkeys;
}

/*
 * Look for all dimension slices where (lower_bound, upper_bound) of the dimension_slice contains
 * the given (start_value, end_value) range
 *
 */
DimensionVec *
ts_dimension_slice_scan_range_limit(int32 dimension_id, StrategyNumber start_strategy,
									int64 start_value, StrategyNumber end_strategy, int64 end_value,
									int limit, const ScanTupLock *tuplock)
{
	DimensionVec *slices = ts_dimension_vec_create(limit > 0 ? limit : DIMENSION_VEC_DEFAULT_SIZE);
	ScanIterator it = ts_dimension_slice_scan_iterator_create(tuplock, CurrentMemoryContext);

	ts_dimension_slice_scan_iterator_set_range(&it,
											   dimension_id,
											   start_strategy,
											   start_value,
											   end_strategy,
											   end_value);
	it.ctx.limit = limit;

	ts_scanner_foreach(&it)
	{
		const TupleInfo *ti = ts_scan_iterator_tuple_info(&it);
		DimensionSlice *slice;
		MemoryContext old;

		switch (ti->lockresult)
		{
			case TM_SelfModified:
			case TM_Ok:
				old = MemoryContextSwitchTo(ti->mctx);
				slice = dimension_slice_from_slot(ti->slot);
				Assert(NULL != slice);
				slices = ts_dimension_vec_add_slice(&slices, slice);
				MemoryContextSwitchTo(old);
				break;
			case TM_Deleted:
			case TM_Updated:
				/* Treat as not found */
				break;
			default:
				elog(ERROR, "unexpected tuple lock status: %d", ti->lockresult);
				pg_unreachable();
				break;
		}
	}

	Assert(limit <= 0 || slices->num_slices <= limit);
	ts_scan_iterator_close(&it);

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
										NULL,
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
										NULL,
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
		NULL,
		mctx);

	return ts_dimension_vec_sort(&slices);
}

static ScanTupleResult
dimension_slice_tuple_delete(TupleInfo *ti, void *data)
{
	bool isnull;
	Datum dimension_slice_id = slot_getattr(ti->slot, Anum_dimension_slice_id, &isnull);

	if (ti->lockresult != TM_Ok)
	{
		if (IsolationUsesXactSnapshot())
		{
			/* For Repeatable Read and Serializable isolation level report error
			 * if we cannot lock the tuple
			 */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to lock hypertable catalog tuple, lock result is %d for "
							"hypertable "
							"ID (%d)",
							ti->lockresult,
							DatumGetInt32(dimension_slice_id))));
		}
	}

	bool *delete_constraints = data;
	CatalogSecurityContext sec_ctx;

	Assert(!isnull);

	/* delete chunk constraints */
	if (NULL != delete_constraints && *delete_constraints)
		ts_chunk_constraint_delete_by_dimension_slice_id(DatumGetInt32(dimension_slice_id));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
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

	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};

	return dimension_slice_scan_limit_internal(
		DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
		scankey,
		1,
		dimension_slice_tuple_delete,
		&delete_constraints,
		0,
		RowExclusiveLock,
		&scantuplock,
		CurrentMemoryContext);
}

int
ts_dimension_slice_delete_by_id(int32 dimension_slice_id, bool delete_constraints)
{
	FormData_dimension_slice form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_dimension_slice_tuple(dimension_slice_id, &tid, &form);
	Ensure(found, "dimension slice id %d not found", dimension_slice_id);

	dimension_slice_delete_catalog_tuple(&tid);
	return true;
}

static ScanTupleResult
dimension_slice_fill(TupleInfo *ti, void *data)
{
	switch (ti->lockresult)
	{
		case TM_SelfModified:
		case TM_Ok:
		{
			DimensionSlice **slice = data;
			bool should_free;
			HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

			memcpy(&(*slice)->fd, GETSTRUCT(tuple), sizeof(FormData_dimension_slice));

			if (should_free)
				heap_freetuple(tuple);
			break;
		}
		case TM_Deleted:
		case TM_Updated:
			/* Same as not found */
			break;
		default:
			elog(ERROR, "unexpected tuple lock status: %d", ti->lockresult);
			pg_unreachable();
			break;
	}

	return SCAN_DONE;
}

/*
 * Scan for an existing slice that exactly matches the given slice's dimension
 * and range. If a match is found, the given slice is updated with slice ID
 * and the tuple is locked.
 *
 * Returns true if the dimension slice was found (and locked), false
 * otherwise.
 */
bool
ts_dimension_slice_scan_for_existing(const DimensionSlice *slice, const ScanTupLock *tuplock)
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

	return dimension_slice_scan_limit_internal(
		DIMENSION_SLICE_DIMENSION_ID_RANGE_START_RANGE_END_IDX,
		scankey,
		3,
		dimension_slice_fill,
		(DimensionSlice **) &slice,
		1,
		AccessShareLock,
		tuplock,
		CurrentMemoryContext);
}

DimensionSlice *
ts_dimension_slice_from_tuple(TupleInfo *ti)
{
	DimensionSlice *slice;
	MemoryContext old;

	lock_result_ok_or_abort(ti);
	old = MemoryContextSwitchTo(ti->mctx);
	slice = dimension_slice_from_slot(ti->slot);
	MemoryContextSwitchTo(old);

	return slice;
}

static ScanTupleResult
dimension_slice_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;
	*slice = ts_dimension_slice_from_tuple(ti);
	return SCAN_DONE;
}

/* Scan for a slice by dimension slice id.
 *
 * If you're scanning for a tuple, you have to provide a lock, since, otherwise,
 * concurrent threads can do bad things with the tuple and you probably want
 * it to not change nor disappear. */
DimensionSlice *
ts_dimension_slice_scan_by_id_and_lock(int32 dimension_slice_id, const ScanTupLock *tuplock,
									   MemoryContext mctx, LOCKMODE lockmode)
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
										lockmode,
										tuplock,
										mctx);

	return slice;
}

ScanIterator
ts_dimension_slice_scan_iterator_create(const ScanTupLock *tuplock, MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(DIMENSION_SLICE, AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;
	it.ctx.tuplock = tuplock;

	return it;
}

void
ts_dimension_slice_scan_iterator_set_slice_id(ScanIterator *it, int32 slice_id,
											  const ScanTupLock *tuplock)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(), DIMENSION_SLICE, DIMENSION_SLICE_ID_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_dimension_slice_id_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(slice_id));
	it->ctx.tuplock = tuplock;
}

DimensionSlice *
ts_dimension_slice_scan_iterator_get_by_id(ScanIterator *it, int32 slice_id,
										   const ScanTupLock *tuplock)
{
	TupleInfo *ti;
	DimensionSlice *slice = NULL;

	ts_dimension_slice_scan_iterator_set_slice_id(it, slice_id, tuplock);
	ts_scan_iterator_start_or_restart_scan(it);
	ti = ts_scan_iterator_next(it);
	Assert(ti);

	if (ti)
	{
		slice = ts_dimension_slice_from_tuple(ti);
		Assert(ts_scan_iterator_next(it) == NULL);
	}

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
ts_dimension_slices_collide(const DimensionSlice *slice1, const DimensionSlice *slice2)
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
ts_dimension_slices_equal(const DimensionSlice *slice1, const DimensionSlice *slice2)
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
ts_dimension_slice_cut(DimensionSlice *to_cut, const DimensionSlice *other, int64 coord)
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
dimension_slice_insert_relation(const Relation rel, DimensionSlice *slice)
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
 *
 * Only slices that don't already exist in the catalog will be inserted. Note
 * that all slices that already exist (i.e., have a valid ID) MUST be locked
 * with a tuple lock (e.g., FOR KEY SHARE) prior to calling this function
 * since they won't be created. Otherwise it is not possible to guarantee that
 * all slices still exist once the transaction commits.
 *
 * Returns the number of slices inserted.
 */
int
ts_dimension_slice_insert_multi(DimensionSlice **slices, Size num_slices)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	Size i, n = 0;

	rel = table_open(catalog_get_table_id(catalog, DIMENSION_SLICE), RowExclusiveLock);

	for (i = 0; i < num_slices; i++)
	{
		if (slices[i]->fd.id == 0)
		{
			dimension_slice_insert_relation(rel, slices[i]);
			n++;
		}
	}

	table_close(rel, RowExclusiveLock);

	return n;
}

void
ts_dimension_slice_insert(DimensionSlice *slice)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = table_open(catalog_get_table_id(catalog, DIMENSION_SLICE), RowExclusiveLock);

	dimension_slice_insert_relation(rel, slice);

	/* Keeping a row lock to prevent VACUUM or ALTER TABLE from running while working on the table.
	 * This is known to cause issues in certain situations.
	 */
	table_close(rel, NoLock);
}

static ScanTupleResult
dimension_slice_nth_tuple_found(TupleInfo *ti, void *data)
{
	DimensionSlice **slice = data;
	MemoryContext old = MemoryContextSwitchTo(ti->mctx);

	*slice = dimension_slice_from_slot(ti->slot);
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
		NULL,
		CurrentMemoryContext);
	if (num_tuples < n)
		return NULL;

	return ret;
}

int32
ts_dimension_slice_oldest_valid_chunk_for_reorder(int32 job_id, int32 dimension_id,
												  StrategyNumber start_strategy, int64 start_value,
												  StrategyNumber end_strategy, int64 end_value)
{
	int32 result_chunk_id = -1;
	ScanIterator it = ts_dimension_slice_scan_iterator_create(NULL, CurrentMemoryContext);
	bool done = false;

	ts_dimension_slice_scan_iterator_set_range(&it,
											   dimension_id,
											   start_strategy,
											   start_value,
											   end_strategy,
											   end_value);
	ts_scan_iterator_start_scan(&it);

	while (!done)
	{
		const TupleInfo *ti = ts_scan_iterator_next(&it);
		ListCell *lc;
		DimensionSlice *slice;
		List *chunk_ids = NIL;

		if (NULL == ti)
			break;

		slice = dimension_slice_from_slot(ti->slot);
		ts_chunk_constraint_scan_by_dimension_slice_to_list(slice,
															&chunk_ids,
															CurrentMemoryContext);

		foreach (lc, chunk_ids)
		{
			/* Look for a chunk that a) doesn't have a job stat (reorder ) and b) is not compressed
			 * (should not reorder a compressed chunk) */
			int32 chunk_id = lfirst_int(lc);
			BgwPolicyChunkStats *chunk_stat = ts_bgw_policy_chunk_stats_find(job_id, chunk_id);

			if ((chunk_stat == NULL || chunk_stat->fd.num_times_job_run == 0) &&
				ts_chunk_get_compression_status(chunk_id) == CHUNK_COMPRESS_NONE)
			{
				/* Save the chunk_id */
				result_chunk_id = chunk_id;
				done = true;
				break;
			}
		}
	}

	ts_scan_iterator_close(&it);

	return result_chunk_id;
}

List *
ts_dimension_slice_get_chunkids_to_compress(int32 dimension_id, StrategyNumber start_strategy,
											int64 start_value, StrategyNumber end_strategy,
											int64 end_value, bool compress, bool recompress,
											int32 numchunks)
{
	List *chunk_ids = NIL;
	int32 maxchunks = numchunks > 0 ? numchunks : -1;
	ScanIterator it = ts_dimension_slice_scan_iterator_create(NULL, CurrentMemoryContext);
	bool done = false;

	ts_dimension_slice_scan_iterator_set_range(&it,
											   dimension_id,
											   start_strategy,
											   start_value,
											   end_strategy,
											   end_value);
	ts_scan_iterator_start_scan(&it);

	while (!done)
	{
		DimensionSlice *slice;
		TupleInfo *ti;
		ListCell *lc;
		List *slice_chunk_ids = NIL;

		ti = ts_scan_iterator_next(&it);

		if (NULL == ti)
			break;

		slice = dimension_slice_from_slot(ti->slot);
		ts_chunk_constraint_scan_by_dimension_slice_to_list(slice,
															&slice_chunk_ids,
															CurrentMemoryContext);
		foreach (lc, slice_chunk_ids)
		{
			int32 chunk_id = lfirst_int(lc);
			ChunkCompressionStatus st = ts_chunk_get_compression_status(chunk_id);

			if ((compress && st == CHUNK_COMPRESS_NONE) ||
				(recompress && st == CHUNK_COMPRESS_UNORDERED))
			{
				/* found a chunk that is not compressed or needs recompress
				 * caller needs to check the correct chunk status
				 */
				chunk_ids = lappend_int(chunk_ids, chunk_id);

				if (maxchunks > 0 && list_length(chunk_ids) >= maxchunks)
				{
					done = true;
					break;
				}
			}
		}
	}

	ts_scan_iterator_close(&it);

	return chunk_ids;
}

/* This function checks for overlap between the range we want to update
 for the OSM chunk and the chunks currently in timescaledb (not managed by OSM)
 */
bool
ts_osm_chunk_range_overlaps(int32 osm_dimension_slice_id, int32 dimension_id, int64 range_start,
							int64 range_end)
{
	bool res;
	DimensionVec *vec = dimension_slice_collision_scan(dimension_id, range_start, range_end);
	/* there is only one dimension slice for the OSM chunk. The OSM chunk may not
	 * necessarily appear in the list of overlapping ranges because when first tiered,
	 * it is given a range [max, infinity)
	 */
	if (vec->num_slices >= 2 ||
		(vec->num_slices == 1 && vec->slices[0]->fd.id != osm_dimension_slice_id))
		res = true;
	else
		res = false;
	pfree(vec);
	return res;
}

int
ts_dimension_slice_range_update(DimensionSlice *slice)
{
	FormData_dimension_slice form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_dimension_slice_tuple(slice->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", slice->fd.id);

	if (form.range_start != slice->fd.range_start || form.range_end != slice->fd.range_end)
	{
		form.range_start = slice->fd.range_start;
		form.range_end = slice->fd.range_end;
		dimension_slice_update_catalog_tuple(&tid, &form);
	}
	return true;
}
