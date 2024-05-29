/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <nodes/memnodes.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>

#include <compat/compat.h>
#include "ts_catalog/catalog.h"
#include <scan_iterator.h>
#include <scanner.h>
#include <time_bucket.h>
#include <time_utils.h>

#include "continuous_aggs/materialize.h"
#include "debug_point.h"
#include "invalidation_threshold.h"
#include "ts_catalog/continuous_agg.h"

/*
 * Invalidation threshold.
 *
 * The invalidation threshold acts as a dampener on a hypertable to make sure
 * that invalidations written during inserts won't cause too much write
 * amplification in "hot" regions---typically the "head" of the table. The
 * presumption is that most inserts happen at recent time intervals, and those
 * intervals will be invalid until writes move out of them. Therefore, it
 * isn't worth writing invalidations in that region since it is presumed
 * out-of-date anyway. Further, although it is possible to refresh a
 * continuous aggregate in those "hot" regions, it will lead to partially
 * filled buckets. Thus, refreshing those intervals is discouraged since the
 * aggregate will be immediately out-of-date until the buckets are filled. The
 * invalidation threshold is, in other words, used as a marker that lags
 * behind the head of the hypertable, where invalidations are written before
 * the threshold but not after it.
 *
 * The invalidation threshold is moved forward (and only forward) by refreshes
 * on continuous aggregates when it covers a window that stretches beyond the
 * current threshold. The invalidation threshold needs to be moved in its own
 * transaction, with exclusive access, before the refresh starts to
 * materialize data. This is to avoid losing any invalidations that occur
 * between the start of the transaction that moves the threshold and its end
 * (when the new threshold becomes visible).
 *
 * ______________________________________________
 * |_______________________________________|_____| recent data
 *                                        ^
 *      invalidations written here        |  no invalidations
 *                                        |
 *                               invalidation threshold
 *
 * Transactions that use an isolation level stronger than READ COMMITTED will
 * not be able to "see" changes to the invalidation threshold that may have
 * been made while they were running. Therefore, they always create records
 * in the hypertable invalidation log. See the cache_inval_entry_write()
 * implementation in tsl/src/continuous_aggs/insert.c
 */

typedef struct InvalidationThresholdData
{
	const ContinuousAgg *cagg;
	const InternalTimeRange *refresh_window;
	int64 computed_invalidation_threshold;
} InvalidationThresholdData;

static ScanTupleResult
invalidation_threshold_scan_update(TupleInfo *ti, void *const data)
{
	DEBUG_WAITPOINT("invalidation_threshold_scan_update_enter");

	InvalidationThresholdData *invthresh = (InvalidationThresholdData *) data;

	/* If the tuple was modified concurrently, retry the operation and use a new snapshot
	 * to see the updated tuple. */
	if (ti->lockresult == TM_Updated)
		return SCAN_RESTART_WITH_NEW_SNAPSHOT;

	if (ti->lockresult != TM_Ok)
	{
		elog(ERROR,
			 "unable to lock invalidation threshold tuple for hypertable %d (lock result %d)",
			 invthresh->cagg->data.raw_hypertable_id,
			 ti->lockresult);

		pg_unreachable();
	}

	bool isnull;
	Datum datum =
		slot_getattr(ti->slot, Anum_continuous_aggs_invalidation_threshold_watermark, &isnull);

	/* NULL should never happen because we always initialize the threshold with the MIN
	 * value of the partition type */
	Ensure(!isnull,
		   "invalidation threshold for hypertable %d is null",
		   invthresh->cagg->data.raw_hypertable_id);

	int64 current_invalidation_threshold = DatumGetInt64(datum);

	/* Compute new invalidation threshold. Note that this computation caps the
	 * threshold at the end of the last bucket that holds data in the
	 * underlying hypertable. */
	invthresh->computed_invalidation_threshold =
		invalidation_threshold_compute(invthresh->cagg, invthresh->refresh_window);

	if (invthresh->computed_invalidation_threshold > current_invalidation_threshold)
	{
		bool nulls[Natts_continuous_agg];
		Datum values[Natts_continuous_agg];
		bool do_replace[Natts_continuous_agg] = { false };
		bool should_free;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;
		TupleDesc tupdesc = ts_scanner_get_tupledesc(ti);

		heap_deform_tuple(tuple, tupdesc, values, nulls);

		do_replace[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			true;
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			Int64GetDatum(invthresh->computed_invalidation_threshold);

		new_tuple = heap_modify_tuple(tuple, tupdesc, values, nulls, do_replace);

		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);
	}
	else
	{
		elog(DEBUG1,
			 "hypertable %d existing watermark >= new invalidation threshold " INT64_FORMAT
			 " " INT64_FORMAT,
			 invthresh->cagg->data.raw_hypertable_id,
			 current_invalidation_threshold,
			 invthresh->computed_invalidation_threshold);
		invthresh->computed_invalidation_threshold = current_invalidation_threshold;
	}

	return SCAN_CONTINUE;
}

/*
 * Set a new invalidation threshold.
 *
 * The threshold is only updated if the new threshold is greater than the old
 * one.
 *
 * On success, the new threshold is returned, otherwise the existing threshold
 * is returned instead.
 */
int64
invalidation_threshold_set_or_get(const ContinuousAgg *cagg,
								  const InternalTimeRange *refresh_window)
{
	bool found = false;
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	InvalidationThresholdData updatectx = {
		.cagg = cagg,
		.refresh_window = refresh_window,
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
		.index =
			catalog_get_index(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD, BGW_JOB_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.data = &updatectx,
		.tuple_found = invalidation_threshold_scan_update,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = CurrentMemoryContext,
		.tuplock = &scantuplock,
		.flags = SCANNER_F_KEEPLOCK,
		/* We update the threshold value using this scanner. Since the scanner uses SnapshotSelf
		 * per default, the updated tuple would become immediately visible to the scanner (the
		 * snapshot includes "changes made by the current command") and ts_scanner_scan_one() would
		 * fail due to the second found tuple. A normal MVCC snapshot is used to prevent the update
		 * is immediately seen by the scanner. */
		.snapshot = GetLatestSnapshot(),
	};

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(cagg->data.raw_hypertable_id));

	found = ts_scanner_scan_one(&scanctx, false, CAGG_INVALIDATION_THRESHOLD_NAME);
	Ensure(found,
		   "invalidation threshold for hypertable %d not found",
		   cagg->data.raw_hypertable_id);

	return updatectx.computed_invalidation_threshold;
}

/*
 * Compute a new invalidation threshold.
 *
 * The new invalidation threshold returned is the end of the given refresh
 * window, unless it ends at "infinity" in which case the threshold is capped
 * at the end of the last bucket materialized.
 */
int64
invalidation_threshold_compute(const ContinuousAgg *cagg, const InternalTimeRange *refresh_window)
{
	bool max_refresh = false;
	Hypertable *ht = ts_hypertable_get_by_id(cagg->data.raw_hypertable_id);

	if (IS_TIMESTAMP_TYPE(refresh_window->type))
		max_refresh = TS_TIME_IS_END(refresh_window->end, refresh_window->type) ||
					  TS_TIME_IS_NOEND(refresh_window->end, refresh_window->type);
	else
		max_refresh = TS_TIME_IS_MAX(refresh_window->end, refresh_window->type);

	if (max_refresh)
	{
		bool isnull;
		int64 maxval = ts_hypertable_get_open_dim_max_value(ht, 0, &isnull);

		if (isnull)
		{
			/* No data in hypertable */
			return cagg_get_time_min(cagg);
		}
		else
		{
			if (cagg->bucket_function->bucket_fixed_interval == false)
			{
				return ts_compute_beginning_of_the_next_bucket_variable(maxval,
																		cagg->bucket_function);
			}

			int64 bucket_width = ts_continuous_agg_fixed_bucket_width(cagg->bucket_function);
			Assert(bucket_width > 0);
			int64 bucket_start = ts_time_bucket_by_type(bucket_width, maxval, refresh_window->type);
			/* Add one bucket to get to the end of the last bucket */
			return ts_time_saturating_add(bucket_start, bucket_width, refresh_window->type);
		}
	}

	return refresh_window->end;
}

/*
 * Initialize the invalidation threshold.
 *
 * The initial value of the invalidation threshold should be the MIN
 * value for the Continuous Aggregate partition type.
 */
void
invalidation_threshold_initialize(const ContinuousAgg *cagg)
{
	bool found = false;
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
		.index =
			catalog_get_index(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD, BGW_JOB_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.lockmode = ShareUpdateExclusiveLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = CurrentMemoryContext,
		.flags = SCANNER_F_KEEPLOCK,
	};

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(cagg->data.raw_hypertable_id));

	found = ts_scanner_scan_one(&scanctx, false, CAGG_INVALIDATION_THRESHOLD_NAME);

	if (!found)
	{
		Relation rel =
			table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					   ShareUpdateExclusiveLock);
		TupleDesc desc = RelationGetDescr(rel);
		Datum values[Natts_continuous_aggs_invalidation_threshold];
		bool nulls[Natts_continuous_aggs_invalidation_threshold] = { false };
		CatalogSecurityContext sec_ctx;
		/* get the MIN value for the partition type */
		int64 min_value = cagg_get_time_min(cagg);

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_hypertable_id)] =
			Int32GetDatum(cagg->data.raw_hypertable_id);

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			Int64GetDatum(min_value);

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_catalog_insert_values(rel, desc, values, nulls);
		ts_catalog_restore_user(&sec_ctx);
		table_close(rel, NoLock);
	}
}
