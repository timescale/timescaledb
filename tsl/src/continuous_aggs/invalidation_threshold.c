/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <access/xact.h>
#include <nodes/memnodes.h>
#include <storage/lockdefs.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>

#include "ts_catalog/catalog.h"
#include <scanner.h>
#include <scan_iterator.h>
#include <compat/compat.h>
#include <time_utils.h>
#include <time_bucket.h>

#include "ts_catalog/continuous_agg.h"
#include "continuous_aggs/materialize.h"
#include "invalidation_threshold.h"

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
	int64 threshold;
	bool was_updated;
} InvalidationThresholdData;

static ScanTupleResult
scan_update_invalidation_threshold(TupleInfo *ti, void *data)
{
	InvalidationThresholdData *invthresh = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	Form_continuous_aggs_invalidation_threshold form =
		(Form_continuous_aggs_invalidation_threshold) GETSTRUCT(tuple);

	if (invthresh->threshold > form->watermark)
	{
		HeapTuple new_tuple = heap_copytuple(tuple);
		form = (Form_continuous_aggs_invalidation_threshold) GETSTRUCT(new_tuple);

		form->watermark = invthresh->threshold;
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);
		invthresh->was_updated = true;
	}
	else
	{
		elog(DEBUG1,
			 "hypertable %d existing watermark >= new invalidation threshold " INT64_FORMAT
			 " " INT64_FORMAT,
			 form->hypertable_id,
			 form->watermark,
			 invthresh->threshold);
		invthresh->threshold = form->watermark;
	}

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
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
invalidation_threshold_set_or_get(int32 raw_hypertable_id, int64 invalidation_threshold)
{
	bool threshold_found;
	InvalidationThresholdData data = {
		.threshold = invalidation_threshold,
		.was_updated = false,
	};
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(raw_hypertable_id));

	/* NOTE: this function deliberately takes an AccessExclusiveLock when updating the invalidation
	 * threshold, instead of the weaker RowExclusiveLock lock normally held for such operations: in
	 * order to ensure we do not lose invalidations from concurrent mutations, we must ensure that
	 * all transactions which read the invalidation threshold have either completed, or not yet read
	 * the value; if we used a RowExclusiveLock we could race such a transaction and update the
	 * threshold between the time it is read but before the other transaction commits. This would
	 * cause us to lose the updates. The AccessExclusiveLock ensures no one else can possibly be
	 * reading the threshold.
	 */
	threshold_found =
		ts_catalog_scan_one(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD /*=table*/,
							CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY /*=indexid*/,
							scankey /*=scankey*/,
							1 /*=num_keys*/,
							scan_update_invalidation_threshold /*=tuple_found*/,
							AccessExclusiveLock /*=lockmode*/,
							CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME /*=table_name*/,
							&data /*=data*/);

	if (!threshold_found)
	{
		Catalog *catalog = ts_catalog_get();
		/* NOTE: this function deliberately takes a stronger lock than RowExclusive, see the comment
		 * above for the rationale
		 */
		Relation rel =
			table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					   AccessExclusiveLock);
		TupleDesc desc = RelationGetDescr(rel);
		Datum values[Natts_continuous_aggs_invalidation_threshold];
		bool nulls[Natts_continuous_aggs_invalidation_threshold] = { false };

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_hypertable_id)] =
			Int32GetDatum(raw_hypertable_id);
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_invalidation_threshold_watermark)] =
			Int64GetDatum(invalidation_threshold);

		ts_catalog_insert_values(rel, desc, values, nulls);
		table_close(rel, NoLock);
	}

	return data.threshold;
}

static ScanTupleResult
invalidation_threshold_tuple_found(TupleInfo *ti, void *data)
{
	int64 *threshold = data;
	bool isnull;
	Datum datum =
		slot_getattr(ti->slot, Anum_continuous_aggs_invalidation_threshold_watermark, &isnull);

	Assert(!isnull);
	*threshold = DatumGetInt64(datum);

	return SCAN_CONTINUE;
}

int64
invalidation_threshold_get(int32 hypertable_id)
{
	int64 threshold = 0;
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	if (!ts_catalog_scan_one(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD /*=table*/,
							 CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY /*=indexid*/,
							 scankey /*=scankey*/,
							 1 /*=num_keys*/,
							 invalidation_threshold_tuple_found /*=tuple_found*/,
							 AccessShareLock /*=lockmode*/,
							 CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_TABLE_NAME /*=table_name*/,
							 &threshold /*=data*/))
		elog(ERROR, "could not find invalidation threshold for hypertable %d", hypertable_id);

	return threshold;
}

static ScanTupleResult
invalidation_threshold_htid_found(TupleInfo *tinfo, void *data)
{
	if (tinfo->lockresult != TM_Ok)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not acquire lock for invalidation threshold row %d",
						tinfo->lockresult),
				 errhint("Retry the operation again.")));
	}
	return SCAN_DONE;
}

/* lock row corresponding to hypertable id in
 * continuous_aggs_invalidation_threshold table in AccessExclusive mode,
 * block till lock is acquired.
 */
void
invalidation_threshold_lock(int32 raw_hypertable_id)
{
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	int retcnt = 0;
	ScannerCtx scanctx;

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(raw_hypertable_id));

	/* lock table in AccessShare mode and the row with AccessExclusive */
	scanctx = (ScannerCtx){ .table = catalog_get_table_id(catalog,
														  CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
							.index = catalog_get_index(catalog,
													   CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
													   CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY),
							.nkeys = 1,
							.scankey = scankey,
							.limit = 1,
							.tuple_found = invalidation_threshold_htid_found,
							.lockmode = AccessShareLock,
							.scandirection = ForwardScanDirection,
							.result_mctx = CurrentMemoryContext,
							.tuplock = &scantuplock };
	retcnt = ts_scanner_scan(&scanctx);
	if (retcnt > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("found multiple invalidation rows for hypertable %d", raw_hypertable_id)));
	}
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
		Datum maxdat = ts_hypertable_get_open_dim_max_value(ht, 0, &isnull);

		if (isnull)
		{
			/* No data in hypertable */
			if (ts_continuous_agg_bucket_width_variable(cagg))
			{
				/*
				 * To determine inscribed/circumscribed refresh window for variable-sized
				 * buckets we should be able to calculate time_bucket(window.begin) and
				 * time_bucket(window.end). This, however, is not possible in general case.
				 * As an example, the minimum date is 4714-11-24 BC, which is before any
				 * reasonable default `origin` value. Thus for variable-sized buckets
				 * instead of minimum date we use -infinity since time_bucket(-infinity)
				 * is well-defined as -infinity.
				 *
				 * For more details see:
				 * - ts_compute_inscribed_bucketed_refresh_window_variable()
				 * - ts_compute_circumscribed_bucketed_refresh_window_variable()
				 */
				return ts_time_get_nobegin(refresh_window->type);
			}
			else
			{
				/* For fixed-sized buckets return min (start of time) */
				return ts_time_get_min(refresh_window->type);
			}
		}
		else
		{
			int64 maxval = ts_time_value_to_internal(maxdat, refresh_window->type);

			if (ts_continuous_agg_bucket_width_variable(cagg))
			{
				return ts_compute_beginning_of_the_next_bucket_variable(maxval,
																		cagg->bucket_function);
			}

			int64 bucket_width = ts_continuous_agg_bucket_width(cagg);
			int64 bucket_start = ts_time_bucket_by_type(bucket_width, maxval, refresh_window->type);
			/* Add one bucket to get to the end of the last bucket */
			return ts_time_saturating_add(bucket_start, bucket_width, refresh_window->type);
		}
	}

	return refresh_window->end;
}
