/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/fmgrprotos.h>
#include <utils/snapmgr.h>
#include <utils/guc.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <miscadmin.h>
#include <fmgr.h>

#include <catalog.h>
#include <continuous_agg.h>
#include <dimension.h>
#include <hypertable.h>
#include <hypertable_cache.h>
#include <time_bucket.h>
#include <time_utils.h>
#include <utils.h>

#include "refresh.h"
#include "materialize.h"
#include "invalidation.h"
#include "invalidation_threshold.h"

typedef struct CaggRefreshState
{
	ContinuousAgg cagg;
	Hypertable *cagg_ht;
	InternalTimeRange refresh_window;
	SchemaAndName partial_view;
} CaggRefreshState;

static Hypertable *
cagg_get_hypertable_or_fail(int32 hypertable_id)
{
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid continuous aggregate state"),
				 errdetail("A continuous aggregate references a hypertable that does not exist.")));

	return ht;
}

/*
 * Compute the largest possible bucketed window given the time type and
 * internal restrictions.
 *
 * The largest bucketed window is governed by restrictions set by the type and
 * internal, TimescaleDB-specific legacy details (see get_max_window above for
 * further explanation).
 */
static InternalTimeRange
get_largest_bucketed_window(Oid timetype, int64 bucket_width)
{
	InternalTimeRange maxwindow = continuous_agg_materialize_window_max(timetype);
	InternalTimeRange maxbuckets;

	/* For the MIN value, the corresponding bucket either falls on the exact
	 * MIN or it will be below it. Therefore, we add (bucket_width - 1) to
	 * move to the next bucket to be within the allowed range. */
	maxwindow.start = maxwindow.start + bucket_width - 1;
	maxbuckets.start = ts_time_bucket_by_type(bucket_width, maxwindow.start, timetype);
	maxbuckets.end = ts_time_bucket_by_type(bucket_width, maxwindow.end, timetype);

	return maxbuckets;
}

/*
 * Adjust the refresh window to align with buckets in an inclusive manner.
 *
 * It is OK to refresh more than the given refresh window, but not less. Since
 * we can only refresh along bucket boundaries, we need to adjust the refresh
 * window to be inclusive in both ends to be able to refresh the given
 * region. For example, if the dotted region below is the original window, the
 * adjusted refresh window includes all four buckets shown.
 *
 * | ....|.....|..   |
 */
static InternalTimeRange
compute_bucketed_refresh_window(const InternalTimeRange *refresh_window, int64 bucket_width)
{
	InternalTimeRange result = *refresh_window;
	InternalTimeRange largest_bucketed_window =
		get_largest_bucketed_window(refresh_window->type, bucket_width);

	if (result.start <= largest_bucketed_window.start)
		result.start = largest_bucketed_window.start;
	else
		result.start = ts_time_bucket_by_type(bucket_width, result.start, result.type);

	if (result.end >= largest_bucketed_window.end)
		result.end = largest_bucketed_window.end;
	else
	{
		int64 exclusive_end = result.end;
		int64 bucketed_end;

		/* The end of the window is non-inclusive so subtract one before
		 * bucketing in case we're already at the end of the bucket (we don't
		 * want to add an extra bucket). But we also don't want to subtract if
		 * we are at the start of the bucket (we don't want to remove a
		 * bucket). The last  */
		if (result.end > result.start)
			exclusive_end = int64_saturating_sub(result.end, 1);

		bucketed_end = ts_time_bucket_by_type(bucket_width, exclusive_end, result.type);
		/* We get the time value for the start of the bucket, so need to add
		 * bucket_width to get the end of it */
		result.end = bucketed_end + bucket_width;
	}

	return result;
}

/*
 * Initialize the refresh state for a continuous aggregate.
 *
 * The state holds information for executing a refresh of a continuous aggregate.
 */
static void
continuous_agg_refresh_init(CaggRefreshState *refresh, const ContinuousAgg *cagg,
							const InternalTimeRange *refresh_window)
{
	MemSet(refresh, 0, sizeof(*refresh));
	refresh->cagg = *cagg;
	refresh->cagg_ht = cagg_get_hypertable_or_fail(cagg->data.mat_hypertable_id);
	refresh->refresh_window = *refresh_window;
	refresh->partial_view.schema = &refresh->cagg.data.partial_view_schema;
	refresh->partial_view.name = &refresh->cagg.data.partial_view_name;
}

/*
 * Execute a refresh.
 *
 * The refresh will materialize the area given by the refresh window in the
 * refresh state.
 */
static void
continuous_agg_refresh_execute(const CaggRefreshState *refresh,
							   const InternalTimeRange *bucketed_refresh_window)
{
	SchemaAndName cagg_hypertable_name = {
		.schema = &refresh->cagg_ht->fd.schema_name,
		.name = &refresh->cagg_ht->fd.table_name,
	};
	/* The materialization function takes two ranges, one for new data and one
	 * for invalidated data. A refresh just uses one of them so the other one
	 * has a zero range. */
	InternalTimeRange unused_invalidation_range = {
		.type = refresh->refresh_window.type,
		.start = 0,
		.end = 0,
	};
	Dimension *time_dim = hyperspace_get_open_dimension(refresh->cagg_ht->space, 0);

	Assert(time_dim != NULL);

	continuous_agg_update_materialization(refresh->partial_view,
										  cagg_hypertable_name,
										  &time_dim->fd.column_name,
										  *bucketed_refresh_window,
										  unused_invalidation_range,
										  refresh->cagg.data.bucket_width);
}

static void
continuous_agg_refresh_with_window(ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
								   InvalidationStore *invalidations)
{
	CaggRefreshState refresh;
	TupleTableSlot *slot;

	continuous_agg_refresh_init(&refresh, cagg, refresh_window);
	slot = MakeSingleTupleTableSlotCompat(invalidations->tupdesc, &TTSOpsMinimalTuple);

	while (tuplestore_gettupleslot(invalidations->tupstore,
								   true /* forward */,
								   false /* copy */,
								   slot))
	{
		bool isnull;
		Datum start = slot_getattr(
			slot,
			Anum_continuous_aggs_materialization_invalidation_log_lowest_modified_value,
			&isnull);
		Datum end = slot_getattr(
			slot,
			Anum_continuous_aggs_materialization_invalidation_log_greatest_modified_value,
			&isnull);
		InternalTimeRange invalidation = {
			.type = refresh_window->type,
			.start = DatumGetInt64(start),
			/* Invalidations are inclusive at the end, while refresh windows
			 * aren't, so add one to the end of the invalidated region */
			.end = int64_saturating_add(DatumGetInt64(end), 1),
		};
		InternalTimeRange bucketed_refresh_window =
			compute_bucketed_refresh_window(&invalidation, cagg->data.bucket_width);

		if (client_min_messages <= DEBUG1)
		{
			Datum start_ts = ts_internal_to_time_value(DatumGetInt64(bucketed_refresh_window.start),
													   refresh_window->type);
			Datum end_ts = ts_internal_to_time_value(DatumGetInt64(bucketed_refresh_window.end),
													 refresh_window->type);
			Oid outfuncid = InvalidOid;
			bool isvarlena;

			getTypeOutputInfo(refresh_window->type, &outfuncid, &isvarlena);
			Assert(!isvarlena);

			elog(DEBUG1,
				 "refreshing continuous aggregate \"%s\" in window [ %s, %s ]",
				 NameStr(cagg->data.user_view_name),
				 DatumGetCString(OidFunctionCall1(outfuncid, start_ts)),
				 DatumGetCString(OidFunctionCall1(outfuncid, end_ts)));
		}

		continuous_agg_refresh_execute(&refresh, &bucketed_refresh_window);
	}

	ExecDropSingleTupleTableSlot(slot);
}

#define REFRESH_FUNCTION_NAME "refresh_continuous_aggregate()"

/*
 * Refresh a continuous aggregate across the given window.
 */
Datum
continuous_agg_refresh(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Catalog *catalog = ts_catalog_get();
	ContinuousAgg *cagg;
	Hypertable *cagg_ht;
	Dimension *time_dim;
	InternalTimeRange refresh_window = {
		.type = InvalidOid,
		.start = PG_INT64_MIN,
		.end = PG_INT64_MAX,
	};
	InvalidationStore *invalidations;

	PreventCommandIfReadOnly(REFRESH_FUNCTION_NAME);

	/* Prevent running refresh if we're in a transaction block since a refresh
	 * can run two transactions and might take a long time to release locks if
	 * there's a lot to materialize. Strictly, it is optional to prohibit
	 * transaction blocks since there will be only one transaction if the
	 * invalidation threshold needs no update. However, materialization might
	 * still take a long time and it is probably best for conistency to always
	 * prevent transaction blocks.  */
	PreventInTransactionBlock(true, REFRESH_FUNCTION_NAME);

	if (!OidIsValid(cagg_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid continuous aggregate")));

	cagg = ts_continuous_agg_find_by_relid(cagg_relid);

	if (NULL == cagg)
	{
		const char *relname = get_rel_name(cagg_relid);

		if (relname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 (errmsg("continuous aggregate does not exist"))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 (errmsg("relation \"%s\" is not a continuous aggregate", relname))));
	}

	cagg_ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
	Assert(cagg_ht != NULL);
	time_dim = hyperspace_get_open_dimension(cagg_ht->space, 0);
	Assert(time_dim != NULL);
	refresh_window.type = ts_dimension_get_partition_type(time_dim);

	if (!PG_ARGISNULL(1))
		refresh_window.start = ts_time_value_from_arg(PG_GETARG_DATUM(1),
													  get_fn_expr_argtype(fcinfo->flinfo, 1),
													  refresh_window.type);

	if (!PG_ARGISNULL(2))
		refresh_window.end = ts_time_value_from_arg(PG_GETARG_DATUM(2),
													get_fn_expr_argtype(fcinfo->flinfo, 2),
													refresh_window.type);

	if (refresh_window.start >= refresh_window.end)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid refresh window"),
				 errhint("The start of the window must be before the end.")));

	refresh_window = compute_bucketed_refresh_window(&refresh_window, cagg->data.bucket_width);

	elog(DEBUG1,
		 "computed refresh window at [ " INT64_FORMAT ", " INT64_FORMAT "]",
		 refresh_window.start,
		 refresh_window.end);

	/* Perform the refresh across two transactions.
	 *
	 * The first transaction moves the invalidation threshold (if needed) and
	 * copies over invalidations from the hypertable log to the cagg
	 * invalidation log. Doing the threshold and copying as part of the first
	 * transaction ensures that the threshold and new invalidations will be
	 * visible as soon as possible to concurrent refreshes and that we keep
	 * locks for only a short period. Note that the first transaction
	 * serializes around the threshold table lock, which protects both the
	 * threshold and the invalidation processing against concurrent refreshes.
	 *
	 * The second transaction processes the cagg invalidation log and then
	 * performs the actual refresh (materialization of data). This transaction
	 * serializes around a lock on the materialized hypertable for the
	 * continuous aggregate that gets refreshed.
	 */
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					AccessExclusiveLock);
	continuous_agg_invalidation_threshold_set(cagg->data.raw_hypertable_id, refresh_window.end);
	invalidation_process_hypertable_log(cagg, &refresh_window);

	/* Start a new transaction. Note that this invalidates previous memory
	 * allocations (and locks). */
	PopActiveSnapshot();
	CommitTransactionCommand();
	StartTransactionCommand();
	cagg = ts_continuous_agg_find_by_relid(cagg_relid);
	cagg_ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);

	/* Lock the continuous aggregate's materialized hypertable to protect
	 * against concurrent refreshes. Only concurrent reads will be
	 * allowed. This is a heavy lock that serializes all refreshes on the same
	 * continuous aggregate. We might want to consider relaxing this in the
	 * future, e.g., we'd like to at least allow concurrent refreshes on the
	 * same continuous aggregate when they don't have overlapping refresh
	 * windows.
	 */
	LockRelationOid(cagg_ht->main_table_relid, ExclusiveLock);
	invalidations = invalidation_process_cagg_log(cagg, &refresh_window);

	if (invalidations != NULL)
	{
		continuous_agg_refresh_with_window(cagg, &refresh_window, invalidations);
		invalidation_store_free(invalidations);
	}
	else
		elog(NOTICE,
			 "continuous aggregate \"%s\" is already up-to-date",
			 NameStr(cagg->data.user_view_name));

	PG_RETURN_VOID();
}
