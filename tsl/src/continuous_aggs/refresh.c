/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/fmgrprotos.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/snapmgr.h>

#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
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
#include "guc.h"

#define CAGG_REFRESH_LOG_LEVEL (callctx == CAGG_REFRESH_POLICY ? LOG : DEBUG1)

typedef struct CaggRefreshState
{
	ContinuousAgg cagg;
	Hypertable *cagg_ht;
	InternalTimeRange refresh_window;
	SchemaAndName partial_view;
} CaggRefreshState;

static Hypertable *cagg_get_hypertable_or_fail(int32 hypertable_id);
static InternalTimeRange get_largest_bucketed_window(Oid timetype, int64 bucket_width);
static InternalTimeRange
compute_inscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
										  const InternalTimeRange *const refresh_window,
										  const int64 bucket_width);
static InternalTimeRange
compute_circumscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
											  const InternalTimeRange *const refresh_window,
											  const ContinuousAggsBucketFunction *bucket_function);
static void continuous_agg_refresh_init(CaggRefreshState *refresh, const ContinuousAgg *cagg,
										const InternalTimeRange *refresh_window);
static void continuous_agg_refresh_execute(const CaggRefreshState *refresh,
										   const InternalTimeRange *bucketed_refresh_window,
										   const int32 chunk_id);
static void log_refresh_window(int elevel, const ContinuousAgg *cagg,
							   const InternalTimeRange *refresh_window, const char *msg);
static void continuous_agg_refresh_execute_wrapper(const InternalTimeRange *bucketed_refresh_window,
												   const CaggRefreshCallContext callctx,
												   const long iteration, void *arg1_refresh,
												   void *arg2_chunk_id);
static void update_merged_refresh_window(const InternalTimeRange *bucketed_refresh_window,
										 const CaggRefreshCallContext callctx, const long iteration,
										 void *arg1_merged_refresh_window, void *arg2);
static void continuous_agg_refresh_with_window(const ContinuousAgg *cagg,
											   const InternalTimeRange *refresh_window,
											   const InvalidationStore *invalidations,
											   int32 chunk_id, const bool do_merged_refresh,
											   const InternalTimeRange merged_refresh_window,
											   const CaggRefreshCallContext callctx);
static void emit_up_to_date_notice(const ContinuousAgg *cagg, const CaggRefreshCallContext callctx);
static bool process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
												   const InternalTimeRange *refresh_window,
												   const CaggRefreshCallContext callctx,
												   int32 chunk_id);
static void fill_bucket_offset_origin(const ContinuousAgg *cagg,
									  const InternalTimeRange *const refresh_window,
									  NullableDatum *offset, NullableDatum *origin);

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
	InternalTimeRange maxwindow = {
		.type = timetype,
		.start = ts_time_get_min(timetype),
		.end = ts_time_get_end_or_max(timetype),
	};
	InternalTimeRange maxbuckets = {
		.type = timetype,
	};

	/* For the MIN value, the corresponding bucket either falls on the exact
	 * MIN or it will be below it. Therefore, we add (bucket_width - 1) to
	 * move to the next bucket to be within the allowed range. */
	maxwindow.start = ts_time_saturating_add(maxwindow.start, bucket_width - 1, timetype);
	maxbuckets.start = ts_time_bucket_by_type(bucket_width, maxwindow.start, timetype);
	maxbuckets.end = ts_time_get_end_or_max(timetype);

	return maxbuckets;
}

/*
 * Adjust the refresh window to align with inscribed buckets, so it includes buckets, which are
 * fully covered by the refresh window.
 *
 * Bucketing refresh window is necessary for a continuous aggregate refresh, which can refresh only
 * entire buckets. The result of the function is a bucketed window, where its start is at the start
 * of the first bucket, which is  fully inside the refresh window, and its end is at the end of the
 * last fully covered bucket.
 *
 * Example1, the window needs to shrink:
 *    [---------)      - given refresh window
 * .|....|....|....|.  - buckets
 *       [----)        - inscribed bucketed window
 *
 * Example2, the window is already aligned:
 *       [----)        - given refresh window
 * .|....|....|....|.  - buckets
 *       [----)        - inscribed bucketed window
 *
 * This function is called for the continuous aggregate policy and manual refresh. In such case
 * excluding buckets, which are not fully covered by the refresh window, avoids refreshing a bucket,
 * where part of its data were dropped by a retention policy. See #2198 for details.
 */
static InternalTimeRange
compute_inscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
										  const InternalTimeRange *const refresh_window,
										  const int64 bucket_width)
{
	Assert(cagg != NULL);
	Assert(cagg->bucket_function != NULL);

	NullableDatum NULL_DATUM = INIT_NULL_DATUM;
	InternalTimeRange result = *refresh_window;
	InternalTimeRange largest_bucketed_window =
		get_largest_bucketed_window(refresh_window->type, bucket_width);

	if (refresh_window->start <= largest_bucketed_window.start)
	{
		result.start = largest_bucketed_window.start;
	}
	else
	{
		/* The start time needs to be aligned with the first fully enclosed bucket.
		 * So the original window start is moved to next bucket, except if the start is
		 * already aligned with a bucket, thus 1 is subtracted to avoid moving into next
		 * bucket in the aligned case. */
		int64 included_bucket =
			ts_time_saturating_add(refresh_window->start, bucket_width - 1, refresh_window->type);
		/* Get the start of the included bucket. */
		result.start = ts_time_bucket_by_type_extended(bucket_width,
													   included_bucket,
													   refresh_window->type,
													   NULL_DATUM,
													   NULL_DATUM);
	}

	if (refresh_window->end >= largest_bucketed_window.end)
	{
		result.end = largest_bucketed_window.end;
	}
	else
	{
		/* The window is reduced to the beginning of the bucket, which contains the exclusive
		 * end of the refresh window. */
		result.end = ts_time_bucket_by_type_extended(bucket_width,
													 refresh_window->end,
													 refresh_window->type,
													 NULL_DATUM,
													 NULL_DATUM);
	}
	return result;
}

/*
 * Get the offset as Datum value of an integer based bucket
 */
static Datum
int_bucket_offset_to_datum(Oid type, const ContinuousAggsBucketFunction *bucket_function)
{
	Assert(bucket_function->bucket_time_based == false);

	switch (type)
	{
		case INT2OID:
			return Int16GetDatum(bucket_function->bucket_integer_offset);
		case INT4OID:
			return Int32GetDatum(bucket_function->bucket_integer_offset);
		case INT8OID:
			return Int64GetDatum(bucket_function->bucket_integer_offset);
		default:
			elog(ERROR, "invalid integer time_bucket type \"%s\"", format_type_be(type));
			pg_unreachable();
	}
}

/*
 * Get a NullableDatum for offset and origin based on the CAgg information
 */
static void
fill_bucket_offset_origin(const ContinuousAgg *cagg, const InternalTimeRange *const refresh_window,
						  NullableDatum *offset, NullableDatum *origin)
{
	Assert(cagg != NULL);
	Assert(offset != NULL);
	Assert(origin != NULL);
	Assert(offset->isnull);
	Assert(origin->isnull);

	if (cagg->bucket_function->bucket_time_based)
	{
		if (cagg->bucket_function->bucket_time_offset != NULL)
		{
			offset->isnull = false;
			offset->value = IntervalPGetDatum(cagg->bucket_function->bucket_time_offset);
		}

		if (TIMESTAMP_NOT_FINITE(cagg->bucket_function->bucket_time_origin) == false)
		{
			origin->isnull = false;
			if (refresh_window->type == DATEOID)
			{
				/* Date was converted into a timestamp in process_additional_timebucket_parameter(),
				 * build a Date again */
				origin->value = DirectFunctionCall1(timestamp_date,
													TimestampGetDatum(
														cagg->bucket_function->bucket_time_origin));
			}
			else
			{
				origin->value = TimestampGetDatum(cagg->bucket_function->bucket_time_origin);
			}
		}
	}
	else
	{
		if (cagg->bucket_function->bucket_integer_offset != 0)
		{
			offset->isnull = false;
			offset->value = int_bucket_offset_to_datum(refresh_window->type, cagg->bucket_function);
		}
	}
}

/*
 * Adjust the refresh window to align with circumscribed buckets, so it includes buckets, which
 * fully cover the refresh window.
 *
 * Bucketing refresh window is necessary for a continuous aggregate refresh, which can refresh only
 * entire buckets. The result of the function is a bucketed window, where its start is at the start
 * of a bucket, which contains the start of the refresh window, and its end is at the end of a
 * bucket, which contains the end of the refresh window.
 *
 * Example1, the window needs to expand:
 *    [---------)      - given refresh window
 * .|....|....|....|.  - buckets
 *  [--------------)   - circumscribed bucketed window
 *
 * Example2, the window is already aligned:
 *       [----)        - given refresh window
 * .|....|....|....|.  - buckets
 *       [----)        - inscribed bucketed window
 *
 * This function is called for an invalidation window before refreshing it and after the
 * invalidation window was adjusted to be fully inside a refresh window. In the case of a
 * continuous aggregate policy or manual refresh, the refresh window is the inscribed bucketed
 * window.
 *
 * The circumscribed behaviour is also used for a refresh on drop, when the refresh is called during
 * dropping chunks manually or as part of retention policy.
 */
static InternalTimeRange
compute_circumscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
											  const InternalTimeRange *const refresh_window,
											  const ContinuousAggsBucketFunction *bucket_function)
{
	Assert(cagg != NULL);
	Assert(cagg->bucket_function != NULL);

	if (bucket_function->bucket_fixed_interval == false)
	{
		InternalTimeRange result = *refresh_window;
		ts_compute_circumscribed_bucketed_refresh_window_variable(&result.start,
																  &result.end,
																  bucket_function);
		return result;
	}

	/* Interval is fixed */
	int64 bucket_width = ts_continuous_agg_fixed_bucket_width(bucket_function);
	Assert(bucket_width > 0);

	InternalTimeRange result = *refresh_window;
	InternalTimeRange largest_bucketed_window =
		get_largest_bucketed_window(refresh_window->type, bucket_width);

	/* Get offset and origin for bucket function */
	NullableDatum offset = INIT_NULL_DATUM;
	NullableDatum origin = INIT_NULL_DATUM;
	fill_bucket_offset_origin(cagg, refresh_window, &offset, &origin);

	/* Defined offset and origin in one function is not supported */
	Assert(offset.isnull == true || origin.isnull == true);

	if (refresh_window->start <= largest_bucketed_window.start)
	{
		result.start = largest_bucketed_window.start;
	}
	else
	{
		/* For alignment with a bucket, which includes the start of the refresh window, we just
		 * need to get start of the bucket. */
		result.start = ts_time_bucket_by_type_extended(bucket_width,
													   refresh_window->start,
													   refresh_window->type,
													   offset,
													   origin);
	}

	if (refresh_window->end >= largest_bucketed_window.end)
	{
		result.end = largest_bucketed_window.end;
	}
	else
	{
		int64 exclusive_end;
		int64 bucketed_end;

		Assert(refresh_window->end > result.start);

		/* The end of the window is non-inclusive so subtract one before
		 * bucketing in case we're already at the end of the bucket (we don't
		 * want to add an extra bucket).  */
		exclusive_end = ts_time_saturating_sub(refresh_window->end, 1, refresh_window->type);
		bucketed_end = ts_time_bucket_by_type_extended(bucket_width,
													   exclusive_end,
													   refresh_window->type,
													   offset,
													   origin);

		/* We get the time value for the start of the bucket, so need to add
		 * bucket_width to get the end of it. */
		result.end = ts_time_saturating_add(bucketed_end, bucket_width, refresh_window->type);
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
							   const InternalTimeRange *bucketed_refresh_window,
							   const int32 chunk_id)
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
	const Dimension *time_dim = hyperspace_get_open_dimension(refresh->cagg_ht->space, 0);

	Assert(time_dim != NULL);

	continuous_agg_update_materialization(refresh->cagg_ht,
										  &refresh->cagg,
										  refresh->partial_view,
										  cagg_hypertable_name,
										  &time_dim->fd.column_name,
										  *bucketed_refresh_window,
										  unused_invalidation_range,
										  chunk_id);
}

static void
log_refresh_window(int elevel, const ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
				   const char *msg)
{
	Datum start_ts;
	Datum end_ts;
	Oid outfuncid = InvalidOid;
	bool isvarlena;

	start_ts = ts_internal_to_time_value(refresh_window->start, refresh_window->type);
	end_ts = ts_internal_to_time_value(refresh_window->end, refresh_window->type);
	getTypeOutputInfo(refresh_window->type, &outfuncid, &isvarlena);
	Assert(!isvarlena);

	elog(elevel,
		 "%s \"%s\" in window [ %s, %s ]",
		 msg,
		 NameStr(cagg->data.user_view_name),
		 DatumGetCString(OidFunctionCall1(outfuncid, start_ts)),
		 DatumGetCString(OidFunctionCall1(outfuncid, end_ts)));
}

typedef void (*scan_refresh_ranges_funct_t)(const InternalTimeRange *bucketed_refresh_window,
											const CaggRefreshCallContext callctx,
											const long iteration, /* 0 is first range */
											void *arg1, void *arg2);

static void
continuous_agg_refresh_execute_wrapper(const InternalTimeRange *bucketed_refresh_window,
									   const CaggRefreshCallContext callctx, const long iteration,
									   void *arg1_refresh, void *arg2_chunk_id)
{
	const CaggRefreshState *refresh = (const CaggRefreshState *) arg1_refresh;
	const int32 chunk_id = *(const int32 *) arg2_chunk_id;
	(void) iteration;

	log_refresh_window(CAGG_REFRESH_LOG_LEVEL,
					   &refresh->cagg,
					   bucketed_refresh_window,
					   "continuous aggregate refresh (individual invalidation) on");
	continuous_agg_refresh_execute(refresh, bucketed_refresh_window, chunk_id);
}

static void
update_merged_refresh_window(const InternalTimeRange *bucketed_refresh_window,
							 const CaggRefreshCallContext callctx, const long iteration,
							 void *arg1_merged_refresh_window, void *arg2)
{
	InternalTimeRange *merged_refresh_window = (InternalTimeRange *) arg1_merged_refresh_window;
	(void) arg2;

	if (iteration == 0)
		*merged_refresh_window = *bucketed_refresh_window;
	else
	{
		if (bucketed_refresh_window->start < merged_refresh_window->start)
			merged_refresh_window->start = bucketed_refresh_window->start;

		if (bucketed_refresh_window->end > merged_refresh_window->end)
			merged_refresh_window->end = bucketed_refresh_window->end;
	}
}

static long
continuous_agg_scan_refresh_window_ranges(const ContinuousAgg *cagg,
										  const InternalTimeRange *refresh_window,
										  const InvalidationStore *invalidations,
										  const ContinuousAggsBucketFunction *bucket_function,
										  const CaggRefreshCallContext callctx,
										  scan_refresh_ranges_funct_t exec_func, void *func_arg1,
										  void *func_arg2)
{
	TupleTableSlot *slot;
	long count = 0;

	slot = MakeSingleTupleTableSlot(invalidations->tupdesc, &TTSOpsMinimalTuple);

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
			.end = ts_time_saturating_add(DatumGetInt64(end), 1, refresh_window->type),
		};

		InternalTimeRange bucketed_refresh_window =
			compute_circumscribed_bucketed_refresh_window(cagg, &invalidation, bucket_function);

		(*exec_func)(&bucketed_refresh_window, callctx, count, func_arg1, func_arg2);

		count++;
	}

	ExecDropSingleTupleTableSlot(slot);

	return count;
}

/*
 * Execute refreshes based on the processed invalidations.
 *
 * The given refresh window covers a set of buckets, some of which are
 * out-of-date (invalid) and some which are up-to-date (valid). Invalid
 * buckets that are adjacent form larger ranges, as shown below.
 *
 * Refresh window:  [-----------------------------------------)
 * Invalid ranges:           [-----] [-]   [--] [-] [---]
 * Merged range:             [---------------------------)
 *
 * The maximum number of individual (non-mergeable) ranges are
 * #buckets_in_window/2 (i.e., every other bucket is invalid).
 *
 * Since it might not be efficient to materialize a lot buckets separately
 * when there are many invalid (non-adjecent) buckets/ranges, we put a limit
 * on the number of individual materializations we do. This limit is
 * determined by the MATERIALIZATIONS_PER_REFRESH_WINDOW setting.
 *
 * Thus, if the refresh window covers a large number of buckets, but only a
 * few of them are invalid, it is likely beneficial to materialized these
 * separately to avoid materializing a lot of buckets that are already
 * up-to-date. But if the number of invalid buckets/ranges go above the
 * threshold, we materialize all of them in one go using the "merged range",
 * as illustrated above.
 */
static void
continuous_agg_refresh_with_window(const ContinuousAgg *cagg,
								   const InternalTimeRange *refresh_window,
								   const InvalidationStore *invalidations, int32 chunk_id,
								   const bool do_merged_refresh,
								   const InternalTimeRange merged_refresh_window,
								   const CaggRefreshCallContext callctx)
{
	CaggRefreshState refresh;

	continuous_agg_refresh_init(&refresh, cagg, refresh_window);

	/*
	 * If we're refreshing a finalized CAgg then we should force
	 * the `chunk_id` to be `INVALID_CHUNK_ID` because this column
	 * does not exist anymore in the materialization hypertable.
	 *
	 * The underlying function `spi_update_materialization` that
	 * actually will DELETE and INSERT data into the materialization
	 * hypertable is responsible for check if the `chunk_id` is valid
	 * and then use it or not during the refresh.
	 */
	if (ContinuousAggIsFinalized(cagg))
		chunk_id = INVALID_CHUNK_ID;

	if (do_merged_refresh)
	{
		Assert(merged_refresh_window.type == refresh_window->type);
		Assert(merged_refresh_window.start >= refresh_window->start);
		Assert((cagg->bucket_function->bucket_fixed_interval == false) ||
			   (merged_refresh_window.end -
					ts_continuous_agg_fixed_bucket_width(cagg->bucket_function) <=
				refresh_window->end));

		log_refresh_window(CAGG_REFRESH_LOG_LEVEL,
						   cagg,
						   &merged_refresh_window,
						   "continuous aggregate refresh (merged invalidation) on");
		continuous_agg_refresh_execute(&refresh, &merged_refresh_window, chunk_id);
	}
	else
	{
		long count pg_attribute_unused();
		count = continuous_agg_scan_refresh_window_ranges(cagg,
														  refresh_window,
														  invalidations,
														  cagg->bucket_function,
														  callctx,
														  continuous_agg_refresh_execute_wrapper,
														  (void *) &refresh /* arg1 */,
														  (void *) &chunk_id /* arg2 */);
		Assert(count);
	}
}

#define REFRESH_FUNCTION_NAME "refresh_continuous_aggregate()"
/*
 * Refresh a continuous aggregate across the given window.
 */
Datum
continuous_agg_refresh(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	ContinuousAgg *cagg;
	InternalTimeRange refresh_window = {
		.type = InvalidOid,
	};

	ts_feature_flag_check(FEATURE_CAGG);

	cagg = cagg_get_by_relid_or_fail(cagg_relid);
	refresh_window.type = cagg->partition_type;

	if (!PG_ARGISNULL(1))
		refresh_window.start = ts_time_value_from_arg(PG_GETARG_DATUM(1),
													  get_fn_expr_argtype(fcinfo->flinfo, 1),
													  refresh_window.type,
													  true);
	else
		/* get min time for a cagg depending of the primary partition type */
		refresh_window.start = cagg_get_time_min(cagg);

	if (!PG_ARGISNULL(2))
		refresh_window.end = ts_time_value_from_arg(PG_GETARG_DATUM(2),
													get_fn_expr_argtype(fcinfo->flinfo, 2),
													refresh_window.type,
													true);
	else
		refresh_window.end = ts_time_get_noend_or_max(refresh_window.type);

	continuous_agg_refresh_internal(cagg,
									&refresh_window,
									CAGG_REFRESH_WINDOW,
									PG_ARGISNULL(1),
									PG_ARGISNULL(2));

	PG_RETURN_VOID();
}

static void
emit_up_to_date_notice(const ContinuousAgg *cagg, const CaggRefreshCallContext callctx)
{
	switch (callctx)
	{
		case CAGG_REFRESH_WINDOW:
		case CAGG_REFRESH_CREATION:
			elog(NOTICE,
				 "continuous aggregate \"%s\" is already up-to-date",
				 NameStr(cagg->data.user_view_name));
			break;
		case CAGG_REFRESH_POLICY:
			break;
	}
}

void
continuous_agg_calculate_merged_refresh_window(const ContinuousAgg *cagg,
											   const InternalTimeRange *refresh_window,
											   const InvalidationStore *invalidations,
											   const ContinuousAggsBucketFunction *bucket_function,
											   InternalTimeRange *merged_refresh_window,
											   const CaggRefreshCallContext callctx)
{
	long count pg_attribute_unused();
	count = continuous_agg_scan_refresh_window_ranges(cagg,
													  refresh_window,
													  invalidations,
													  bucket_function,
													  callctx,
													  update_merged_refresh_window,
													  (void *) merged_refresh_window,
													  NULL /* arg2 */);
	Assert(count);
}

static bool
process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
									   const InternalTimeRange *refresh_window,
									   const CaggRefreshCallContext callctx, int32 chunk_id)
{
	InvalidationStore *invalidations;
	Oid hyper_relid = ts_hypertable_id_to_relid(cagg->data.mat_hypertable_id, false);
	bool do_merged_refresh = false;
	InternalTimeRange merged_refresh_window;

	/* Lock the continuous aggregate's materialized hypertable to protect
	 * against concurrent refreshes. Only concurrent reads will be
	 * allowed. This is a heavy lock that serializes all refreshes on the same
	 * continuous aggregate. We might want to consider relaxing this in the
	 * future, e.g., we'd like to at least allow concurrent refreshes on the
	 * same continuous aggregate when they don't have overlapping refresh
	 * windows.
	 */
	LockRelationOid(hyper_relid, ExclusiveLock);
	const CaggsInfo all_caggs_info =
		ts_continuous_agg_get_all_caggs_info(cagg->data.raw_hypertable_id);
	invalidations = invalidation_process_cagg_log(cagg,
												  refresh_window,
												  &all_caggs_info,
												  ts_guc_cagg_max_individual_materializations,
												  &do_merged_refresh,
												  &merged_refresh_window,
												  callctx);

	if (invalidations != NULL || do_merged_refresh)
	{
		if (callctx == CAGG_REFRESH_CREATION)
		{
			Assert(OidIsValid(cagg->relid));
			ereport(NOTICE,
					(errmsg("refreshing continuous aggregate \"%s\"", get_rel_name(cagg->relid)),
					 errhint("Use WITH NO DATA if you do not want to refresh the continuous "
							 "aggregate on creation.")));
		}

		continuous_agg_refresh_with_window(cagg,
										   refresh_window,
										   invalidations,
										   chunk_id,
										   do_merged_refresh,
										   merged_refresh_window,
										   callctx);
		if (invalidations)
			invalidation_store_free(invalidations);
		return true;
	}

	return false;
}

void
continuous_agg_refresh_internal(const ContinuousAgg *cagg,
								const InternalTimeRange *refresh_window_arg,
								const CaggRefreshCallContext callctx, const bool start_isnull,
								const bool end_isnull)
{
	int32 mat_id = cagg->data.mat_hypertable_id;
	InternalTimeRange refresh_window = *refresh_window_arg;
	int64 invalidation_threshold;

	/* Connect to SPI manager due to the underlying SPI calls */
	int rc = SPI_connect_ext(SPI_OPT_NONATOMIC);
	if (rc != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	/* Lock down search_path */
	rc = SPI_exec("SET LOCAL search_path TO pg_catalog, pg_temp", 0);
	if (rc < 0)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), (errmsg("could not set search_path"))));

	/* Like regular materialized views, require owner to refresh. */
	if (!object_ownercheck(RelationRelationId, cagg->relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(cagg->relid)),
					   get_rel_name(cagg->relid));

	PreventCommandIfReadOnly(REFRESH_FUNCTION_NAME);

	/* Prevent running refresh if we're in a transaction block since a refresh
	 * can run two transactions and might take a long time to release locks if
	 * there's a lot to materialize. Strictly, it is optional to prohibit
	 * transaction blocks since there will be only one transaction if the
	 * invalidation threshold needs no update. However, materialization might
	 * still take a long time and it is probably best for consistency to always
	 * prevent transaction blocks.  */
	PreventInTransactionBlock(true, REFRESH_FUNCTION_NAME);

	/* No bucketing when open ended */
	if (!(start_isnull && end_isnull))
	{
		if (cagg->bucket_function->bucket_fixed_interval == false)
		{
			refresh_window = *refresh_window_arg;
			ts_compute_inscribed_bucketed_refresh_window_variable(&refresh_window.start,
																  &refresh_window.end,
																  cagg->bucket_function);
		}
		else
		{
			int64 bucket_width = ts_continuous_agg_fixed_bucket_width(cagg->bucket_function);
			Assert(bucket_width > 0);
			refresh_window =
				compute_inscribed_bucketed_refresh_window(cagg, refresh_window_arg, bucket_width);
		}
	}

	if (refresh_window.start >= refresh_window.end)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("refresh window too small"),
				 errdetail("The refresh window must cover at least one bucket of data."),
				 errhint("Align the refresh window with the bucket"
						 " time zone or use at least two buckets.")));
	/*
	 * Perform the refresh across two transactions.
	 *
	 * The first transaction moves the invalidation threshold (if needed) and
	 * copies over invalidations from the hypertable log to the cagg
	 * invalidation log. Doing the threshold and copying as part of the first
	 * transaction ensures that the threshold and new invalidations will be
	 * visible as soon as possible to concurrent refreshes and that we keep
	 * locks for only a short period.
	 *
	 * The second transaction processes the cagg invalidation log and then
	 * performs the actual refresh (materialization of data). This transaction
	 * serializes around a lock on the materialized hypertable for the
	 * continuous aggregate that gets refreshed.
	 */

	/* Set the new invalidation threshold. Note that this only updates the
	 * threshold if the new value is greater than the old one. Otherwise, the
	 * existing threshold is returned. */
	invalidation_threshold = invalidation_threshold_set_or_get(cagg, &refresh_window);

	/* We must also cap the refresh window at the invalidation threshold. If
	 * we process invalidations after the threshold, the continuous aggregates
	 * won't be refreshed when the threshold is moved forward in the
	 * future. The invalidation threshold should already be aligned on bucket
	 * boundary. */
	if (refresh_window.end > invalidation_threshold)
		refresh_window.end = invalidation_threshold;

	/* Capping the end might have made the window 0, or negative, so nothing to refresh in that
	 * case.
	 *
	 * For variable width buckets we use a refresh_window.start value that is lower than the
	 * -infinity value (ts_time_get_nobegin < ts_time_get_min). Therefore, the first check in the
	 * following if statement is not enough. If the invalidation_threshold returns the min_value for
	 * the data type, we end up with [nobegin, min_value] which is an invalid time interval.
	 * Therefore, we have also to check if the invalidation_threshold is defined. If not, no refresh
	 * is needed.  */
	if ((refresh_window.start >= refresh_window.end) ||
		(IS_TIMESTAMP_TYPE(refresh_window.type) &&
		 invalidation_threshold == ts_time_get_min(refresh_window.type)))
	{
		emit_up_to_date_notice(cagg, callctx);

		rc = SPI_finish();
		if (rc != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));

		return;
	}

	/* Process invalidations in the hypertable invalidation log */
	const CaggsInfo all_caggs_info =
		ts_continuous_agg_get_all_caggs_info(cagg->data.raw_hypertable_id);
	invalidation_process_hypertable_log(cagg, refresh_window.type, &all_caggs_info);

	/* Commit and Start a new transaction */
	SPI_commit_and_chain();

	cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_id, false);

	if (!process_cagg_invalidations_and_refresh(cagg, &refresh_window, callctx, INVALID_CHUNK_ID))
		emit_up_to_date_notice(cagg, callctx);

	rc = SPI_finish();
	if (rc != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
}
