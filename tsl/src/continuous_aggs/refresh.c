/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <executor/spi.h>
#include <executor/tuptable.h>
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
#include <utils/tuplestore.h>

#include "bgw_policy/policies_v2.h"
#include "chunk.h"
#include "debug_point.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "guc.h"
#include "hypertable.h"
#include "invalidation.h"
#include "invalidation_threshold.h"
#include "jsonb_utils.h"
#include "materialize.h"
#include "process_utility.h"
#include "refresh.h"
#include "time_bucket.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_jobs_refresh_ranges.h"

#define CAGG_REFRESH_LOG_LEVEL                                                                     \
	(context.callctx == CAGG_REFRESH_POLICY || context.callctx == CAGG_REFRESH_POLICY_BATCHED ?    \
		 LOG :                                                                                     \
		 DEBUG1)

typedef struct ContinuousAggRefreshState
{
	ContinuousAgg cagg;
	Hypertable *cagg_ht;
	InternalTimeRange refresh_window;
	SchemaAndName partial_view;
	bool bucketing_refresh_window;
} ContinuousAggRefreshState;

typedef struct CaggRefreshSpiContext
{
	const char *old_decompression_limit;
	int save_nestlevel;
} CaggRefreshSpiContext;

static InternalTimeRange get_largest_bucketed_window(Oid timetype, int64 bucket_width);
static InternalTimeRange
compute_inscribed_bucketed_refresh_window(const InternalTimeRange *const refresh_window,
										  const ContinuousAggBucketFunction *bucket_function);
static void continuous_agg_refresh_init(ContinuousAggRefreshState *refresh,
										const ContinuousAgg *cagg,
										const InternalTimeRange *refresh_window,
										bool bucketing_refresh_window);
static void continuous_agg_refresh_execute(const ContinuousAggRefreshState *refresh,
										   const InternalTimeRange *bucketed_refresh_window);
static void log_refresh_window(int elevel, const ContinuousAgg *cagg,
							   const InternalTimeRange *refresh_window,
							   ContinuousAggRefreshContext context);
static void continuous_agg_refresh_execute_wrapper(const InternalTimeRange *bucketed_refresh_window,
												   const ContinuousAggRefreshContext context,
												   const long iteration, void *arg1_refresh);
static void continuous_agg_refresh_with_window(const ContinuousAgg *cagg,
											   const InternalTimeRange *refresh_window,
											   const InvalidationStore *invalidations,
											   const ContinuousAggRefreshContext context,
											   bool bucketing_refresh_window);
static bool process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
												   const InternalTimeRange *refresh_window,
												   const ContinuousAggRefreshContext context,
												   bool bucketing_refresh_window);
Hypertable *
cagg_get_hypertable_or_fail(int32 hypertable_id)
{
	Hypertable *ht = ts_hypertable_get_by_id(hypertable_id);

	if (NULL == ht)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid continuous aggregate state"),
				 errdetail("A continuous aggregate references a hypertable that does not exist.")));
	}

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
compute_inscribed_bucketed_refresh_window(const InternalTimeRange *const refresh_window,
										  const ContinuousAggBucketFunction *bucket_function)
{
	Assert(bucket_function != NULL);

	if (bucket_function->bucket_fixed_interval == false)
	{
		InternalTimeRange result = *refresh_window;
		ts_compute_inscribed_bucketed_refresh_window_variable(&result.start,
															  &result.end,
															  bucket_function);
		return result;
	}

	int64 bucket_width = ts_continuous_agg_fixed_bucket_width(bucket_function);
	Assert(bucket_width > 0);

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
		result.start =
			cagg_fixed_current_bucket_start(included_bucket, refresh_window->type, bucket_function);
	}

	if (refresh_window->end >= largest_bucketed_window.end)
	{
		result.end = largest_bucketed_window.end;
	}
	else
	{
		/* The window is reduced to the beginning of the bucket, which contains the exclusive
		 * end of the refresh window. */
		result.end = cagg_fixed_current_bucket_start(refresh_window->end,
													 refresh_window->type,
													 bucket_function);
	}
	return result;
}

/*
 * Get the offset as Datum value of an integer based bucket
 */
static Datum
int_bucket_offset_to_datum(Oid type, const ContinuousAggBucketFunction *bucket_function)
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
fill_bucket_offset_origin(const ContinuousAggBucketFunction *bucket_function, Oid type,
						  NullableDatum *offset, NullableDatum *origin)
{
	Assert(bucket_function != NULL);
	Assert(offset != NULL);
	Assert(origin != NULL);
	Assert(offset->isnull);
	Assert(origin->isnull);

	if (bucket_function->bucket_time_based)
	{
		if (bucket_function->bucket_time_offset != NULL)
		{
			offset->isnull = false;
			offset->value = IntervalPGetDatum(bucket_function->bucket_time_offset);
		}

		if (TIMESTAMP_NOT_FINITE(bucket_function->bucket_time_origin) == false)
		{
			origin->isnull = false;
			if (type == DATEOID)
			{
				/* Date was converted into a timestamp in process_additional_timebucket_parameter(),
				 * build a Date again */
				origin->value =
					DirectFunctionCall1(timestamp_date,
										TimestampGetDatum(bucket_function->bucket_time_origin));
			}
			else
			{
				origin->value = TimestampGetDatum(bucket_function->bucket_time_origin);
			}
		}
	}
	else
	{
		if (bucket_function->bucket_integer_offset != 0)
		{
			offset->isnull = false;
			offset->value = int_bucket_offset_to_datum(type, bucket_function);
		}
	}
}

/*
 * Compute the start of the bucket containing the given timestamp, accounting
 * for the CAgg's offset and origin.
 *
 * This is a convenience wrapper that combines fill_bucket_offset_origin and
 * ts_time_bucket_by_type_extended for fixed-interval buckets.
 */
int64
cagg_fixed_current_bucket_start(int64 timestamp, Oid type,
								const ContinuousAggBucketFunction *bucket_function)
{
	int64 bucket_width = ts_continuous_agg_fixed_bucket_width(bucket_function);
	Assert(bucket_width > 0);
	NullableDatum offset = INIT_NULL_DATUM;
	NullableDatum origin = INIT_NULL_DATUM;
	fill_bucket_offset_origin(bucket_function, type, &offset, &origin);
	Assert(offset.isnull == true || origin.isnull == true);
	return ts_time_bucket_by_type_extended(bucket_width, timestamp, type, offset, origin);
}

/*
 * Compute the start of the bucket immediately following the bucket that
 * contains the given timestamp.  Equivalently, this is the exclusive end of
 * the bucket containing timestamp.
 *
 * This is a convenience wrapper used when the caller needs to advance past the
 * current bucket (e.g. computing an invalidation threshold or the exclusive
 * upper bound of a circumscribed refresh window).
 */
int64
cagg_fixed_next_bucket_start(int64 timestamp, Oid type,
							 const ContinuousAggBucketFunction *bucket_function)
{
	int64 bucket_width = ts_continuous_agg_fixed_bucket_width(bucket_function);
	Assert(bucket_width > 0);
	int64 bucket_start = cagg_fixed_current_bucket_start(timestamp, type, bucket_function);
	return ts_time_saturating_add(bucket_start, bucket_width, type);
}

/*
 * Compute the start of the bucket containing the given timestamp, dispatching
 * to the fixed-interval or variable-interval implementation as appropriate.
 */
int64
cagg_current_bucket_start(int64 timestamp, Oid type,
						  const ContinuousAggBucketFunction *bucket_function)
{
	if (bucket_function->bucket_fixed_interval)
	{
		return cagg_fixed_current_bucket_start(timestamp, type, bucket_function);
	}
	return ts_cagg_variable_current_bucket_start(timestamp, bucket_function);
}

/*
 * Compute the start of the bucket immediately following the bucket containing
 * the given timestamp, dispatching to the fixed-interval or variable-interval
 * implementation as appropriate.
 */
int64
cagg_next_bucket_start(int64 timestamp, Oid type,
					   const ContinuousAggBucketFunction *bucket_function)
{
	if (bucket_function->bucket_fixed_interval)
	{
		return cagg_fixed_next_bucket_start(timestamp, type, bucket_function);
	}
	return ts_cagg_variable_next_bucket_start(timestamp, bucket_function);
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
InternalTimeRange
compute_circumscribed_bucketed_refresh_window(const InternalTimeRange *const refresh_window,
											  const ContinuousAggBucketFunction *bucket_function)
{
	Assert(bucket_function != NULL);

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

	if (refresh_window->start <= largest_bucketed_window.start)
	{
		result.start = largest_bucketed_window.start;
	}
	else
	{
		/* For alignment with a bucket, which includes the start of the refresh window, we just
		 * need to get start of the bucket. */
		result.start = cagg_fixed_current_bucket_start(refresh_window->start,
													   refresh_window->type,
													   bucket_function);
	}

	if (refresh_window->end >= largest_bucketed_window.end)
	{
		result.end = largest_bucketed_window.end;
	}
	else
	{
		Assert(refresh_window->end > result.start);

		int64 exclusive_end;

		/* The end of the window is non-inclusive so subtract one before
		 * bucketing in case we're already at the end of the bucket (we don't
		 * want to add an extra bucket).  */
		exclusive_end = ts_time_saturating_sub(refresh_window->end, 1, refresh_window->type);
		result.end =
			cagg_fixed_next_bucket_start(exclusive_end, refresh_window->type, bucket_function);
	}
	return result;
}

/*
 * Initialize the refresh state for a continuous aggregate.
 *
 * The state holds information for executing a refresh of a continuous aggregate.
 */
static void
continuous_agg_refresh_init(ContinuousAggRefreshState *refresh, const ContinuousAgg *cagg,
							const InternalTimeRange *refresh_window, bool bucketing_refresh_window)
{
	MemSet(refresh, 0, sizeof(*refresh));
	refresh->cagg = *cagg;
	refresh->cagg_ht = cagg_get_hypertable_or_fail(cagg->data.mat_hypertable_id);
	refresh->refresh_window = *refresh_window;
	refresh->bucketing_refresh_window = bucketing_refresh_window;
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
continuous_agg_refresh_execute(const ContinuousAggRefreshState *refresh,
							   const InternalTimeRange *bucketed_refresh_window)
{
	SchemaAndName cagg_hypertable_name = {
		.schema = &refresh->cagg_ht->fd.schema_name,
		.name = &refresh->cagg_ht->fd.table_name,
	};
	const Dimension *time_dim = hyperspace_get_open_dimension(refresh->cagg_ht->space, 0);

	Assert(time_dim != NULL);

	continuous_agg_update_materialization(refresh->cagg_ht,
										  &refresh->cagg,
										  refresh->partial_view,
										  cagg_hypertable_name,
										  &time_dim->fd.column_name,
										  *bucketed_refresh_window);
}

static void
log_refresh_window(int elevel, const ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
				   ContinuousAggRefreshContext context)
{
	const char *msg = "continuous aggregate refresh (individual invalidation) on";
	if (context.callctx == CAGG_REFRESH_POLICY_BATCHED)
	{
		elog(elevel,
			 "%s \"%s\" in window [ %s, %s ] (batch %d of %d)",
			 msg,
			 NameStr(cagg->data.user_view_name),
			 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
			 ts_internal_to_time_string(refresh_window->end, refresh_window->type),
			 context.processing_batch,
			 context.number_of_batches);
	}
	else
	{
		elog(elevel,
			 "%s \"%s\" in window [ %s, %s ]",
			 msg,
			 NameStr(cagg->data.user_view_name),
			 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
			 ts_internal_to_time_string(refresh_window->end, refresh_window->type));
	}
}

typedef void (*scan_refresh_ranges_funct_t)(const InternalTimeRange *bucketed_refresh_window,
											const ContinuousAggRefreshContext context,
											const long iteration, /* 0 is first range */
											void *arg1);

static void
continuous_agg_refresh_execute_wrapper(const InternalTimeRange *bucketed_refresh_window,
									   const ContinuousAggRefreshContext context,
									   const long iteration, void *arg1_refresh)
{
	const ContinuousAggRefreshState *refresh = (const ContinuousAggRefreshState *) arg1_refresh;
	(void) iteration;

	log_refresh_window(CAGG_REFRESH_LOG_LEVEL, &refresh->cagg, bucketed_refresh_window, context);
	continuous_agg_refresh_execute(refresh, bucketed_refresh_window);
}

static long
continuous_agg_scan_refresh_window_ranges(const ContinuousAgg *cagg,
										  const InternalTimeRange *refresh_window,
										  const InvalidationStore *invalidations,
										  const ContinuousAggRefreshContext context,
										  scan_refresh_ranges_funct_t exec_func, void *func_arg1)
{
	TupleTableSlot *slot;
	long count = 0;
	ContinuousAggRefreshState *refresh = (ContinuousAggRefreshState *) func_arg1;

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

		InternalTimeRange bucketed_refresh_window = {
			.type = invalidation.type,
			.start = invalidation.start,
			.end = invalidation.end,
		};

		if (refresh->bucketing_refresh_window)
		{
			bucketed_refresh_window =
				compute_circumscribed_bucketed_refresh_window(&invalidation, cagg->bucket_function);
		}
		(*exec_func)(&bucketed_refresh_window, context, count, func_arg1);

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
								   const InvalidationStore *invalidations,
								   const ContinuousAggRefreshContext context,
								   bool bucketing_refresh_window)
{
	ContinuousAggRefreshState refresh;

	continuous_agg_refresh_init(&refresh, cagg, refresh_window, bucketing_refresh_window);

	long count pg_attribute_unused();
	count = continuous_agg_scan_refresh_window_ranges(cagg,
													  refresh_window,
													  invalidations,
													  context,
													  continuous_agg_refresh_execute_wrapper,
													  (void *) &refresh /* arg1 */);
	Assert(count);
}

#define REFRESH_FUNCTION_NAME "refresh_continuous_aggregate()"

/*
 * Refresh a continuous aggregate over a window, splitting it into batches when
 * incremental refresh is enabled.
 *
 * This is the shared entry point used by both the manual refresh
 * (refresh_continuous_aggregate) and the continuous aggregate refresh policy.
 *
 * For a normal refresh, batching is driven by the invalidation logs, so it only
 * produces batches for the regions that actually need to be refreshed. A forced
 * refresh ignores the invalidation logs and instead batches every bucket-aligned
 * chunk of the window that contains data, so the whole window is re-materialized.
 * Set buckets_per_batch to 0 for a single atomic pass.
 */
void
continuous_agg_refresh_batched(ContinuousAgg *cagg, InternalTimeRange *refresh_window,
							   ContinuousAggRefreshContext context, bool extend_last_bucket)
{
	List *refresh_window_list = continuous_agg_split_refresh_window(cagg,
																	refresh_window,
																	context.buckets_per_batch,
																	context.force);

	bool batched = (refresh_window_list != NIL);
	if (!batched)
	{
		/* No batching: refresh the whole window as a single batch */
		refresh_window_list = lappend(refresh_window_list, refresh_window);
	}
	else
	{
		/* Batches are already bucket-aligned by the split function */
		switch (context.callctx)
		{
			case CAGG_REFRESH_POLICY:
				context.callctx = CAGG_REFRESH_POLICY_BATCHED;
				break;
			case CAGG_REFRESH_WINDOW:
				context.callctx = CAGG_REFRESH_WINDOW_BATCHED;
				break;
			default:
				break;
		}
	}

	context.number_of_batches = list_length(refresh_window_list);

	/*
	 * The list is always built oldest-first. When refresh_newest_first is true we
	 * iterate from the last element down to the first using index-based access so
	 * that no reversal copy of the list is needed.
	 */
	int32 processing_batch = 0;
	int32 nbatches = context.number_of_batches;
	int32 batch_start = context.refresh_newest_first ? nbatches - 1 : 0;
	int32 batch_end = context.refresh_newest_first ? -1 : nbatches;
	int32 batch_step = context.refresh_newest_first ? -1 : 1;
	bool any_refreshed = false;
	for (int32 batch_idx = batch_start; batch_idx != batch_end; batch_idx += batch_step)
	{
		InternalTimeRange *batch_window =
			(InternalTimeRange *) list_nth(refresh_window_list, batch_idx);

		elog(DEBUG1,
			 "refreshing continuous aggregate \"%s\" from %s to %s",
			 NameStr(cagg->data.user_view_name),
			 ts_internal_to_time_string(batch_window->start, batch_window->type),
			 ts_internal_to_time_string(batch_window->end, batch_window->type));

		context.processing_batch = ++processing_batch;

		/* extend_last_bucket must only apply to the boundary batch -- the one
		 * whose window abuts the adjacent policy.  For newest-first ordering
		 * that is batch 1; for oldest-first it is the final batch.
		 * In non-batched mode (single batch) the one batch is always the boundary. */
		bool apply_extend =
			extend_last_bucket &&
			(context.refresh_newest_first ? processing_batch == 1 :
											processing_batch == context.number_of_batches);

		any_refreshed |= continuous_agg_refresh_internal(cagg,
														 batch_window,
														 context,
														 !batched, /* bucketing_refresh_window */
														 apply_extend);
		DEBUG_ERROR_INJECTION(psprintf("cagg_policy_batch_%d_after_refresh", processing_batch));

		if (context.max_batches_per_execution > 0 &&
			processing_batch >= context.max_batches_per_execution &&
			processing_batch < context.number_of_batches)
		{
			elog(LOG,
				 "reached maximum number of batches per execution (%d), batches not processed (%d)",
				 context.max_batches_per_execution,
				 context.number_of_batches - processing_batch);
			break;
		}
	}

	if (!any_refreshed)
	{
		emit_up_to_date_notice(cagg, context);
	}
}

/*
 * Refresh a continuous aggregate across the given window.
 */
Datum
continuous_agg_refresh(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool force = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	ContinuousAgg *cagg;
	InternalTimeRange refresh_window = {
		.type = InvalidOid,
	};

	ts_feature_flag_check(FEATURE_CAGG);

	cagg = cagg_get_by_relid_or_fail(cagg_relid);
	refresh_window.type = cagg->partition_type;

	/*
	 * Check ownership up front, before any work that touches the source
	 * hypertable pr other cagg related objects (e.g. computing the batches
	 * for an incremental refresh).
	 */
	ts_cagg_permissions_check(cagg_relid, GetUserId());

	if (!PG_ARGISNULL(1))
	{
		refresh_window.start = ts_time_value_from_arg(PG_GETARG_DATUM(1),
													  get_fn_expr_argtype(fcinfo->flinfo, 1),
													  refresh_window.type,
													  true);
	}
	else
	{
		/* get min time for a cagg depending of the primary partition type */
		refresh_window.start = cagg_get_time_min(cagg);
		refresh_window.start_isnull = true;
	}

	if (!PG_ARGISNULL(2))
	{
		refresh_window.end = ts_time_value_from_arg(PG_GETARG_DATUM(2),
													get_fn_expr_argtype(fcinfo->flinfo, 2),
													refresh_window.type,
													true);
	}
	else
	{
		refresh_window.end = ts_time_get_noend_or_max(refresh_window.type);
		refresh_window.end_isnull = true;
	}

	/*
	 * Manual refreshes batch by default (DEFAULT_BUCKETS_PER_BATCH), matching the
	 * continuous aggregate policy. Callers can override through the options JSONB;
	 * set buckets_per_batch to 0 to force a single-pass atomic refresh.
	 */
	int32 buckets_per_batch = DEFAULT_BUCKETS_PER_BATCH;
	int32 max_batches_per_execution = 0;
	bool refresh_newest_first = DEFAULT_REFRESH_NEWEST_FIRST;

	if (!PG_ARGISNULL(4))
	{
		Jsonb *options = PG_GETARG_JSONB_P(4);
		bool found;

		int32 v = ts_jsonb_get_int32_field(options, POL_REFRESH_CONF_KEY_BUCKETS_PER_BATCH, &found);
		if (found)
		{
			buckets_per_batch = v;
		}

		v = ts_jsonb_get_int32_field(options,
									 POL_REFRESH_CONF_KEY_MAX_BATCHES_PER_EXECUTION,
									 &found);
		if (found)
		{
			max_batches_per_execution = v;
		}

		bool b =
			ts_jsonb_get_bool_field(options, POL_REFRESH_CONF_KEY_REFRESH_NEWEST_FIRST, &found);
		if (found)
		{
			refresh_newest_first = b;
		}
	}

	if (buckets_per_batch < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid buckets per batch"),
				 errdetail("buckets_per_batch: %d", buckets_per_batch),
				 errhint("The buckets per batch should be greater than or equal to zero.")));
	}

	if (max_batches_per_execution < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid max batches per execution"),
				 errdetail("max_batches_per_execution: %d", max_batches_per_execution),
				 errhint(
					 "The max batches per execution should be greater than or equal to zero.")));
	}

	ContinuousAggRefreshContext context = {
		.callctx = CAGG_REFRESH_WINDOW,
		.buckets_per_batch = buckets_per_batch,
		.max_batches_per_execution = max_batches_per_execution,
		.refresh_newest_first = refresh_newest_first,
		.force = force,
	};

	continuous_agg_refresh_batched(cagg, &refresh_window, context, false /*extend_last_bucket*/);
	DEBUG_WAITPOINT("after_cagg_refresh_window");

	PG_RETURN_VOID();
}

static bool
process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
									   const InternalTimeRange *refresh_window,
									   const ContinuousAggRefreshContext context,
									   bool bucketing_refresh_window)
{
	/* Lock the continuous aggregate's catalog table entry to protect against concurrent refreshes
	 * on the same cagg processing the cagg invalidation logs for that CAgg.
	 */
	bool found = ts_lock_continuous_agg_tuple(cagg->data.mat_hypertable_id);
	Ensure(found,
		   "continuous aggregate with mat_hypertable_id %d not found",
		   cagg->data.mat_hypertable_id);
	invalidation_process_cagg_log(cagg, refresh_window);

	DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn2");
	DEBUG_WAITPOINT("before_process_cagg_invalidations_for_refresh_lock");

	SPI_commit_and_chain();

	DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn3");
	DEBUG_WAITPOINT("after_process_cagg_invalidations_for_refresh_lock");

	InvalidationStore *invalidations =
		collect_and_delete_cagg_invalidations_in_window(cagg, refresh_window, context.force);

	if (invalidations != NULL)
	{
		if (context.callctx == CAGG_REFRESH_CREATION)
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
										   context,
										   bucketing_refresh_window);
		invalidation_store_free(invalidations);
	}

	return invalidations != NULL;
}

static void
cleanup_before_cagg_refresh_exit(const ContinuousAgg *cagg,
								 const CaggRefreshSpiContext *cagg_spi_ctx)
{
	/* Remove the refresh window registration inserted above so it does
	 * not block future refreshes from the same backend. */
	ts_cagg_jobs_refresh_ranges_delete_by_pid(cagg->data.mat_hypertable_id, MyProcPid);

	SetConfigOption("timescaledb.max_tuples_decompressed_per_dml_transaction",
					cagg_spi_ctx->old_decompression_limit,
					PGC_USERSET,
					PGC_S_SESSION);

	/* Restore search_path */
	AtEOXact_GUC(false, cagg_spi_ctx->save_nestlevel);
}

static void
continuous_agg_refresh_spi_setup_and_connect(CaggRefreshSpiContext *cagg_spi_ctx)
{
	bool nonatomic = ts_process_utility_is_context_nonatomic();

	/* Reset the saved ProcessUtilityContext value promptly before
	 * calling Prevent* checks so the potential unsupported (atomic)
	 * value won't linger there in case of ereport exit.
	 * See: https://github.com/timescale/timescaledb/pull/7566
	 */
	ts_process_utility_context_reset();

	PreventCommandIfReadOnly(REFRESH_FUNCTION_NAME);

	/* Prevent running refresh if we're in a transaction block since a refresh
	 * can run two transactions and might take a long time to release locks if
	 * there's a lot to materialize. Strictly, it is optional to prohibit
	 * transaction blocks since there will be only one transaction if the
	 * invalidation threshold needs no update. However, materialization might
	 * still take a long time and it is probably best for consistency to always
	 * prevent transaction blocks.  */
	PreventInTransactionBlock(nonatomic, REFRESH_FUNCTION_NAME);

	/*
	 * We don't cagg refresh to fail because of decompression limit. So disable
	 * the decompression limit for the duration of the refresh.
	 */
	cagg_spi_ctx->old_decompression_limit =
		GetConfigOption("timescaledb.max_tuples_decompressed_per_dml_transaction", false, false);
	SetConfigOption("timescaledb.max_tuples_decompressed_per_dml_transaction",
					"0",
					PGC_USERSET,
					PGC_S_SESSION);

	/* Connect to SPI manager due to the underlying SPI calls */
	int rc = SPI_connect_ext(SPI_OPT_NONATOMIC);
	if (rc != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));
	}

	/* Lock down search_path */
	cagg_spi_ctx->save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();
}

/* rollback and cleanup after the failed refresh transaction */
static void
rollback_and_error(const ContinuousAgg *cagg, CaggRefreshSpiContext *cagg_spi_ctx, ErrorData *edata)
{
	/*
	 * Every spi_execute pushes a snapshot on the stack (unless a snapshot
	 * is explicitly passed to it). This is usually cleaned up after a
	 * successful execute. However, if this fails, the longjmp skips the
	 * cleanup step.
	 * As a result, when SPI_rollback_and_chain is called after the longjmp, it
	 * can find snapshots that were left behind (resulting in
	 * "portal snapshots did not account for all active snapshots" error).
	 * The rollback cleans up the snapshot stack and then throws an error.
	 * So we use a try-catch block around SPI_rollback_and_chain to ignore this
	 * error.
	 */
	PG_TRY();
	{
		SPI_rollback_and_chain();
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	/* Commit the cleanup transaction, then throw the original error. */
	cleanup_before_cagg_refresh_exit(cagg, cagg_spi_ctx);
	SPI_commit();
	ThrowErrorData(edata);
}

bool
continuous_agg_refresh_internal(const ContinuousAgg *cagg_arg,
								const InternalTimeRange *refresh_window_arg,
								const ContinuousAggRefreshContext context,
								bool bucketing_refresh_window, bool extend_last_bucket)
{
	const ContinuousAgg *volatile cagg = cagg_arg;
	int32 mat_id = cagg->data.mat_hypertable_id;
	InternalTimeRange refresh_window = *refresh_window_arg;
	int64 invalidation_threshold;
	CaggRefreshSpiContext cagg_spi_ctx = {};

	continuous_agg_refresh_spi_setup_and_connect(&cagg_spi_ctx);
	/* No bucketing when open ended */
	if (bucketing_refresh_window &&
		!(refresh_window_arg->start_isnull && refresh_window_arg->end_isnull))
	{
		refresh_window =
			compute_inscribed_bucketed_refresh_window(refresh_window_arg, cagg->bucket_function);
	}

	/* If there is no other policy defined after this, the inscribed bucket calculated above
	 * is correct. However, in the case of concurrent policies, if this isn't the last
	 * policy defined then we should extend the end of the window to include the partial
	 * bucket. This is done to ensure concurrent policies that are 'adjacent' don't skip a
	 * bucket We don't need to do this when the CAgg is created WITH DATA, or manually
	 * refreshed
	 */
	if (extend_last_bucket && !(refresh_window_arg->start_isnull && refresh_window_arg->end_isnull))
	{
		refresh_window.end =
			cagg_next_bucket_start(refresh_window.end, refresh_window.type, cagg->bucket_function);
	}

	if (refresh_window.start >= refresh_window.end)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("refresh window too small"),
				 errdetail("The refresh window must cover at least one bucket of data."),
				 errhint("Align the refresh window with the bucket"
						 " time zone or use at least two buckets.")));
	}

	/*
	 * Perform the refresh across three transactions.
	 *
	 * The first transaction registers the refresh window in the concurrent-
	 * refresh tracking table to detect overlapping concurrent refreshes, then
	 * moves the invalidation threshold (if needed) and copies invalidations
	 * from the hypertable log to the cagg invalidation log.
	 *
	 * The second transaction cuts the cagg invalidation log entries so that
	 * they either fit entirely within the refresh window or are disjoint from
	 * it. It serializes around a lock on the materialized hypertable to prevent
	 * concurrent refreshes of the same cagg from cutting the log simultaneously.
	 *
	 * The third transaction performs the actual materialization, then removes
	 * the processed invalidation log entries and the refresh window registration.
	 * No additional range locking is needed here because the refresh window was
	 * already registered in the tracking table in the first transaction, which
	 * prevents any concurrent refresh with an overlapping window from running.
	 */

	/*
	 * Register this refresh's window in the concurrent-refresh tracking table
	 * (continuous_aggs_jobs_refresh_ranges).  The function acquires
	 * ShareUpdateExclusiveLock on that table, which conflicts with itself,
	 * serializing the overlap check and insert so no two sessions can race past
	 * it.  During the scan it also removes entries whose backend is dead.
	 *
	 * If a live entry with an overlapping window already exists, we raise an
	 * error.
	 */
	if (!ts_cagg_jobs_refresh_ranges_lock_and_register(mat_id,
													   refresh_window.start,
													   refresh_window.end,
													   MyProcPid,
													   context.job_id))
	{
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not refresh continuous aggregate \"%s\" due to a concurrent refresh",
						NameStr(cagg->data.user_view_name)),
				 errdetail("A concurrent refresh on window [%s, %s) is already in progress.",
						   ts_internal_to_time_string(refresh_window.start, refresh_window.type),
						   ts_internal_to_time_string(refresh_window.end, refresh_window.type))));
	}

	DEBUG_WAITPOINT("cagg_refresh_before_first_txn_commit");

	volatile bool refreshed = false;
	volatile ErrorData *edata = NULL;
	PG_TRY();
	{
		/*
		 * Commit the registration row and start a new transaction. Kept inside
		 * PG_TRY so that an error here (e.g. cancel arriving after the commit
		 * is durable but before the chained transaction has started) is routed
		 * through rollback_and_error, which deletes the just-committed
		 * registration row in a fresh transaction.
		 */
		SPI_commit_and_chain();
		DEBUG_ERROR_INJECTION("cagg_refresh_fail_at_txn1_start");

		DEBUG_WAITPOINT("cagg_refresh_after_register");

		/* Set the new invalidation threshold. Note that this only updates the
		 * threshold if the new value is greater than the old one. Otherwise, the
		 * existing threshold is returned. */
		invalidation_threshold = invalidation_threshold_set_or_get(cagg, &refresh_window);

		/* We must also cap the refresh window at the invalidation threshold. If
		 * we process invalidations after the threshold, the continuous aggregates
		 * won't be refreshed when the threshold is moved forward in the
		 * future.
		 *
		 * However, we cannot just set refresh_window.end to the invalidation_threshold,
		 * because the invalidation_threshold returned from invalidation_threshold_set_or_get()
		 * can be one that set by a sibling cagg with a different bucket width and thus
		 * not aligned to this cagg's bucket boundary.
		 * For example, assuming the hypertable has two caggs, one with 4 hour bucket
		 * and the other with 6hour bucket. If the 6 hour cagg had a refresh that advanced
		 * the threshold, the threshold would be at a 6 hour boundary (e.g, 2000-01-01 06:00:00).
		 * If we then refresh the 4 hour cagg on the same range, the threshold calculated by
		 * the 4 hour cagg would be at a 4 hour boundary (e.g., 2000-01-01 04:00:00), which is
		 * smaller than the 6 hour stored threshold. In that case,
		 * invalidation_threshold_set_or_get() would return the stored threshold.
		 *
		 * Therefore, we need to floor the invalidation_threshold to this cagg's own bucket boundary
		 * before using it to cap the cagg's refresh window.
		 *
		 * Skip flooring when the threshold is at type min (no data) or type max
		 * (data inserted near the type's maximum value — we need to cover that
		 * last bucket).
		 */
		int64 computed_invalidation_threshold_for_cagg = invalidation_threshold;
		if (invalidation_threshold > ts_time_get_min(refresh_window.type) &&
			invalidation_threshold < ts_time_get_max(refresh_window.type))
		{
			computed_invalidation_threshold_for_cagg =
				cagg_current_bucket_start(invalidation_threshold,
										  refresh_window.type,
										  cagg->bucket_function);
		}

		if (refresh_window.end > computed_invalidation_threshold_for_cagg)
		{
			refresh_window.end = computed_invalidation_threshold_for_cagg;
		}

		/*
		 * Cap the refresh window start at the earliest chunk when the
		 * hypertable has tiered data but reads are disabled.
		 *
		 * Without this cap the refresh window may begin before the
		 * earliest chunk, causing invalidations in that range to be
		 * processed and removed. By raising the start to the earliest
		 * chunk boundary we preserve those earlier invalidations so
		 * they can be processed later when tiered reads are re-enabled.
		 *
		 * We only apply the cap when tiered data exists but reads are
		 * off, because in that case the refresh cannot see the tiered
		 * data and would still consume the invalidations. When tiered
		 * reads are on (or there is no tiered data), no cap is needed.
		 */
		if (!ts_guc_enable_osm_reads)
		{
			int32 osm_chunk_id = ts_chunk_get_osm_chunk_id(cagg->data.raw_hypertable_id);

			if (osm_chunk_id != INVALID_CHUNK_ID)
			{
				int64 earliest_start =
					invalidation_get_earliest_chunk_start(cagg->data.raw_hypertable_id);

				if (earliest_start > refresh_window.start)
				{
					refresh_window.start = earliest_start;
				}
			}
		}

		/* Capping the end might have made the window 0, or negative, so nothing to refresh in that
		 * case.
		 *
		 * For variable width buckets we use a refresh_window.start value that is lower than the
		 * -infinity value (ts_time_get_nobegin < ts_time_get_min). Therefore, the first check in
		 * the following if statement is not enough. If the invalidation_threshold returns the
		 * min_value for the data type, we end up with [nobegin, min_value] which is an invalid time
		 * interval. Therefore, we have also to check if the invalidation_threshold is defined. If
		 * not, no refresh is needed.  */
		if (refresh_window.start < refresh_window.end &&
			!(IS_TIMESTAMP_TYPE(refresh_window.type) &&
			  invalidation_threshold == ts_time_get_min(refresh_window.type)))
		{
			invalidation_process_hypertable_log(cagg->data.raw_hypertable_id, refresh_window.type);

			DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn1");
			/* Commit and Start a new transaction */
			SPI_commit_and_chain();

			/* Debug error injection / waitpoint based on which batch is being processed */
			DEBUG_ERROR_INJECTION(
				psprintf("cagg_policy_batch_%d_after_txn_1", context.processing_batch));
			DEBUG_WAITPOINT(
				psprintf("cagg_policy_batch_%d_after_txn_1_wait", context.processing_batch));

			cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_id, false);

			refreshed = process_cagg_invalidations_and_refresh(cagg,
															   &refresh_window,
															   context,
															   bucketing_refresh_window);

			DEBUG_WAITPOINT("after_process_cagg_materializations");
		}
	}
	PG_CATCH();
	{
		/*
		 * Save the error and clear the error state so that
		 * We must switch out of ErrorContext first
		 * and use a long-lived context because the current transaction
		 * context is about to be destroyed by the rollback.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();
	if (edata)
	{
		rollback_and_error(cagg, &cagg_spi_ctx, (ErrorData *) edata);
		return false;
	}

	cleanup_before_cagg_refresh_exit(cagg, &cagg_spi_ctx);

	SPI_commit();
	int rc = SPI_finish();
	if (rc != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
	}

	return refreshed;
}

static void
debug_refresh_window(const ContinuousAgg *cagg, const InternalTimeRange *refresh_window,
					 const char *msg)
{
	elog(DEBUG1,
		 "%s \"%s\" in window [ %s, %s ] internal [ " INT64_FORMAT ", " INT64_FORMAT
		 " ] minimum [ %s ]",
		 msg,
		 NameStr(cagg->data.user_view_name),
		 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
		 ts_internal_to_time_string(refresh_window->end, refresh_window->type),
		 refresh_window->start,
		 refresh_window->end,
		 ts_internal_to_time_string(ts_time_get_min(refresh_window->type), refresh_window->type));
}

List *
continuous_agg_split_refresh_window(ContinuousAgg *cagg, InternalTimeRange *original_refresh_window,
									int32 buckets_per_batch, bool force)
{
	/* Do not produce batches when the number of buckets per batch is zero (disabled) */
	if (buckets_per_batch == 0)
	{
		return NIL;
	}

	InternalTimeRange refresh_window = {
		.type = original_refresh_window->type,
		.start = original_refresh_window->start,
		.start_isnull = original_refresh_window->start_isnull,
		.end = original_refresh_window->end,
		.end_isnull = original_refresh_window->end_isnull,
	};

	debug_refresh_window(cagg, &refresh_window, "begin");

	const Hypertable *ht = cagg_get_hypertable_or_fail(cagg->data.raw_hypertable_id);
	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);

	/*
	 * Cap the refresh window to the min and max time of the hypertable
	 *
	 * In order to don't produce unnecessary batches we need to check if the start and end of the
	 * refresh window is NULL then get the min/max slice from the original hypertable
	 *
	 */
	if (refresh_window.start_isnull)
	{
		debug_refresh_window(cagg, &refresh_window, "START IS NULL");
		DimensionSlice *slice;
		// If tiered reads are disabled, we cap the refresh window to the start of the hypertable
		// Get the earliest *non-osm* slice in this scenario.
		if (!ts_guc_enable_osm_reads)
		{
			slice = ts_dimension_slice_earliest_non_osm_slice(time_dim->fd.id);
		}
		else
		{
			slice = ts_dimension_slice_nth_earliest_slice(time_dim->fd.id, 1);
		}

		/* If still there's no MIN slice range start then return no batches */
		if (NULL == slice || TS_TIME_IS_MIN(slice->fd.range_start, refresh_window.type) ||
			TS_TIME_IS_NOBEGIN(slice->fd.range_start, refresh_window.type))
		{
			elog(DEBUG1,
				 "no min slice range start for continuous aggregate \"%s.%s\", falling back to "
				 "single batch processing",
				 NameStr(cagg->data.user_view_schema),
				 NameStr(cagg->data.user_view_name));
			return NIL;
		}
		refresh_window.start = cagg_current_bucket_start(slice->fd.range_start,
														 refresh_window.type,
														 cagg->bucket_function);
		refresh_window.start_isnull = false;
	}

	if (refresh_window.end_isnull)
	{
		debug_refresh_window(cagg, &refresh_window, "END IS NULL");
		DimensionSlice *slice = ts_dimension_slice_nth_latest_slice(time_dim->fd.id, 1);

		/* If still there's no MAX slice range start then return no batches */
		if (NULL == slice || TS_TIME_IS_MAX(slice->fd.range_end, refresh_window.type) ||
			TS_TIME_IS_NOEND(slice->fd.range_end, refresh_window.type))
		{
			elog(DEBUG1,
				 "no min slice range start for continuous aggregate \"%s.%s\", falling back to "
				 "single batch processing",
				 NameStr(cagg->data.user_view_schema),
				 NameStr(cagg->data.user_view_name));
			return NIL;
		}
		/*
		 * Cap the window end at the next bucket boundary after the actual
		 * maximum data value in the hypertable, not the chunk's range_end.
		 *
		 * invalidation_threshold_compute() uses
		 * ts_hypertable_get_open_dim_max_value() for the same reason.  Using
		 * the same value here keeps single-pass and incremental refresh
		 * behaviour consistent.
		 */
		bool maxval_isnull;
		int64 maxval = ts_hypertable_get_open_dim_max_value(ht, 0, &maxval_isnull);

		if (maxval_isnull)
		{
			elog(DEBUG1,
				 "no data in hypertable for continuous aggregate \"%s.%s\", falling back to "
				 "single batch processing",
				 NameStr(cagg->data.user_view_schema),
				 NameStr(cagg->data.user_view_name));
			return NIL;
		}

		refresh_window.end =
			cagg_next_bucket_start(maxval, refresh_window.type, cagg->bucket_function);
		refresh_window.end_isnull = false;
	}

	/* Compute the inscribed bucket for the capped refresh window range */
	const int64 bucket_width = ts_continuous_agg_bucket_width(cagg->bucket_function);
	refresh_window =
		compute_inscribed_bucketed_refresh_window(&refresh_window, cagg->bucket_function);

	/* Check if the refresh size is large enough to produce bathes, if not then return no batches */
	const int64 refresh_window_size = i64abs(refresh_window.end - refresh_window.start);
	const int64 batch_size = (bucket_width * buckets_per_batch);

	if (refresh_window_size <= batch_size)
	{
		Oid type = IS_TIMESTAMP_TYPE(refresh_window.type) ? INTERVALOID : refresh_window.type;
		Datum refresh_size_interval = ts_internal_to_interval_value(refresh_window_size, type);
		Datum batch_size_interval = ts_internal_to_interval_value(batch_size, type);

		elog(DEBUG1,
			 "refresh window size (%s) is smaller than or equal to batch size (%s), falling back "
			 "to single batch processing",
			 ts_datum_to_string(refresh_size_interval, type),
			 ts_datum_to_string(batch_size_interval, type));
		return NIL;
	}

	debug_refresh_window(cagg, &refresh_window, "before produce batches");

	/* $3 = CAGG_INVALIDATION_WRONG_GREATEST_VALUE sentinel, $4/$5 = refresh window bounds */
	static const char *inval_query =
		"SELECT lowest_modified_value, greatest_modified_value FROM ("
		"  SELECT lowest_modified_value, greatest_modified_value"
		"    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log"
		"   WHERE materialization_id = $1"
		"     AND greatest_modified_value >= lowest_modified_value"
		"  UNION ALL"
		"  SELECT pg_catalog.min(lowest_modified_value), pg_catalog.max(greatest_modified_value)"
		"    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log"
		"   WHERE hypertable_id = $2"
		"     AND greatest_modified_value >= lowest_modified_value"
		") t"
		" WHERE lowest_modified_value IS NOT NULL"
		"   AND greatest_modified_value IS NOT NULL"
		"   AND greatest_modified_value != $3"
		"   AND greatest_modified_value >= $4"
		"   AND lowest_modified_value < $5"
		" ORDER BY 1 ASC";

	List *refresh_window_list = NIL;
	MemoryContext oldcontext = CurrentMemoryContext;

	int inval_count;
	TupleDesc inval_tupdesc = NULL;
	int save_nestlevel = 0;
	bool spi_connected = false;

	/*
	 * A forced refresh must re-materialize the whole window regardless of the
	 * invalidation logs. We model that as a single synthetic invalidation
	 * spanning the entire refresh window and skip the invalidation-log query
	 * altogether.
	 */
	if (!force)
	{
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			elog(ERROR, "could not connect to SPI");
		}
		spi_connected = true;

		save_nestlevel = NewGUCNestLevel();
		RestrictSearchPath();

		Oid inval_types[] = { INT4OID, INT4OID, INT8OID, INT8OID, INT8OID };
		Datum inval_values[] = { Int32GetDatum(cagg->data.mat_hypertable_id),
								 Int32GetDatum(ht->fd.id),
								 Int64GetDatum(CAGG_INVALIDATION_WRONG_GREATEST_VALUE),
								 Int64GetDatum(refresh_window.start),
								 Int64GetDatum(refresh_window.end) };
		char inval_nulls[] = { false, false, false, false, false };

		int res = SPI_execute_with_args(inval_query,
										5,
										inval_types,
										inval_values,
										inval_nulls,
										true /* read_only */,
										0);
		if (res < 0)
		{
			elog(ERROR, "%s: could not fetch invalidation ranges for cagg refresh", __func__);
		}

		Assert(SPI_processed <= INT_MAX);
		inval_count = (int) SPI_processed;
		Assert(SPI_tuptable != NULL);
		inval_tupdesc = SPI_tuptable->tupdesc;
	}
	else
	{
		/* Single synthetic invalidation covering the whole refresh window. */
		inval_count = 1;
	}

	/*
	 * Open a streaming catalog scan for dimension slices overlapping the refresh
	 * window.
	 * Filter to slices overlapping [refresh_window.start, refresh_window.end):
	 *   range_start < refresh_window.end   (BTLessStrategyNumber)
	 *   range_end   > refresh_window.start (BTGreaterStrategyNumber; subtract 1 to
	 *                                       offset the +1 adjustment in set_range)
	 */
	ScanIterator dim_it = ts_dimension_slice_scan_iterator_create(NULL, CurrentMemoryContext);
	ts_dimension_slice_scan_iterator_set_range(&dim_it,
											   time_dim->fd.id,
											   BTLessStrategyNumber,
											   refresh_window.end,
											   BTGreaterStrategyNumber,
											   (refresh_window.start > PG_INT64_MIN) ?
												   refresh_window.start - 1 :
												   PG_INT64_MIN);
	ts_scan_iterator_start_or_restart_scan(&dim_it);

	/*
	 * Merge scan: walk oldest-first through the dimension slice cursor and the
	 * SPI invalidations result. Note that the invalidations are ordered by lowest_modified_value
	 * ascending, and dimension slices are ordered by range_start ascending).
	 *
	 * Rather than stepping through every bucket boundary, we skip ahead to the
	 * bucket containing max(dim_low, inval_low) whenever cur is in a gap before
	 * either source's next range.
	 *
	 * A batch is emitted only when it overlaps both an active dimension slice and
	 * an active invalidation range.
	 */
	const ContinuousAggBucketFunction *bf = cagg->bucket_function;
	int64 fixed_batch_size = bf->bucket_fixed_interval ? ts_continuous_agg_fixed_bucket_width(bf) *
															 (int64) buckets_per_batch :
														 0;
	bool dim_has_more;
	int64 dim_low = 0, dim_high = 0;
	{
		TupleInfo *ti = ts_scan_iterator_next(&dim_it);
		dim_has_more = (ti != NULL);
		if (dim_has_more)
		{
			bool isnull;
			dim_low =
				DatumGetInt64(slot_getattr(ti->slot, Anum_dimension_slice_range_start, &isnull));
			dim_high =
				DatumGetInt64(slot_getattr(ti->slot, Anum_dimension_slice_range_end, &isnull));
		}
	}

	int inval_idx = 0;
	int64 inval_low = 0, inval_high = 0;
	if (force)
	{
		/* Synthetic invalidation spanning the whole window. It stays active for
		 * the entire merge loop (inval_high == window end, so it is never
		 * advanced past), making every data-backed chunk eligible for a batch. */
		inval_low = refresh_window.start;
		inval_high = refresh_window.end;
	}
	else if (inval_idx < inval_count)
	{
		bool isnull;
		inval_low =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[inval_idx], inval_tupdesc, 1, &isnull));
		inval_high =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[inval_idx], inval_tupdesc, 2, &isnull));
	}

	int64 cur = refresh_window.start;
	while (cur < refresh_window.end)
	{
		/* Advance past dim slices that end at or before cur */
		while (dim_has_more && dim_high <= cur)
		{
			TupleInfo *ti = ts_scan_iterator_next(&dim_it);
			dim_has_more = (ti != NULL);
			if (dim_has_more)
			{
				bool isnull;
				dim_low = DatumGetInt64(
					slot_getattr(ti->slot, Anum_dimension_slice_range_start, &isnull));
				dim_high =
					DatumGetInt64(slot_getattr(ti->slot, Anum_dimension_slice_range_end, &isnull));
			}
		}

		/* Advance past invalidations that end at or before cur.
		 * This is correct because we are reading invalidation entries already ordered by
		 * lowest_modified_value.
		 */
		while (inval_idx < inval_count && inval_high <= cur)
		{
			inval_idx++;
			if (inval_idx < inval_count)
			{
				bool isnull;
				inval_low = DatumGetInt64(
					SPI_getbinval(SPI_tuptable->vals[inval_idx], inval_tupdesc, 1, &isnull));
				inval_high = DatumGetInt64(
					SPI_getbinval(SPI_tuptable->vals[inval_idx], inval_tupdesc, 2, &isnull));
			}
		}

		if (!dim_has_more || inval_idx >= inval_count)
		{
			break;
		}

		/*
		 * If cur is before the start of either source, jump ahead to the bucket
		 * containing max(dim_low, inval_low), skipping the empty gap entirely.
		 * Guard new_cur > cur to avoid an infinite loop when the jump target falls
		 * inside the current bucket (fall through to the emit check in that case).
		 */
		int64 jump_target = Max(dim_low, inval_low);
		if (jump_target > cur)
		{
			int64 new_cur =
				cagg_current_bucket_start(jump_target, original_refresh_window->type, bf);
			if (new_cur > cur)
			{
				cur = new_cur;
				continue;
			}
		}

		/* Compute the end of this batch */
		int64 next;
		if (bf->bucket_fixed_interval)
		{
			next = cur + fixed_batch_size;
		}
		else
		{
			next = cur;
			for (int32 i = 0; i < buckets_per_batch; i++)
			{
				next = ts_cagg_variable_next_bucket_start(next, bf);
			}
		}
		if (next > refresh_window.end)
		{
			next = refresh_window.end;
		}

		/* Emit the batch */
		MemoryContext spi_context = MemoryContextSwitchTo(oldcontext);
		InternalTimeRange *range = palloc0(sizeof(InternalTimeRange));
		range->start = cur;
		range->end = next;
		range->type = original_refresh_window->type;
		refresh_window_list = lappend(refresh_window_list, range);
		MemoryContextSwitchTo(spi_context);
		debug_refresh_window(cagg, range, "batch produced");

		cur = next;
	}

	ts_scan_iterator_close(&dim_it);

	/* Done with SPI (only used for the invalidation-log query in the non-forced
	 * path; a forced refresh never connects). Restore search_path. */
	if (spi_connected)
	{
		AtEOXact_GUC(false, save_nestlevel);
		int res = SPI_finish();
		if (res != SPI_OK_FINISH)
		{
			elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));
		}
	}

	/*
	 * Apply null restoration to the first and last batch if needed
	 */
	if (list_length(refresh_window_list) >= 2)
	{
		if (original_refresh_window->start_isnull)
		{
			InternalTimeRange *oldest = linitial(refresh_window_list);
			oldest->start = cagg_get_time_min(cagg);
			oldest->start_isnull = true;
		}
		if (original_refresh_window->end_isnull)
		{
			InternalTimeRange *newest = llast(refresh_window_list);
			newest->end = ts_time_get_noend_or_max(original_refresh_window->type);
			newest->end_isnull = true;
		}
	}
	else
	{
		elog(DEBUG1,
			 "only %d batch(es) produced for continuous aggregate \"%s.%s\", "
			 "falling back to single batch processing",
			 list_length(refresh_window_list),
			 NameStr(cagg->data.user_view_schema),
			 NameStr(cagg->data.user_view_name));
		refresh_window_list = NIL;
	}

	return refresh_window_list;
}
