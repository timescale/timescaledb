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

#include "bgw_policy/policies_v2.h"
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

static Hypertable *cagg_get_hypertable_or_fail(int32 hypertable_id);
static InternalTimeRange get_largest_bucketed_window(Oid timetype, int64 bucket_width);
static InternalTimeRange
compute_inscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
										  const InternalTimeRange *const refresh_window,
										  const int64 bucket_width);
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
static void emit_up_to_date_notice(const ContinuousAgg *cagg,
								   const ContinuousAggRefreshContext context);
static bool process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
												   const InternalTimeRange *refresh_window,
												   const ContinuousAggRefreshContext context,
												   bool bucketing_refresh_window, bool force);
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

	InternalTimeRange result = *refresh_window;
	InternalTimeRange largest_bucketed_window =
		get_largest_bucketed_window(refresh_window->type, bucket_width);

	/* Get offset and origin for bucket function */
	NullableDatum offset = INIT_NULL_DATUM;
	NullableDatum origin = INIT_NULL_DATUM;
	fill_bucket_offset_origin(cagg->bucket_function, refresh_window->type, &offset, &origin);

	/* Defined offset and origin in one function is not supported */
	Assert(offset.isnull == true || origin.isnull == true);

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
													   offset,
													   origin);
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
													 offset,
													 origin);
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
void
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
compute_circumscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
											  const InternalTimeRange *const refresh_window,
											  const ContinuousAggBucketFunction *bucket_function)
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
	fill_bucket_offset_origin(cagg->bucket_function, refresh_window->type, &offset, &origin);

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
		elog(elevel,
			 "%s \"%s\" in window [ %s, %s ] (batch %d of %d)",
			 msg,
			 NameStr(cagg->data.user_view_name),
			 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
			 ts_internal_to_time_string(refresh_window->end, refresh_window->type),
			 context.processing_batch,
			 context.number_of_batches);
	else
		elog(elevel,
			 "%s \"%s\" in window [ %s, %s ]",
			 msg,
			 NameStr(cagg->data.user_view_name),
			 ts_internal_to_time_string(refresh_window->start, refresh_window->type),
			 ts_internal_to_time_string(refresh_window->end, refresh_window->type));
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
				compute_circumscribed_bucketed_refresh_window(cagg,
															  &invalidation,
															  cagg->bucket_function);
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
 * Refresh a continuous aggregate across the given window.
 */
Datum
continuous_agg_refresh(PG_FUNCTION_ARGS)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool force = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	Jsonb *options = PG_ARGISNULL(4) ? NULL : PG_GETARG_JSONB_P(4);
	bool process_hypertable_invalidations = true;
	ContinuousAgg *cagg;
	InternalTimeRange refresh_window = {
		.type = InvalidOid,
	};

	ts_feature_flag_check(FEATURE_CAGG);

	if (options)
	{
		bool found;
		bool value = ts_jsonb_get_bool_field(options,
											 POL_REFRESH_CONF_KEY_PROCESS_HYPERTABLE_INVALIDATIONS,
											 &found);
		process_hypertable_invalidations = !found || value;
	}

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

	ContinuousAggRefreshContext context = { .callctx = CAGG_REFRESH_WINDOW };
	continuous_agg_refresh_internal(cagg,
									&refresh_window,
									context,
									PG_ARGISNULL(1),
									PG_ARGISNULL(2),
									true,
									force,
									process_hypertable_invalidations,
									false /*extend_last_bucket*/);

	PG_RETURN_VOID();
}

static void
emit_up_to_date_notice(const ContinuousAgg *cagg, const ContinuousAggRefreshContext context)
{
	switch (context.callctx)
	{
		case CAGG_REFRESH_WINDOW:
		case CAGG_REFRESH_CREATION:
			elog(NOTICE,
				 "continuous aggregate \"%s\" is already up-to-date",
				 NameStr(cagg->data.user_view_name));
			break;
		case CAGG_REFRESH_POLICY:
		case CAGG_REFRESH_POLICY_BATCHED:
			break;
	}
}

static bool
process_cagg_invalidations_and_refresh(const ContinuousAgg *cagg,
									   const InternalTimeRange *refresh_window,
									   const ContinuousAggRefreshContext context,
									   bool bucketing_refresh_window, bool force)
{
	Oid hyper_relid = ts_hypertable_id_to_relid(cagg->data.mat_hypertable_id, false);

	/* Lock the continuous aggregate's materialized hypertable to protect against
	 * concurrent invalidation log processing.
	 *
	 * This is supposed to be a short transaction and in the future we can consider
	 * relaxing this lock.
	 */
	LockRelationOid(hyper_relid, ShareUpdateExclusiveLock);
	invalidation_process_cagg_log(cagg, refresh_window);

	DEBUG_WAITPOINT("before_process_cagg_invalidations_for_refresh_lock");
	DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn2");

	SPI_commit_and_chain();

	DEBUG_WAITPOINT("after_process_cagg_invalidations_for_refresh_lock");
	DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn3");

	InvalidationStore *invalidations =
		collect_and_delete_cagg_invalidations_in_window(cagg, refresh_window, force);

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

static void continuous_agg_refresh_spi_setup_and_connect(CaggRefreshSpiContext *cagg_spi_ctx)
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
		elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

	/* Lock down search_path */
	cagg_spi_ctx->save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();
}

void
continuous_agg_refresh_internal(const ContinuousAgg *cagg,
								const InternalTimeRange *refresh_window_arg,
								const ContinuousAggRefreshContext context, const bool start_isnull,
								const bool end_isnull, bool bucketing_refresh_window, bool force,
								bool process_hypertable_invalidations, bool extend_last_bucket)
{
	int32 mat_id = cagg->data.mat_hypertable_id;
	InternalTimeRange refresh_window = *refresh_window_arg;
	int64 invalidation_threshold;
	CaggRefreshSpiContext cagg_spi_ctx = {};
	/* Like regular materialized views, require owner to refresh. */
	if (!object_ownercheck(RelationRelationId, cagg->relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(cagg->relid)),
					   get_rel_name(cagg->relid));

	continuous_agg_refresh_spi_setup_and_connect(&cagg_spi_ctx);
	/* No bucketing when open ended */
	if (bucketing_refresh_window && !(start_isnull && end_isnull))
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

	/* If there is no other policy defined after this, the inscribed bucket calculated above
	 * is correct. However, in the case of concurrent policies, if this isn't the last
	 * policy defined then we should extend the end of the window to include the partial
	 * bucket. This is done to ensure concurrent policies that are 'adjacent' don't skip a
	 * bucket We don't need to do this when the CAgg is created WITH DATA, or manually
	 * refreshed
	 */
	if (extend_last_bucket && !(start_isnull && end_isnull))
	{
		if (cagg->bucket_function->bucket_fixed_interval == false)
		{
			refresh_window.end =
				ts_compute_beginning_of_the_next_bucket_variable(refresh_window.end,
																 cagg->bucket_function);
		}
		else
		{
			int64 bucket_width = ts_continuous_agg_fixed_bucket_width(cagg->bucket_function);
			refresh_window.end =
				ts_time_saturating_add(refresh_window.end, bucket_width, refresh_window.type);
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
		ereport(ERROR,
				(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
				 errmsg("could not refresh continuous aggregate \"%s\" due to a concurrent refresh",
						NameStr(cagg->data.user_view_name)),
				 errdetail("A concurrent refresh on window [%s, %s) is already in progress.",
						   ts_internal_to_time_string(refresh_window.start, refresh_window.type),
						   ts_internal_to_time_string(refresh_window.end, refresh_window.type))));

	DEBUG_WAITPOINT("cagg_refresh_before_first_txn_commit");

	volatile bool refreshed = false;
	PG_TRY();
	{
		/* Commit and Start a new transaction */
		SPI_commit_and_chain();

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
		 * smaller than the 6 hour stored threshold. In that case, invalidation_threshold_set_or_get()
		 * would return the stored threshold.
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
			if (cagg->bucket_function->bucket_fixed_interval)
			{
				NullableDatum offset = INIT_NULL_DATUM;
				NullableDatum origin = INIT_NULL_DATUM;
				fill_bucket_offset_origin(cagg->bucket_function, refresh_window.type, &offset, &origin);
				int64 bucket_width = ts_continuous_agg_fixed_bucket_width(cagg->bucket_function);
				computed_invalidation_threshold_for_cagg =
					ts_time_bucket_by_type_extended(bucket_width,
													invalidation_threshold,
													refresh_window.type,
													offset,
													origin);
			}
			else
			{
				computed_invalidation_threshold_for_cagg =
					ts_compute_start_of_current_bucket_variable(invalidation_threshold,
																cagg->bucket_function);
			}
		}

		if (refresh_window.end > computed_invalidation_threshold_for_cagg)
			refresh_window.end = computed_invalidation_threshold_for_cagg;

		/* Capping the end might have made the window 0, or negative, so nothing to refresh in that
		 * case.
		 *
		 * For variable width buckets we use a refresh_window.start value that is lower than the
		 * -infinity value (ts_time_get_nobegin < ts_time_get_min). Therefore, the first check in the
		 * following if statement is not enough. If the invalidation_threshold returns the min_value for
		 * the data type, we end up with [nobegin, min_value] which is an invalid time interval.
		 * Therefore, we have also to check if the invalidation_threshold is defined. If not, no refresh
		 * is needed.  */
		if (refresh_window.start < refresh_window.end &&
			!(IS_TIMESTAMP_TYPE(refresh_window.type) &&
			 invalidation_threshold == ts_time_get_min(refresh_window.type)))
		{
			if (process_hypertable_invalidations) //fix this : remove this
			{
				invalidation_process_hypertable_log(cagg->data.raw_hypertable_id,
													refresh_window.type);
			}

			DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_txn1");
			/* Commit and Start a new transaction */
			SPI_commit_and_chain();

			/* Debug error injection / waitpoint based on which batch is being processed */
			DEBUG_WAITPOINT(
				psprintf("cagg_policy_batch_%d_after_txn_1_wait", context.processing_batch));
			DEBUG_ERROR_INJECTION(
				psprintf("cagg_policy_batch_%d_after_txn_1", context.processing_batch));

			cagg = ts_continuous_agg_find_by_mat_hypertable_id(mat_id, false);

			refreshed = process_cagg_invalidations_and_refresh(cagg,
															   &refresh_window,
															   context,
															   bucketing_refresh_window,
															   force);

			DEBUG_WAITPOINT("after_process_cagg_materializations");
		}
	}
	PG_CATCH();
	{
		/*
		 * The current transaction (Tx1, Tx2, or Tx3) is in an aborted state,
		 * but the removal of the refresh ranges in jobs_refresh_ranges needs a
		 * live transaction. Roll it back and start a new transaction so we can
		 * perform cleanup.
		 */
		SPI_rollback_and_chain();

		/*
		 * Clean up, including removing the refresh window registration inserted in Txn1
		 */
		cleanup_before_cagg_refresh_exit(cagg, &cagg_spi_ctx);

		/* Commit the cleanup transaction, then re-throw the original error. */
		SPI_commit_and_chain();
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!refreshed)
		emit_up_to_date_notice(cagg, context);

	cleanup_before_cagg_refresh_exit(cagg, &cagg_spi_ctx);

	SPI_commit();
	int rc = SPI_finish();
	if (rc != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
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
		 ts_datum_to_string(Int64GetDatum(ts_time_get_min(refresh_window->type)),
							refresh_window->type));
}

List *
continuous_agg_split_refresh_window(ContinuousAgg *cagg, InternalTimeRange *original_refresh_window,
									int32 buckets_per_batch, bool refresh_newest_first)
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
		DimensionSlice *slice = ts_dimension_slice_nth_earliest_slice(time_dim->fd.id, 1);

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
		refresh_window.start = slice->fd.range_start;
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
		refresh_window.end = slice->fd.range_end;
		refresh_window.end_isnull = false;
	}

	/* Compute the inscribed bucket for the capped refresh window range */
	const int64 bucket_width = ts_continuous_agg_bucket_width(cagg->bucket_function);
	if (cagg->bucket_function->bucket_fixed_interval == false)
	{
		ts_compute_inscribed_bucketed_refresh_window_variable(&refresh_window.start,
															  &refresh_window.end,
															  cagg->bucket_function);
	}
	else
	{
		refresh_window =
			compute_inscribed_bucketed_refresh_window(cagg, &refresh_window, bucket_width);
	}

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

	/*
	 * Produce the batches to be processed
	 *
	 * The refresh window is split into multiple batches of size `batch_size` each. The batches are
	 * produced in reverse order so that the first range produced is the last range to be processed.
	 *
	 * The batches are produced in reverse order because the most recent data should be the first to
	 * be processed and be visible for the users.
	 *
	 * It takes in account the invalidation logs (hypertable and materialization hypertable) to
	 * avoid producing wholes that have no data to be processed.
	 *
	 * The logic is something like the following:
	 * 1. Get dimension slices from the original hypertables
	 * 2. Get either hypertable and materialization hypertable invalidation logs
	 * 3. Produce the batches in reverse order
	 * 4. Check if the produced batch overlaps either with dimension slices #1 and invalidation logs
	 * #2
	 * 5. If the batch overlaps with both then it's a valid batch to be processed
	 * 6. If the batch overlaps with only one of them then it's not a valid batch to be processed
	 * 7. If the batch does not overlap with any of them then it's not a valid batch to be processed
	 */
	const char *query_str_template = " \
		WITH dimension_slices AS ( \
			SELECT \
				range_start AS start, \
				range_end AS end \
			FROM \
				_timescaledb_catalog.dimension_slice \
				JOIN _timescaledb_catalog.dimension ON dimension.id = dimension_slice.dimension_id \
			WHERE \
				hypertable_id = $1 \
				AND dimension_id = $2 \
				AND range_end >= range_start \
			ORDER BY \
				%s \
		), \
		invalidation_logs AS ( \
			SELECT \
				lowest_modified_value, \
				greatest_modified_value \
			FROM \
				_timescaledb_catalog.continuous_aggs_materialization_invalidation_log \
			WHERE \
				materialization_id = $3 \
				AND greatest_modified_value >= lowest_modified_value \
			UNION ALL \
			SELECT \
				pg_catalog.min(lowest_modified_value) AS lowest_modified_value, \
				pg_catalog.max(greatest_modified_value) AS greatest_modified_value \
			FROM \
				_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log \
			WHERE \
				hypertable_id = $1 \
				AND greatest_modified_value >= lowest_modified_value \
		) \
		SELECT \
			refresh_start AS start, \
			LEAST($6::numeric, refresh_start::numeric + $4::numeric)::bigint AS end \
		FROM \
			pg_catalog.generate_series($5, $6, $4) AS refresh_start \
		WHERE \
			EXISTS ( \
			    SELECT FROM dimension_slices \
				WHERE \
					pg_catalog.int8range(refresh_start, LEAST($6::numeric, refresh_start::numeric + $4::numeric)::bigint) \
					OPERATOR(pg_catalog.&&) \
					pg_catalog.int8range(dimension_slices.start, dimension_slices.end) \
			) \
			AND EXISTS ( \
				SELECT FROM \
					invalidation_logs \
				WHERE \
					pg_catalog.int8range(refresh_start, LEAST($6::numeric, refresh_start::numeric + $4::numeric)::bigint) \
					OPERATOR(pg_catalog.&&) \
					pg_catalog.int8range(lowest_modified_value, greatest_modified_value) \
					AND lowest_modified_value IS NOT NULL \
					AND (greatest_modified_value IS NOT NULL AND greatest_modified_value != $7) \
			) \
		ORDER BY \
			refresh_start %s;";

	const char *query_str = psprintf(query_str_template,
									 refresh_newest_first ? "range_end DESC" : "range_start ASC",
									 refresh_newest_first ? "DESC" : "ASC");

	/* List of InternalTimeRange elements to be returned */
	List *refresh_window_list = NIL;

	/* Prepare for SPI call */
	int res;
	Oid types[] = { INT4OID, INT4OID, INT4OID, INT8OID, INT8OID, INT8OID, INT8OID };
	Datum values[] = { Int32GetDatum(ht->fd.id),
					   Int32GetDatum(time_dim->fd.id),
					   Int32GetDatum(cagg->data.mat_hypertable_id),
					   Int64GetDatum(batch_size),
					   Int64GetDatum(refresh_window.start),
					   Int64GetDatum(refresh_window.end),
					   Int64GetDatum(CAGG_INVALIDATION_WRONG_GREATEST_VALUE) };
	char nulls[] = { false, false, false, false, false, false, false };
	MemoryContext oldcontext = CurrentMemoryContext;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	res = SPI_execute_with_args(query_str,
								7,
								types,
								values,
								nulls,
								false /* read_only */,
								0 /* count */);

	if (res < 0)
		elog(ERROR, "%s: could not produce batches for the policy cagg refresh", __func__);

	if (SPI_processed == 1)
	{
		elog(DEBUG1,
			 "only one batch produced for continuous aggregate \"%s.%s\", falling back to single "
			 "batch processing",
			 NameStr(cagg->data.user_view_schema),
			 NameStr(cagg->data.user_view_name));

		/* Restore search_path */
		AtEOXact_GUC(false, save_nestlevel);

		res = SPI_finish();
		if (res != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

		return NIL;
	}

	/* Build the batches list */
	for (uint64 batch = 0; batch < SPI_processed; batch++)
	{
		bool range_start_isnull, range_end_isnull;
		Datum range_start =
			SPI_getbinval(SPI_tuptable->vals[batch], SPI_tuptable->tupdesc, 1, &range_start_isnull);
		Datum range_end =
			SPI_getbinval(SPI_tuptable->vals[batch], SPI_tuptable->tupdesc, 2, &range_end_isnull);

		/* We need to allocate the list in the old memory context because here we're in the SPI
		 * context */
		MemoryContext saved_context = MemoryContextSwitchTo(oldcontext);
		InternalTimeRange *range = palloc0(sizeof(InternalTimeRange));
		range->start = DatumGetInt64(range_start);
		range->start_isnull = range_start_isnull;
		range->end = DatumGetInt64(range_end);
		range->end_isnull = range_end_isnull;
		range->type = original_refresh_window->type;

		/* For variable-length buckets, circumscribe the batch to bucket boundaries.
		 * The batch size calculation uses a 30-day approximation for months, so we need
		 * to expand batches to cover complete buckets.*/
		if (cagg->bucket_function->bucket_fixed_interval == false)
		{
			ts_compute_circumscribed_bucketed_refresh_window_variable(&range->start,
																	  &range->end,
																	  cagg->bucket_function);
		}
		/*
		 * To make sure that the first range (or last range in case of refreshing from oldest to
		 * newest) is aligned with the end of the refresh window we need to set the end to the
		 * maximum value of the time type if the original refresh window end is NULL.
		 */
		if (((batch == 0 && refresh_newest_first) ||
			 (batch == (SPI_processed - 1) && !refresh_newest_first)) &&
			original_refresh_window->end_isnull)
		{
			range->end = ts_time_get_noend_or_max(range->type);
			range->end_isnull = true;
		}

		/*
		 * To make sure that the last range (or first range in case of refreshing from oldest to
		 * newest) is aligned with the start of the refresh window we need to set the start to the
		 * maximum value of the time type if the original refresh window start is NULL.
		 */
		if (((batch == (SPI_processed - 1) && refresh_newest_first) ||
			 (batch == 0 && !refresh_newest_first)) &&
			original_refresh_window->start_isnull)
		{
			range->start = cagg_get_time_min(cagg);
			range->start_isnull = true;
		}

		refresh_window_list = lappend(refresh_window_list, range);
		MemoryContextSwitchTo(saved_context);

		debug_refresh_window(cagg, range, "batch produced");
	}

	/* Restore search_path */
	AtEOXact_GUC(false, save_nestlevel);

	res = SPI_finish();
	if (res != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	if (refresh_window_list == NIL)
	{
		elog(DEBUG1,
			 "no valid batches produced for continuous aggregate \"%s.%s\", falling back to single "
			 "batch processing",
			 NameStr(cagg->data.user_view_schema),
			 NameStr(cagg->data.user_view_name));
	}

	return refresh_window_list;
}
