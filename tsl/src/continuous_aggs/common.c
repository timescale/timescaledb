/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/fmgrprotos.h>
#include <fmgr.h>

#include <catalog.h>
#include <continuous_agg.h>
#include <time_utils.h>
#include <time_bucket.h>

#include "common.h"

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
	InternalTimeRange maxbuckets;

	/* For the MIN value, the corresponding bucket either falls on the exact
	 * MIN or it will be below it. Therefore, we add (bucket_width - 1) to
	 * move to the next bucket to be within the allowed range. */
	maxwindow.start = ts_time_saturating_add(maxwindow.start, bucket_width - 1, timetype);
	maxbuckets.start = ts_time_bucket_by_type(bucket_width, maxwindow.start, timetype);
	maxbuckets.end = ts_time_get_end_or_max(timetype);

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
InternalTimeRange
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
			exclusive_end = ts_time_saturating_sub(result.end, 1, result.type);

		bucketed_end = ts_time_bucket_by_type(bucket_width, exclusive_end, result.type);

		/* We get the time value for the start of the bucket, so need to add
		 * bucket_width to get the end of it */
		result.end = ts_time_saturating_add(bucketed_end, bucket_width, refresh_window->type);
	}

	return result;
}

ContinuousAgg *
continuous_agg_func_with_window(const FunctionCallInfo fcinfo, InternalTimeRange *window)
{
	Oid cagg_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	ContinuousAgg *cagg;
	Hypertable *cagg_ht;
	Dimension *time_dim;

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
	window->type = ts_dimension_get_partition_type(time_dim);

	if (!PG_ARGISNULL(1))
		window->start = ts_time_value_from_arg(PG_GETARG_DATUM(1),
											   get_fn_expr_argtype(fcinfo->flinfo, 1),
											   window->type);
	else
		window->start = ts_time_get_min(window->type);

	if (!PG_ARGISNULL(2))
		window->end = ts_time_value_from_arg(PG_GETARG_DATUM(2),
											 get_fn_expr_argtype(fcinfo->flinfo, 2),
											 window->type);
	else
		window->end = ts_time_get_noend_or_max(window->type);

	return cagg;
}
