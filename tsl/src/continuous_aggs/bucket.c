/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "bucket.h"

static void fill_bucket_offset_origin(const ContinuousAgg *cagg,
									  const InternalTimeRange *const refresh_window,
									  NullableDatum *offset, NullableDatum *origin);
static InternalTimeRange get_largest_bucketed_window(Oid timetype, int64 bucket_width);
static Datum int_bucket_offset_to_datum(Oid type,
										const ContinuousAggsBucketFunction *bucket_function);

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
InternalTimeRange
cagg_compute_inscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
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
cagg_compute_circumscribed_bucketed_refresh_window(
	const ContinuousAgg *cagg, const InternalTimeRange *const refresh_window,
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
