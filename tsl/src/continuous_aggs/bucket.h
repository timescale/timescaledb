/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "common.h"
#include "time_bucket.h"

InternalTimeRange
cagg_compute_inscribed_bucketed_refresh_window(const ContinuousAgg *cagg,
											   const InternalTimeRange *const refresh_window,
											   const int64 bucket_width);
InternalTimeRange cagg_compute_circumscribed_bucketed_refresh_window(
	const ContinuousAgg *cagg, const InternalTimeRange *const refresh_window,
	const ContinuousAggsBucketFunction *bucket_function);
