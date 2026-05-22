/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#include "invalidation.h"
#include "materialize.h"
#include "ts_catalog/continuous_agg.h"

/* Default buckets per batch for incremental refresh */
#define DEFAULT_BUCKETS_PER_BATCH 10
/* Default refresh newest first */
#define DEFAULT_REFRESH_NEWEST_FIRST true

Hypertable *cagg_get_hypertable_or_fail(int32 hypertable_id);
extern Datum continuous_agg_refresh(PG_FUNCTION_ARGS);
extern void continuous_agg_refresh_internal(const ContinuousAgg *cagg,
											const InternalTimeRange *refresh_window,
											ContinuousAggRefreshContext context,
											const bool start_isnull, const bool end_isnull,
											bool force, bool extend_last_bucket);
extern List *continuous_agg_split_refresh_window(const ContinuousAgg *cagg,
												 const InternalTimeRange *original_refresh_window,
												 int32 buckets_per_batch,
												 bool refresh_newest_first, bool force);
InternalTimeRange
compute_circumscribed_bucketed_refresh_window(const InternalTimeRange *const refresh_window,
											  const ContinuousAggBucketFunction *bucket_function);

extern int64 cagg_fixed_current_bucket_start(int64 timestamp, Oid type,
											 const ContinuousAggBucketFunction *bucket_function);
extern int64 cagg_fixed_next_bucket_start(int64 timestamp, Oid type,
										  const ContinuousAggBucketFunction *bucket_function);
extern int64 cagg_current_bucket_start(int64 timestamp, Oid type,
									   const ContinuousAggBucketFunction *bucket_function);
extern int64 cagg_next_bucket_start(int64 timestamp, Oid type,
									const ContinuousAggBucketFunction *bucket_function);
