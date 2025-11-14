/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "continuous_aggs/materialize.h"
#include "ts_catalog/continuous_agg.h"

/*
 * Invalidation.
 *
 * A common representation of an invalidation that works across both the
 * hypertable invalidation log and the continuous aggregate invalidation log.
 */
typedef struct Invalidation
{
	int32 hyper_id;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
	bool is_modified;
	ItemPointerData tid;
} Invalidation;

#define INVAL_NEG_INFINITY PG_INT64_MIN
#define INVAL_POS_INFINITY PG_INT64_MAX

typedef struct InvalidationStore
{
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;
} InvalidationStore;

typedef struct Hypertable Hypertable;

extern void invalidation_cagg_log_add_entry(int32 cagg_hyper_id, int64 start, int64 end);
extern void invalidation_hyper_log_add_entry(int32 hyper_id, int64 start, int64 end);
extern void continuous_agg_invalidate_raw_ht(const Hypertable *raw_ht, int64 start, int64 end);
extern void continuous_agg_invalidate_mat_ht(const Hypertable *raw_ht, const Hypertable *mat_ht,
											 int64 start, int64 end);
extern Datum continuous_agg_process_hypertable_invalidations(PG_FUNCTION_ARGS);
extern void invalidation_process_hypertable_log(int32 hypertable_id, Oid dimtype);

extern InvalidationStore *invalidation_process_cagg_log(const ContinuousAgg *cagg,
														const InternalTimeRange *refresh_window,
														long max_materializations,
														ContinuousAggRefreshContext context,
														bool force);

extern void invalidation_store_free(InvalidationStore *store);
extern void
invalidation_expand_to_bucket_boundaries(Invalidation *inv, Oid time_type_oid,
										 const ContinuousAggBucketFunction *bucket_function);
extern HeapTuple create_invalidation_tup(const TupleDesc tupdesc, int32 cagg_hyper_id, int64 start,
										 int64 end);
