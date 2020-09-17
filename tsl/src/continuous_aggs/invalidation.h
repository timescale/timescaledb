/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H

#include <postgres.h>

#include "continuous_agg.h"
#include "continuous_aggs/materialize.h"

/*
 * Invalidation.
 *
 * A common representation of an invalidation that works across both the
 * hypertable invalidation log and the continuous aggregate invalidation log.
 */
typedef struct Invalidation
{
	int32 hyper_id;
	int64 modification_time;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
	bool frozen;
	bool is_modified;
	ItemPointerData tid;
} Invalidation;

typedef struct InvalidationStore
{
	Tuplestorestate *tupstore;
	TupleDesc tupdesc;
} InvalidationStore;

typedef struct Hypertable Hypertable;

extern void invalidation_cagg_log_add_entry(int32 cagg_hyper_id, int64 modtime, int64 start,
											int64 end, bool frozen);
extern void invalidation_hyper_log_add_entry(int32 hyper_id, int64 modtime, int64 start, int64 end);
extern void invalidation_add_entry(const Hypertable *ht, int64 start, int64 end);
extern void invalidation_entry_set_from_hyper_invalidation(Invalidation *entry, const TupleInfo *ti,
														   int32 hyper_id);
extern void invalidation_process_hypertable_log(const ContinuousAgg *cagg,
												const InternalTimeRange *refresh_window);
extern void invalidation_cagg_log_unfreeze(const ContinuousAgg *cagg,
										   const InternalTimeRange *unfreeze_window);
extern InvalidationStore *invalidation_process_cagg_log(const ContinuousAgg *cagg,
														const InternalTimeRange *refresh_window);
extern void invalidation_store_free(InvalidationStore *store);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H */
