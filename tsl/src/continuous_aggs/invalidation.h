/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H

#include <postgres.h>

#include "ts_catalog/continuous_agg.h"
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

extern Datum tsl_invalidation_cagg_log_add_entry(PG_FUNCTION_ARGS);
extern Datum tsl_invalidation_hyper_log_add_entry(PG_FUNCTION_ARGS);
void remote_invalidation_log_add_entry(const Hypertable *raw_ht,
									   ContinuousAggHypertableStatus caggstatus, int32 entry_id,
									   int64 start, int64 end);

extern void invalidation_process_hypertable_log(int32 mat_hypertable_id, int32 raw_hypertable_id,
												Oid dimtype, const CaggsInfo *all_caggs_info);
extern void remote_invalidation_process_hypertable_log(int32 mat_hypertable_id,
													   int32 raw_hypertable_id, Oid dimtype,
													   const CaggsInfo *all_caggs);
extern Datum tsl_invalidation_process_hypertable_log(PG_FUNCTION_ARGS);

extern InvalidationStore *invalidation_process_cagg_log(
	int32 mat_hypertable_id, int32 raw_hypertable_id, const InternalTimeRange *refresh_window,
	const CaggsInfo *all_caggs_info, const long max_materializations, bool *do_merged_refresh,
	InternalTimeRange *ret_merged_refresh_window);
extern Datum tsl_invalidation_process_cagg_log(PG_FUNCTION_ARGS);
extern void remote_invalidation_process_cagg_log(int32 mat_hypertable_id, int32 raw_hypertable_id,
												 const InternalTimeRange *refresh_window,
												 const CaggsInfo *all_caggs,
												 bool *do_merged_refresh,
												 InternalTimeRange *ret_merged_refresh_window);

extern void remote_invalidation_log_delete(int32 raw_hypertable_id,
										   ContinuousAggHypertableStatus caggstatus);
extern Datum tsl_drop_dist_ht_invalidation_trigger(PG_FUNCTION_ARGS);
extern void remote_drop_dist_ht_invalidation_trigger(int32 raw_hypertable_id);

extern void invalidation_store_free(InvalidationStore *store);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H */
