/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H
#include <postgres.h>

#include <catalog.h>

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;
} ContinuousAgg;

extern ContinuousAgg *ts_continuous_agg_find_by_view_name(const char *schema, const char *name);
extern void ts_continuous_agg_drop_view_callback(ContinuousAgg *ca, const char *schema,
												 const char *name);

extern void ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id);

extern bool ts_continuous_agg_is_user_view(FormData_continuous_agg *data, const char *schema,
										   const char *name);
extern bool ts_continuous_agg_is_partial_view(FormData_continuous_agg *data, const char *schema,
											  const char *name);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H */
