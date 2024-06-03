/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "dimension.h"
#include <continuous_aggs/materialize.h>
#include <utils/jsonb.h>

extern Datum policy_refresh_cagg_add(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_proc(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_check(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_remove(PG_FUNCTION_ARGS);

int32 policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config);
int64 policy_refresh_cagg_get_refresh_start(const ContinuousAgg *cagg, const Dimension *dim,
											const Jsonb *config, bool *start_isnull);
int64 policy_refresh_cagg_get_refresh_end(const Dimension *dim, const Jsonb *config,
										  bool *end_isnull);
bool policy_refresh_cagg_refresh_start_lt(int32 materialization_id, Oid cmp_type,
										  Datum cmp_interval);
bool policy_refresh_cagg_exists(int32 materialization_id);

Datum policy_refresh_cagg_add_internal(Oid cagg_oid, Oid start_offset_type,
									   NullableDatum start_offset, Oid end_offset_type,
									   NullableDatum end_offset, Interval refresh_interval,
									   bool if_not_exists, bool fixed_schedule,
									   TimestampTz initial_start, const char *timezone);
Datum policy_refresh_cagg_remove_internal(Oid cagg_oid, bool if_exists);
