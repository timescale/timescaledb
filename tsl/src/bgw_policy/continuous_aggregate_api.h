/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "bgw_policy/job.h"
#include "dimension.h"
#include <continuous_aggs/materialize.h>
#include <utils/jsonb.h>

typedef enum PolicyRefreshOffsetOverlapResult
{
	POLICY_REFRESH_OFFSET_OVERLAP,		 /* overlap but not exact */
	POLICY_REFRESH_OFFSET_OVERLAP_EQUAL, /* exact match */
	POLICY_REFRESH_OFFSET_OVERLAP_NONE,	 /* no overlap */
} PolicyRefreshOffsetOverlapResult;

extern Datum policy_refresh_cagg_add(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_proc(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_check(PG_FUNCTION_ARGS);
extern Datum policy_refresh_cagg_remove(PG_FUNCTION_ARGS);

int32 policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config);
int64 policy_refresh_cagg_get_refresh_start(const ContinuousAgg *cagg, const Dimension *dim,
											const Jsonb *config, bool *start_isnull);
int64 policy_refresh_cagg_get_refresh_end(const Dimension *dim, const Jsonb *config,
										  bool *end_isnull);
bool policy_refresh_cagg_get_include_tiered_data(const Jsonb *config, bool *isnull);
int32 policy_refresh_cagg_get_buckets_per_batch(const Jsonb *config);
int32 policy_refresh_cagg_get_max_batches_per_execution(const Jsonb *config);
bool policy_refresh_cagg_get_refresh_newest_first(const Jsonb *config);
bool policy_refresh_cagg_exists(int32 materialization_id);

Datum policy_refresh_cagg_add_internal(
	Oid cagg_oid, Oid start_offset_type, NullableDatum start_offset, Oid end_offset_type,
	NullableDatum end_offset, Interval refresh_interval, bool if_not_exists, bool fixed_schedule,
	TimestampTz initial_start, const char *timezone, NullableDatum include_tiered_data,
	NullableDatum buckets_per_batch, NullableDatum max_batches_per_execution,
	NullableDatum refresh_newest_first);
Datum policy_refresh_cagg_remove_internal(Oid cagg_oid, bool if_exists);

PolicyRefreshOffsetOverlapResult policy_refresh_cagg_check_for_overlaps(ContinuousAgg *cagg,
																		Jsonb *policy_config,
																		int32 existing_job_id);

bool policy_refresh_cagg_check_if_last_policy(PolicyContinuousAggData *policy_data);
