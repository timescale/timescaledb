/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "export.h"
#include "ts_catalog/catalog.h"

typedef struct BgwPolicyChunkStats
{
	FormData_bgw_policy_chunk_stats fd;
} BgwPolicyChunkStats;

extern TSDLLEXPORT void ts_bgw_policy_chunk_stats_insert(BgwPolicyChunkStats *chunk_stats);
extern BgwPolicyChunkStats *ts_bgw_policy_chunk_stats_find(int32 job_id, int32 chunk_id);
extern void ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(int32 job_id);
extern void ts_bgw_policy_chunk_stats_delete_by_chunk_id(int32 chunk_id);
extern TSDLLEXPORT void ts_bgw_policy_chunk_stats_record_job_run(int32 job_id, int32 chunk_id,
																 TimestampTz last_time_job_run);
