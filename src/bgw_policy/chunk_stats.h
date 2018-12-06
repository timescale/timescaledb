/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_JOB_CHUNK_STATS_H
#define TIMESCALEDB_BGW_JOB_CHUNK_STATS_H

#include "catalog.h"
#include "export.h"

typedef struct BgwPolicyChunkStats
{
	FormData_bgw_policy_chunk_stats fd;
} BgwPolicyChunkStats;

extern TSDLLEXPORT void ts_bgw_policy_chunk_stats_insert(BgwPolicyChunkStats *stat);
extern BgwPolicyChunkStats *ts_bgw_policy_chunk_stats_find(int32 job_id, int32 chunk_id);
extern void ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(int32 job_id);
extern void ts_bgw_policy_chunk_stats_delete_by_chunk_id(int32 chunk_id);
extern TSDLLEXPORT void ts_bgw_policy_chunk_stats_record_job_run(int32 job_id, int32 chunk_id, TimestampTz last_time_job_run);

#endif							/* TIMESCALEDB_BGW_JOB_CHUNK_STATS_H */
