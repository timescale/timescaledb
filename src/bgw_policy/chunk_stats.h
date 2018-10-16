/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_JOB_CHUNK_STATS_H
#define TIMESCALEDB_BGW_JOB_CHUNK_STATS_H

#include "catalog.h"

typedef struct BgwPolicyChunkStats
{
	FormData_bgw_policy_chunk_stats fd;
} BgwPolicyChunkStats;

/* Exporting because we use in tsl/test/src */
extern TSDLLEXPORT void ts_bgw_policy_chunk_stats_insert(BgwPolicyChunkStats *stat);
void		ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(int32 job_id);
void		ts_bgw_policy_chunk_stats_delete_by_chunk_id(int32 chunk_id);

#endif							/* TIMESCALEDB_BGW_JOB_CHUNK_STATS_H */
