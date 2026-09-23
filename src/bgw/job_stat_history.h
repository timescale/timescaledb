/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "export.h"
#include "job.h"
#include "job_stat.h"
#include "ts_catalog/catalog.h"

#define INVALID_BGW_JOB_STAT_HISTORY_ID 0

typedef enum BgwJobStatHistoryUpdateType
{
	JOB_STAT_HISTORY_UPDATE_START,
	JOB_STAT_HISTORY_UPDATE_END,
	JOB_STAT_HISTORY_UPDATE_PID,
} BgwJobStatHistoryUpdateType;

extern void ts_bgw_job_stat_history_update(BgwJobStatHistoryUpdateType update_type, BgwJob *job,
										   JobResult result, Jsonb *edata);

/*
 * Attach arbitrary information about what a job execution did. It is stored in
 * the `info` field of the `data` column of the job stat history entry when the
 * job execution is marked as finished. Calling it again for the same execution
 * replaces what was set before.
 *
 * A job execution can span multiple transactions (the continuous aggregate
 * refresh policy, for instance, commits after every batch), so the information
 * is kept in the background worker's per-process memory instead of in a
 * transaction-scoped memory context. It is discarded again when the execution
 * starts and when it ends, so that it cannot leak into the history entry of
 * another execution running in the same process.
 */
extern TSDLLEXPORT void ts_bgw_job_stat_history_set_info(const Jsonb *info);
