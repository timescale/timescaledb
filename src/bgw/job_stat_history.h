/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

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
