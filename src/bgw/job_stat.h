/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "job.h"
#include "ts_catalog/catalog.h"

#define JOB_STAT_FLAGS_DEFAULT 0
#define LAST_CRASH_REPORTED 1

typedef struct BgwJobStat
{
	FormData_bgw_job_stat fd;
} BgwJobStat;

extern TSDLLEXPORT BgwJobStat *ts_bgw_job_stat_find(int job_id);
extern void ts_bgw_job_stat_delete(int job_id);
extern TSDLLEXPORT void ts_bgw_job_stat_mark_start(BgwJob *job);
extern void ts_bgw_job_stat_mark_end(BgwJob *job, JobResult result, Jsonb *edata);
extern bool ts_bgw_job_stat_end_was_marked(BgwJobStat *jobstat);

extern TSDLLEXPORT void ts_bgw_job_stat_set_next_start(int32 job_id, TimestampTz next_start);
extern TSDLLEXPORT bool ts_bgw_job_stat_update_next_start(int32 job_id, TimestampTz next_start,
														  bool allow_unset);

extern TSDLLEXPORT void ts_bgw_job_stat_upsert_next_start(int32 bgw_job_id, TimestampTz next_start);

extern bool ts_bgw_job_stat_should_execute(BgwJobStat *jobstat, BgwJob *job);

extern TimestampTz ts_bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job,
											  int32 consecutive_failed_launches);
extern TSDLLEXPORT void ts_bgw_job_stat_mark_crash_reported(BgwJob *job, JobResult result);

extern TSDLLEXPORT TimestampTz ts_get_next_scheduled_execution_slot(BgwJob *job,
																	TimestampTz finish_time);
