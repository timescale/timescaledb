/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef BGW_JOB_STAT_H
#define BGW_JOB_STAT_H

#include "catalog.h"
#include "job.h"

typedef struct BgwJobStat
{
	FormData_bgw_job_stat fd;
} BgwJobStat;

typedef enum JobResult
{
	JOB_FAILURE = 0,
	JOB_SUCCESS = 1,
} JobResult;

extern BgwJobStat *bgw_job_stat_find(int job_id);
extern void bgw_job_stat_mark_start(int32 bgw_job_id);
extern void bgw_job_stat_mark_end(BgwJob *job, JobResult result);
extern bool bgw_job_stat_end_was_marked(BgwJobStat *jobstat);

extern void bgw_job_stat_set_next_start(BgwJob *job, TimestampTz next_start);

extern bool bgw_job_stat_should_execute(BgwJobStat *jobstat, BgwJob *job);

extern TimestampTz bgw_job_stat_next_start(BgwJobStat *jobstat, BgwJob *job);

#endif							/* BGW_JOB_STAT_H */
