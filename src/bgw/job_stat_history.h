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

extern void ts_bgw_job_stat_history_mark_start(BgwJob *job);
extern void ts_bgw_job_stat_history_mark_end(BgwJob *job, JobResult result, Jsonb *edata);
