/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef BGW_SCHEDULER_H
#define BGW_SCHEDULER_H
#include <postgres.h>
#include <fmgr.h>
#include "compat.h"
#include <postmaster/bgworker.h>

#include "timer.h"

typedef struct ScheduledBgwJob ScheduledBgwJob;

/* callback used in testing */
typedef void (*register_background_worker_callback_type) (BackgroundWorkerHandle *);

/* Exposed for testing */
extern List *ts_update_scheduled_jobs_list(List *cur_jobs_list, MemoryContext mctx);
#ifdef TS_DEBUG
extern void ts_populate_scheduled_job_tuple(ScheduledBgwJob *sjob, Datum *values);
#endif

extern void ts_bgw_scheduler_process(int32 run_for_interval_ms, register_background_worker_callback_type bgw_register);

/* exposed for access by mock */
extern void ts_bgw_scheduler_setup_callbacks(void);

extern void ts_bgw_job_cache_invalidate_callback(void);

#endif							/* BGW_SCHEDULER_H */
