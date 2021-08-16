/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef BGW_SCHEDULER_H
#define BGW_SCHEDULER_H
#include <postgres.h>
#include <fmgr.h>
#include "compat/compat.h"
#include <postmaster/bgworker.h>

#include "timer.h"

typedef struct ScheduledBgwJob ScheduledBgwJob;

/* callback used in testing */
typedef void (*register_background_worker_callback_type)(BackgroundWorkerHandle *);

/* Exposed for testing */
extern List *ts_update_scheduled_jobs_list(List *cur_jobs_list, MemoryContext mctx);
#ifdef TS_DEBUG
extern void ts_populate_scheduled_job_tuple(ScheduledBgwJob *sjob, Datum *values);
#endif

extern void ts_bgw_scheduler_process(int32 run_for_interval_ms,
									 register_background_worker_callback_type bgw_register);

/* exposed for access by mock */
extern void ts_bgw_scheduler_setup_callbacks(void);

extern void ts_bgw_job_cache_invalidate_callback(void);
extern void ts_bgw_scheduler_register_signal_handlers(void);
extern void ts_bgw_scheduler_setup_mctx(void);

extern BackgroundWorkerHandle *ts_bgw_start_worker(const char *function, const char *name,
												   const char *extra);

#endif /* BGW_SCHEDULER_H */
