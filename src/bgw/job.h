/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef BGW_JOB_H
#define BGW_JOB_H

#include "catalog.h"
#include <postmaster/bgworker.h>

typedef enum JobType
{
	JOB_TYPE_VERSION_CHECK = 0,
	JOB_TYPE_UNKNOWN,
	_MAX_JOB_TYPE
} JobType;

typedef struct BgwJob
{
	FormData_bgw_job fd;
	JobType		bgw_type;

} BgwJob;

typedef bool job_main_func (void);
typedef bool (*unknown_job_type_hook_type) (BgwJob *job);

extern BackgroundWorkerHandle *ts_bgw_start_worker(const char *function, const char *name, const char *extra);

extern BackgroundWorkerHandle *ts_bgw_job_start(BgwJob *job);

extern List *ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx);

extern BgwJob *ts_bgw_job_find(int job_id, MemoryContext mctx);

extern bool ts_bgw_job_has_timeout(BgwJob *job);
extern TimestampTz ts_bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);

extern bool ts_bgw_job_delete_by_id_internal(int32 job_id);

extern bool ts_bgw_job_execute(BgwJob *job);

PGDLLEXPORT extern Datum ts_bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void ts_bgw_job_set_unknown_job_type_hook(unknown_job_type_hook_type hook);
extern void ts_bgw_job_set_job_entrypoint_function_name(char *func_name);
extern bool ts_bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func, int64 initial_runs, Interval *next_interval);

#endif							/* BGW_JOB_H */
