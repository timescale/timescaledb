/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef BGW_JOB_H
#define BGW_JOB_H

#include <postgres.h>
#include <storage/lock.h>
#include <postmaster/bgworker.h>

#include "export.h"
#include "ts_catalog/catalog.h"

typedef struct BgwJob
{
	FormData_bgw_job fd;
} BgwJob;

typedef bool job_main_func(void);
typedef bool (*scheduler_test_hook_type)(BgwJob *job);

extern BackgroundWorkerHandle *ts_bgw_job_start(BgwJob *job, Oid user_oid);

extern List *ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx);
extern List *ts_bgw_job_get_scheduled(size_t alloc_size, MemoryContext mctx);

extern TSDLLEXPORT List *ts_bgw_job_find_by_proc(const char *proc_name, const char *proc_schema);
extern TSDLLEXPORT List *ts_bgw_job_find_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT List *ts_bgw_job_find_by_proc_and_hypertable_id(const char *proc_name,
																   const char *proc_schema,
																   int32 hypertable_id);

extern bool ts_bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx);

TSDLLEXPORT BgwJob *ts_bgw_job_find(int job_id, MemoryContext mctx, bool fail_if_not_found);

extern bool ts_bgw_job_has_timeout(BgwJob *job);
extern TimestampTz ts_bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);

extern TSDLLEXPORT bool ts_bgw_job_delete_by_id(int32 job_id);
extern TSDLLEXPORT bool ts_bgw_job_update_by_id(int32 job_id, BgwJob *job);
extern TSDLLEXPORT int32 ts_bgw_job_insert_relation(Name application_name,
													Interval *schedule_interval,
													Interval *max_runtime, int32 max_retries,
													Interval *retry_period, Name proc_schema,
													Name proc_name, Name owner, bool scheduled,
													int32 hypertable_id, Jsonb *config);
extern TSDLLEXPORT void ts_bgw_job_permission_check(BgwJob *job);

extern TSDLLEXPORT void ts_bgw_job_validate_job_owner(Oid owner);

extern bool ts_bgw_job_execute(BgwJob *job);

extern TSDLLEXPORT Datum ts_bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void ts_bgw_job_set_scheduler_test_hook(scheduler_test_hook_type hook);
extern void ts_bgw_job_set_job_entrypoint_function_name(char *func_name);
extern bool ts_bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func, int64 initial_runs,
											  Interval *next_interval);

#endif /* BGW_JOB_H */
