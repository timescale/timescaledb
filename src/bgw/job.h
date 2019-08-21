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
#include "catalog.h"

typedef enum JobType
{
	JOB_TYPE_VERSION_CHECK = 0,
	JOB_TYPE_REORDER,
	JOB_TYPE_DROP_CHUNKS,
	JOB_TYPE_CONTINUOUS_AGGREGATE,
	JOB_TYPE_COMPRESS_CHUNKS,
	/* end of real jobs */
	JOB_TYPE_UNKNOWN,
	_MAX_JOB_TYPE
} JobType;

typedef struct BgwJob
{
	FormData_bgw_job fd;
	JobType bgw_type;
} BgwJob;

typedef bool job_main_func(void);
typedef bool (*unknown_job_type_hook_type)(BgwJob *job);
typedef Oid (*unknown_job_type_owner_hook_type)(BgwJob *job);

extern BackgroundWorkerHandle *ts_bgw_start_worker(const char *function, const char *name,
												   const char *extra);

extern BackgroundWorkerHandle *ts_bgw_job_start(BgwJob *job, Oid user_oid);

extern List *ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx);

extern bool ts_bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx);

TSDLLEXPORT BgwJob *ts_bgw_job_find(int job_id, MemoryContext mctx, bool fail_if_not_found);

extern bool ts_bgw_job_has_timeout(BgwJob *job);
extern TimestampTz ts_bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);

extern TSDLLEXPORT bool ts_bgw_job_delete_by_id(int32 job_id);
extern TSDLLEXPORT int32 ts_bgw_job_insert_relation(Name application_name, Name job_type,
													Interval *schedule_interval,
													Interval *max_runtime, int32 max_retries,
													Interval *retry_period);
extern TSDLLEXPORT void ts_bgw_job_update_by_id(int32 job_id, BgwJob *updated_job);

extern TSDLLEXPORT void ts_bgw_job_permission_check(BgwJob *job);

extern bool ts_bgw_job_execute(BgwJob *job);

extern Oid ts_bgw_job_owner(BgwJob *job);
extern TSDLLEXPORT Datum ts_bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void ts_bgw_job_set_unknown_job_type_hook(unknown_job_type_hook_type hook);
extern void ts_bgw_job_set_unknown_job_type_owner_hook(unknown_job_type_owner_hook_type hook);
extern void ts_bgw_job_set_job_entrypoint_function_name(char *func_name);
extern bool ts_bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func, int64 initial_runs,
											  Interval *next_interval);

#endif /* BGW_JOB_H */
