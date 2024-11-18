/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <postmaster/bgworker.h>
#include <storage/lock.h>

#include "export.h"
#include "ts_catalog/catalog.h"

#define TELEMETRY_INITIAL_NUM_RUNS 12
#define SCHEDULER_APPNAME "TimescaleDB Background Worker Scheduler"

typedef struct BgwJobHistory
{
	int64 id;
	TimestampTz execution_start;
} BgwJobHistory;

typedef struct BgwJob
{
	FormData_bgw_job fd;
	BgwJobHistory job_history;
} BgwJob;

/* Positive result numbers reserved for success */
typedef enum JobResult
{
	JOB_FAILURE_TO_START = -1,
	JOB_FAILURE_IN_EXECUTION = 0,
	JOB_SUCCESS = 1,
} JobResult;

typedef bool job_main_func(void);
typedef bool (*scheduler_test_hook_type)(BgwJob *job);

extern BackgroundWorkerHandle *ts_bgw_job_start(BgwJob *job, Oid user_oid);

extern List *ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx);
extern List *ts_bgw_job_get_scheduled(size_t alloc_size, MemoryContext mctx);

extern TSDLLEXPORT List *ts_bgw_job_find_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT List *ts_bgw_job_find_by_proc_and_hypertable_id(const char *proc_name,
																   const char *proc_schema,
																   int32 hypertable_id);

extern bool ts_bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx);
TSDLLEXPORT bool ts_lock_job_id(int32 job_id, LOCKMODE mode, bool session_lock, LOCKTAG *tag,
								bool block);
TSDLLEXPORT BgwJob *ts_bgw_job_find(int job_id, MemoryContext mctx, bool fail_if_not_found);

extern bool ts_bgw_job_has_timeout(BgwJob *job);
extern TimestampTz ts_bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);

extern TSDLLEXPORT bool ts_bgw_job_delete_by_id(int32 job_id);
extern TSDLLEXPORT bool ts_bgw_job_update_by_id(int32 job_id, BgwJob *job);
extern TSDLLEXPORT int32 ts_bgw_job_insert_relation(
	Name application_name, Interval *schedule_interval, Interval *max_runtime, int32 max_retries,
	Interval *retry_period, Name proc_schema, Name proc_name, Name check_schema, Name check_name,
	Oid owner, bool scheduled, bool fixed_schedule, int32 hypertable_id, Jsonb *config,
	TimestampTz initial_start, const char *timezone);
extern TSDLLEXPORT void ts_bgw_job_permission_check(BgwJob *job, const char *cmd);

extern TSDLLEXPORT void ts_bgw_job_validate_job_owner(Oid owner);

extern JobResult ts_bgw_job_execute(BgwJob *job);
extern TSDLLEXPORT void ts_bgw_job_run_config_check(Oid check, int32 job_id, Jsonb *config);

extern TSDLLEXPORT Datum ts_bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void ts_bgw_job_set_scheduler_test_hook(scheduler_test_hook_type hook);
extern void ts_bgw_job_set_job_entrypoint_function_name(char *func_name);
extern TSDLLEXPORT bool ts_bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func,
														  int64 initial_runs,
														  Interval *next_interval, bool atomic,
														  bool mark);
extern TSDLLEXPORT void ts_bgw_job_validate_schedule_interval(Interval *schedule_interval);
extern TSDLLEXPORT char *ts_bgw_job_validate_timezone(Datum timezone);

extern TSDLLEXPORT bool ts_is_telemetry_job(BgwJob *job);
ScanTupleResult ts_bgw_job_change_owner(TupleInfo *ti, void *data);

extern TSDLLEXPORT Oid ts_bgw_job_get_funcid(BgwJob *job);
extern TSDLLEXPORT const char *ts_bgw_job_function_call_string(BgwJob *job);
