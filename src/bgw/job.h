#ifndef BGW_JOB_H
#define BGW_JOB_H

#include <postmaster/bgworker.h>

typedef enum JobType JobType;
typedef struct BgwJob BgwJob;

#include "catalog.h"

enum JobType
{
	JOB_TYPE_VERSION_CHECK = 0,
	JOB_TYPE_UNKNOWN,
	_MAX_JOB_TYPE
};

struct BgwJob
{
	FormData_bgw_job fd;
	JobType		bgw_type;

};

typedef bool job_main_func (void);
typedef bool (*unknown_job_type_hook_type) (BgwJob *job);

BackgroundWorkerHandle *bgw_start_worker(const char *function, const char *name, const char *extra);

BackgroundWorkerHandle *bgw_job_start(BgwJob *job);

extern List *bgw_job_get_all(size_t alloc_size, MemoryContext mctx);

extern BgwJob *bgw_job_find(int job_id, MemoryContext mctx);

extern bool bgw_job_has_timeout(BgwJob *job);
extern TimestampTz bgw_job_timeout_at(BgwJob *job, TimestampTz start_time);


bool		bgw_job_execute(BgwJob *job);

PGDLLEXPORT extern Datum ts_bgw_job_entrypoint(PG_FUNCTION_ARGS);
extern void bgw_job_set_unknown_job_type_hook(unknown_job_type_hook_type hook);
extern void bgw_job_set_job_entrypoint_function_name(char *func_name);
extern bool bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func, int64 initial_runs, Interval *next_interval);

#endif							/* BGW_JOB_H */
