#include <postgres.h>
#include <executor/executor.h>

#include "executor.h"

static ExecutorRun_hook_type prev_ExecutorRun_hook;
static uint64 additional_tuples;

void
executor_add_number_tuples_processed(uint64 count)
{
	additional_tuples += count;
}

uint64
executor_get_additional_tuples_processed()
{
	return additional_tuples;
}

static void
timescaledb_ExecutorRun(QueryDesc *queryDesc,
						ScanDirection direction,
						uint64 count)
{
	additional_tuples = 0;
	if (prev_ExecutorRun_hook)
		(*prev_ExecutorRun_hook) (queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);
	queryDesc->estate->es_processed += additional_tuples;
}

void
_executor_init(void)
{
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = timescaledb_ExecutorRun;
}

void
_executor_fini(void)
{
	ExecutorRun_hook = prev_ExecutorRun_hook;
}
