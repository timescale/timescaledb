#include <postgres.h>
#include <executor/executor.h>
#include <access/xact.h>

#include "executor.h"
#include "compat.h"

static ExecutorRun_hook_type prev_ExecutorRun_hook;
static uint64 additional_tuples;
static uint64 level = 0;

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

void
executor_level_enter(void)
{
	if (0 == level)
	{
		additional_tuples = 0;
	}
	level++;
}

void
executor_level_exit(void)
{
	level--;
}

#if PG10
static void
timescaledb_ExecutorRun(QueryDesc *queryDesc,
						ScanDirection direction,
						uint64 count,
						bool execute_once)
{
	executor_level_enter();

	if (prev_ExecutorRun_hook)
		(*prev_ExecutorRun_hook) (queryDesc, direction, count, execute_once);
	else
		standard_ExecutorRun(queryDesc, direction, count, execute_once);

	executor_level_exit();
	if (0 == level)
	{
		queryDesc->estate->es_processed += additional_tuples;
	}
}

#elif PG96

static void
timescaledb_ExecutorRun(QueryDesc *queryDesc,
						ScanDirection direction,
						uint64 count)
{
	executor_level_enter();

	if (prev_ExecutorRun_hook)
		(*prev_ExecutorRun_hook) (queryDesc, direction, count);
	else
		standard_ExecutorRun(queryDesc, direction, count);

	executor_level_exit();
	if (0 == level)
	{
		queryDesc->estate->es_processed += additional_tuples;
	}
}
#endif

static void
executor_AtEOXact_abort(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			level = 0;
		default:
			break;
	}

}

void
_executor_init(void)
{
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = timescaledb_ExecutorRun;

	RegisterXactCallback(executor_AtEOXact_abort, NULL);
}

void
_executor_fini(void)
{
	ExecutorRun_hook = prev_ExecutorRun_hook;
	UnregisterXactCallback(executor_AtEOXact_abort, NULL);
}
