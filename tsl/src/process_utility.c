/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <commands/event_trigger.h>
#include <catalog/pg_trigger.h>

#include "process_utility.h"
#include "remote/dist_commands.h"
#include "remote/dist_ddl.h"

#if !PG96

void
tsl_ddl_command_start(ProcessUtilityArgs *args)
{
	dist_ddl_start(args);
}

void
tsl_ddl_command_end(EventTriggerData *command)
{
	dist_ddl_end(command);
}

void
tsl_sql_drop(List *dropped_objects)
{
	dist_ddl_drop(dropped_objects);
}

static void
tsl_process_utility_xact_abort(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			dist_ddl_reset();
			break;
		default:
			break;
	}
}

static void
tsl_process_utility_subxact_abort(SubXactEvent event, SubTransactionId mySubid,
								  SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_ABORT_SUB:
			dist_ddl_reset();
			break;
		default:
			break;
	}
}

void
_tsl_process_utility_init(void)
{
	dist_ddl_init();

	RegisterXactCallback(tsl_process_utility_xact_abort, NULL);
	RegisterSubXactCallback(tsl_process_utility_subxact_abort, NULL);
}

void
_tsl_process_utility_fini(void)
{
	dist_ddl_reset();

	UnregisterXactCallback(tsl_process_utility_xact_abort, NULL);
	UnregisterSubXactCallback(tsl_process_utility_subxact_abort, NULL);
}

#endif /* PG9.6*/
