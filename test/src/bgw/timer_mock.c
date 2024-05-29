/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <storage/bufmgr.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "annotations.h"
#include "bgw/launcher_interface.h"
#include "log.h"
#include "params.h"
#include "scanner.h"
#include "timer_mock.h"
#include "ts_catalog/catalog.h"

static List *bgw_handles = NIL;

static bool mock_wait(TimestampTz until);
static TimestampTz mock_current_time(void);

const Timer ts_mock_timer = {
	.get_current_timestamp = mock_current_time,
	.wait = mock_wait,
};

void
ts_timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle, MemoryContext scheduler_mctx)
{
	elog(WARNING, "[TESTING] Registered new background worker");
	MemoryContext old_context = MemoryContextSwitchTo(scheduler_mctx);
	bgw_handles = lappend(bgw_handles, handle);
	MemoryContextSwitchTo(old_context);
}

/* WARNING: mock_wait must _only_ be called from the bgw_scheduler, calling it from a worker will
 * clobber the timer state */
static bool
mock_wait(TimestampTz until)
{
	elog(WARNING,
		 "[TESTING] Wait until " INT64_FORMAT ", started at " INT64_FORMAT,
		 until,
		 ts_params_get()->current_time);

	ListCell *lc;

	switch (ts_params_get()->mock_wait_type)
	{
		case WAIT_ON_JOB:
			foreach (lc, bgw_handles)
			{
				BackgroundWorkerHandle *bgw_handle = lfirst(lc);
				WaitForBackgroundWorkerShutdown(bgw_handle);
			}
			bgw_handles = NIL;
			TS_FALLTHROUGH;
		case IMMEDIATELY_SET_UNTIL:
			ts_params_set_time(until, false);
			return true;
		case WAIT_FOR_OTHER_TO_ADVANCE:
		{
			/* Wait for another process to set "next time" */
			ts_reset_and_wait_timer_latch();

			return true;
		}
		case WAIT_FOR_STANDARD_WAITLATCH:
			ts_get_standard_timer()->wait(until);
			return true;
		default:
			return false;
	}
}

static TimestampTz
mock_current_time()
{
	return ts_params_get()->current_time;
}
