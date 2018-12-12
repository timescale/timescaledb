/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <storage/bufmgr.h>
#include <utils/rel.h>
#include <utils/tqual.h>

#include "timer_mock.h"
#include "log.h"
#include "scanner.h"
#include "catalog.h"
#include "compat.h"
#include "params.h"
#include "bgw/launcher_interface.h"


static BackgroundWorkerHandle *bgw_handle = NULL;


static bool mock_wait(TimestampTz until);
static TimestampTz mock_current_time(void);

const Timer ts_mock_timer = {
	.get_current_timestamp = mock_current_time,
	.wait = mock_wait,
};

void
ts_timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle)
{
	elog(WARNING, "[TESTING] Registered new background worker");
	bgw_handle = handle;
}

/* WARNING: mock_wait must _only_ be called from the bgw_scheduler, calling it from a worker will clobber the timer state */
static bool
mock_wait(TimestampTz until)
{
	elog(WARNING, "[TESTING] Wait until %ld, started at %ld", until, ts_params_get()->current_time);

	switch (ts_params_get()->mock_wait_type)
	{
		case WAIT_ON_JOB:
			if (bgw_handle != NULL)
			{
				WaitForBackgroundWorkerShutdown(bgw_handle);
				bgw_handle = NULL;
			}
			/* Now fall through to set the time */
		case IMMEDIATELY_SET_UNTIL:
			ts_params_set_time(until, false);
			return true;
		case WAIT_FOR_OTHER_TO_ADVANCE:
			{
				/* Wait for another process to set "next time" */
				ts_reset_and_wait_timer_latch();

				return true;
			}
		default:
			return false;
	}
}

static TimestampTz
mock_current_time()
{
	return ts_params_get()->current_time;
}
