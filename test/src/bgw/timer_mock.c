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

const Timer mock_timer = {
	.get_current_timestamp = mock_current_time,
	.wait = mock_wait,
};

void
timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle)
{
	elog(WARNING, "[TESTING] Registered new background worker");
	bgw_handle = handle;
}

/* WARNING: mock_wait must _only_ be called from the bgw_scheduler, calling it from a worker will clobber the timer state */
static bool
mock_wait(TimestampTz until)
{
	elog(WARNING, "[TESTING] Wait until %ld, started at %ld", until, params_get()->current_time);

	if (!params_get()->mock_wait_returns_immediately && bgw_handle != NULL)
	{
		WaitForBackgroundWorkerShutdown(bgw_handle);
		bgw_handle = NULL;
	}
	params_set_time(until);
	return true;
}

static TimestampTz
mock_current_time()
{
	return params_get()->current_time;
}
