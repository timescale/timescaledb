/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <utils/snapmgr.h>
#include <utils/memutils.h>
#include <access/xact.h>
#include <pgstat.h>

#include "timer.h"
#include "compat.h"
#include "config.h"

#define MAX_TIMEOUT (5*1000L)
#define MILLISECS_PER_SEC 1000L
#define USECS_PER_MILLISEC 1000L

static inline void
on_postmaster_death(void)
{
	/*
	 * Don't call exit hooks cause we want to bail out quickly. We don't care
	 * about cleaning up shared memory in this case anyway since it's
	 * potentially corrupt.
	 */
	on_exit_reset();
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while timescaledb scheduler was working")));
}

static long
get_timeout_millisec(TimestampTz by_time)
{
	long		timeout_sec = 0;
	int			timeout_usec = 0;

	if (TIMESTAMP_IS_NOBEGIN(by_time))
		return 0;

	TimestampDifference(GetCurrentTimestamp(), by_time, &timeout_sec, &timeout_usec);


	if (timeout_sec <= 0 && timeout_usec <= 0)
		return 0;

	return timeout_sec * MILLISECS_PER_SEC + timeout_usec / USECS_PER_MILLISEC;
}

static bool
wait_using_wait_latch(TimestampTz until)
{
	int			wl_rc;

	long		timeout = get_timeout_millisec(until);

	if (timeout > MAX_TIMEOUT)
		timeout = MAX_TIMEOUT;

	/* Wait latch requires timeout to be <= INT_MAX */
	if ((int64) timeout > (int64) INT_MAX)
		timeout = INT_MAX;

	wl_rc = WaitLatchCompat(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, timeout);
	ResetLatch(MyLatch);
	if (wl_rc & WL_POSTMASTER_DEATH)
		on_postmaster_death();

	return true;
}


static const Timer standard_timer = {
	.get_current_timestamp = GetCurrentTimestamp,
	.wait = wait_using_wait_latch,
};


static const Timer *current_timer_implementation = &standard_timer;

static inline const Timer *
timer_get()
{
	return current_timer_implementation;
}

bool
timer_wait(TimestampTz until)
{
	return timer_get()->wait(until);
}

TimestampTz
timer_get_current_timestamp()
{
	return timer_get()->get_current_timestamp();
}

#ifdef TS_DEBUG
void
timer_set(const Timer *timer)
{
	current_timer_implementation = timer;
}
#endif
