/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "debug_wait.h"

#include <postgres.h>

#include <fmgr.h>

#include <access/hash.h>
#include <storage/ipc.h>
#include <storage/lock.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include "export.h"

static uint64
ts_debug_waitpoint_tag_to_id(const char *tagname)
{
	return DatumGetUInt32(hash_any((const unsigned char *) tagname, strlen(tagname)));
}

void
ts_debug_waitpoint_init(DebugWait *waitpoint, const char *tagname)
{
	/* Use 64-bit hashing to get two independent 32-bit hashes */
	uint64 hash = ts_debug_waitpoint_tag_to_id(tagname);

	SET_LOCKTAG_ADVISORY(waitpoint->tag, MyDatabaseId, (uint32)(hash >> 32), (uint32) hash, 1);
	waitpoint->tagname = pstrdup(tagname);
	ereport(DEBUG3,
			(errmsg("initializing waitpoint '%s' to use " UINT64_FORMAT,
					waitpoint->tagname,
					hash)));
}

/*
 * Wait for the waitpoint to be released.
 *
 * This is handled by first trying to get a shared lock, which will not block
 * other sessions that try to grab the same lock but will block if an
 * exclusive lock is already taken, and then release the lock immediately
 * after.
 *
 * This function can decide to block while taking the shared lock or it can
 * have a retry loop to take the share lock. This retry loop option is useful
 * in cases where this function gets called from deep down inside a transaction
 * where interrupts are not being served currently.
 */
void
ts_debug_waitpoint_wait(DebugWait *waitpoint, bool blocking)
{
	LockAcquireResult lock_acquire_result pg_attribute_unused();
	bool lock_release_result pg_attribute_unused();

	ereport(DEBUG3, (errmsg("waiting on waitpoint '%s'", waitpoint->tagname)));

	if (blocking)
		lock_acquire_result = LockAcquire(&waitpoint->tag, ShareLock, true, false);
	else
	{
		/*
		 * Trying to wait indefinitely here could lead to hangs. The current
		 * behavior is to retry for retry_count and return with a warning
		 * if that's crossed.
		 *
		 * If required, in future, we could take an additional option to decide
		 * if the caller wants to retry indefinitely or return with a warning.
		 * But the current behavior based on the "blocking" argument is ok for
		 * now.
		 */
		unsigned int retry_count = 1000;

		/* try to acquire the lock without waiting. */
		do
		{
			/* try to acquire the lock without waiting. */
			lock_acquire_result = LockAcquire(&waitpoint->tag, ShareLock, true, true);

			if (lock_acquire_result == LOCKACQUIRE_OK)
				break;

			/* don't dare to take a lock when the proc is exiting! */
			if (proc_exit_inprogress || ProcDiePending)
				return;

			if (retry_count == 0)
			{
				elog(WARNING, "timeout while acquiring waitpoint lock");
				return;
			}
			retry_count--;

			/* retry after some time */
			pg_usleep(100L);

		} while (lock_acquire_result == LOCKACQUIRE_NOT_AVAIL);
	}
	Assert(lock_acquire_result == LOCKACQUIRE_OK);

	lock_release_result = LockRelease(&waitpoint->tag, ShareLock, true);
	Assert(lock_release_result);

	ereport(DEBUG3, (errmsg("proceeding after waitpoint '%s'", waitpoint->tagname)));
}

static void
debug_waitpoint_enable(DebugWait *waitpoint)
{
	ereport(DEBUG1, (errmsg("enabling waitpoint \"%s\"", waitpoint->tagname)));
	if (LockAcquire(&waitpoint->tag, ExclusiveLock, true, true) == LOCKACQUIRE_NOT_AVAIL)
		ereport(NOTICE, (errmsg("debug waitpoint \"%s\" already enabled", waitpoint->tagname)));
}

static void
debug_waitpoint_release(DebugWait *waitpoint)
{
	ereport(DEBUG1, (errmsg("releasing waitpoint \"%s\"", waitpoint->tagname)));
	if (!LockRelease(&waitpoint->tag, ExclusiveLock, true))
		elog(ERROR, "cannot release waitpoint \"%s\"", waitpoint->tagname);
}

/*
 * Enable a waitpoint to block when being reached.
 *
 * This function will always succeed since we will not lock the waitpoint if
 * it is already locked. A notice will be printed if the waitpoint is already
 * enabled.
 */
TS_FUNCTION_INFO_V1(ts_debug_waitpoint_enable);

Datum
ts_debug_waitpoint_enable(PG_FUNCTION_ARGS)
{
	text *tag = PG_GETARG_TEXT_PP(0);
	DebugWait waitpoint;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no tag provided")));

	ts_debug_waitpoint_init(&waitpoint, text_to_cstring(tag));
	debug_waitpoint_enable(&waitpoint);
	PG_RETURN_VOID();
}

/*
 * Release a waitpoint allowing execution to proceed.
 */
TS_FUNCTION_INFO_V1(ts_debug_waitpoint_release);
Datum
ts_debug_waitpoint_release(PG_FUNCTION_ARGS)
{
	text *tag = PG_GETARG_TEXT_PP(0);
	DebugWait waitpoint;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no tag provided")));

	ts_debug_waitpoint_init(&waitpoint, text_to_cstring(tag));
	debug_waitpoint_release(&waitpoint);
	PG_RETURN_VOID();
}

/*
 * Get the waitpoint identifier from the tag name.
 */
TS_FUNCTION_INFO_V1(ts_debug_waitpoint_id);

Datum
ts_debug_waitpoint_id(PG_FUNCTION_ARGS)
{
	text *tag = PG_GETARG_TEXT_PP(0);

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no tag provided")));

	PG_RETURN_UINT64(ts_debug_waitpoint_tag_to_id(text_to_cstring(tag)));
}
