/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "debug_wait.h"

#include <postgres.h>

#include <fmgr.h>

#include <access/hash.h>
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
	ereport(DEBUG1,
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
 */
void
ts_debug_waitpoint_wait(DebugWait *waitpoint)
{
	LockAcquireResult lock_acquire_result pg_attribute_unused();
	bool lock_release_result pg_attribute_unused();

	ereport(DEBUG1, (errmsg("waiting on waitpoint '%s'", waitpoint->tagname)));

	/* Take the lock. This should always succeed, anything else is a bug. */
	lock_acquire_result = LockAcquire(&waitpoint->tag, ShareLock, true, false);
	Assert(lock_acquire_result == LOCKACQUIRE_OK);

	lock_release_result = LockRelease(&waitpoint->tag, ShareLock, true);
	Assert(lock_release_result);

	ereport(DEBUG1, (errmsg("proceeding after waitpoint '%s'", waitpoint->tagname)));
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
