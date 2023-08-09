/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "debug_point.h"

#include <postgres.h>

#include <fmgr.h>

#include <access/hash.h>
#include <access/xact.h>
#include <storage/ipc.h>
#include <storage/lock.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include "export.h"
#include "annotations.h"

TS_FUNCTION_INFO_V1(ts_debug_point_enable);
TS_FUNCTION_INFO_V1(ts_debug_point_release);
TS_FUNCTION_INFO_V1(ts_debug_point_id);

/*
 * Debug points only exist in debug code and are intended to allow
 * more controlled testing of the code.
 *
 * Debug points can be used as a wait point (1) or as a way to
 * introduce error injections (2).
 *
 * (1) When used as wait points, execution will halt until the debug points are
 * explicitly released.
 *
 * When waiting on a debug point, there is an attempt to take a shared lock on it.
 * If the debug point is enabled by locking using an exclusive
 * lock, this will block all waiters. Once the exclusive lock is released, all
 * waiters will be able to proceed.
 *
 * (2) is similar to (1), but, instead of waiting for the debug point to be
 * released, it will generate an error immediately.
 *
 */

/* Tag for debug points.
 *
 * Each debug point is identified by a string that is hashed to a 8-byte
 * number and used with the normal advisory locks available in PostgreSQL.
 */
typedef struct DebugPoint
{
	const char *name;
	LOCKTAG tag;
} DebugPoint;

static uint64
debug_point_name_to_id(const char *name)
{
	return DatumGetUInt32(hash_any((const unsigned char *) name, strlen(name)));
}

static void
debug_point_init(DebugPoint *point, const char *name)
{
	/* Use 64-bit hashing to get two independent 32-bit hashes */
	uint64 hash = debug_point_name_to_id(name);

	SET_LOCKTAG_ADVISORY(point->tag, MyDatabaseId, (uint32) (hash >> 32), (uint32) hash, 1);
	point->name = pstrdup(name);
	ereport(DEBUG3,
			(errmsg("initializing debug point '%s' to use " UINT64_FORMAT, point->name, hash)));
}

static void
debug_point_enable(const DebugPoint *point)
{
	LockAcquireResult lock_acquire_result;

	ereport(DEBUG1, (errmsg("enabling debug point \"%s\"", point->name)));

	lock_acquire_result = LockAcquire(&point->tag, ExclusiveLock, true, true);
	switch (lock_acquire_result)
	{
		case LOCKACQUIRE_ALREADY_HELD:
		case LOCKACQUIRE_ALREADY_CLEAR:
			LockRelease(&point->tag, ExclusiveLock, true);
			TS_FALLTHROUGH;
		case LOCKACQUIRE_NOT_AVAIL:
			ereport(ERROR, (errmsg("debug point \"%s\" already enabled", point->name)));
			break;
		case LOCKACQUIRE_OK:
			break;
	}
}

static void
debug_point_release(const DebugPoint *point)
{
	ereport(DEBUG1, (errmsg("releasing debug point \"%s\"", point->name)));

	if (!LockRelease(&point->tag, ExclusiveLock, true))
		ereport(ERROR, (errmsg("cannot release debug point \"%s\"", point->name)));
}

/*
 * Enable a debug point to block when being reached.
 *
 * This function will always succeed since we will not lock the debug point if
 * it is already locked. A notice will be printed if the debug point is already
 * enabled.
 */
Datum
ts_debug_point_enable(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_PP(0);
	DebugPoint point;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no name provided")));

	debug_point_init(&point, text_to_cstring(name));
	debug_point_enable(&point);

	PG_RETURN_VOID();
}

/*
 * Release a debug point allowing execution to proceed.
 */
Datum
ts_debug_point_release(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_PP(0);
	DebugPoint point;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no name provided")));

	debug_point_init(&point, text_to_cstring(name));
	debug_point_release(&point);

	PG_RETURN_VOID();
}

/*
 * Get the debug point identifier from the name.
 */
Datum
ts_debug_point_id(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_PP(0);

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no name provided")));

	PG_RETURN_UINT64(debug_point_name_to_id(text_to_cstring(name)));
}

/*
 * Wait for the debug point to be released.
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
ts_debug_point_wait(const char *name, bool blocking)
{
	DebugPoint point;
	LockAcquireResult lock_acquire_result pg_attribute_unused();
	bool lock_release_result pg_attribute_unused();

	/* Ensure that we are in a transaction before trying for locks */
	if (!IsTransactionState())
		return;

	debug_point_init(&point, name);

	ereport(DEBUG3, (errmsg("waiting on debug point '%s'", point.name)));

	if (blocking)
		lock_acquire_result = LockAcquire(&point.tag, ShareLock, true, false);
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
			lock_acquire_result = LockAcquire(&point.tag, ShareLock, true, true);

			if (lock_acquire_result == LOCKACQUIRE_OK)
				break;

			/* don't dare to take a lock when the proc is exiting! */
			if (proc_exit_inprogress || ProcDiePending)
				return;

			if (retry_count == 0)
			{
				elog(WARNING, "timeout while acquiring debug point lock");
				return;
			}
			retry_count--;

			/* retry after some time */
			pg_usleep(100L);

		} while (lock_acquire_result == LOCKACQUIRE_NOT_AVAIL);
	}
	Assert(lock_acquire_result == LOCKACQUIRE_OK);

	lock_release_result = LockRelease(&point.tag, ShareLock, true);
	Assert(lock_release_result);

	ereport(DEBUG3, (errmsg("proceeding after debug point '%s'", point.name)));
}

/*
 * Produce an error in case if the debug point is enabled.
 *
 * The idea is to enable the debug point separately first which
 * acquires a ShareLock on this tag. With the debug point enabled, this function
 * when invoked will not get the exclusive lock and will be able to raise
 * the error as desired.
 */
void
ts_debug_point_raise_error_if_enabled(const char *name)
{
	DebugPoint point;
	LockAcquireResult lock_acquire_result;

	debug_point_init(&point, name);

	lock_acquire_result = LockAcquire(&point.tag, ExclusiveLock, true, true);
	switch (lock_acquire_result)
	{
		case LOCKACQUIRE_OK:
		case LOCKACQUIRE_ALREADY_HELD:
		case LOCKACQUIRE_ALREADY_CLEAR:
			/* Release/decrement lock count */
			LockRelease(&point.tag, ExclusiveLock, true);
			if (lock_acquire_result == LOCKACQUIRE_OK)
				return;
			break;
		case LOCKACQUIRE_NOT_AVAIL:
			break;
	}

	ereport(ERROR, (errmsg("error injected at debug point '%s'", point.name)));
}
