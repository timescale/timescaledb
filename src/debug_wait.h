/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_DEBUG_WAIT_H_
#define TIMESCALEDB_DEBUG_WAIT_H_

#include <postgres.h>

#include <storage/lock.h>
#include "export.h"

/* Tag for debug waitpoints.
 *
 * Debug waitpoints only exist in debug code and are intended to allow
 * more controlled testing of the code by creating waitpoints where execution
 * will halt until the waitpoints are explicitly released.
 *
 * Each debug waitpoint is identified by a string that is hashed to a 8-byte
 * number and used with the normal advisory locks available in PostgreSQL.
 *
 * When blocking on a waitpoint, there is an attempt to take a shared lock on
 * the waitpoint. If the waitpoint is enabled by locking using an exclusive
 * lock, this will block all waiters. Once the exclusive lock is released, all
 * waiters will be able to proceed.
 */
typedef struct DebugWait
{
	const char *tagname;
	LOCKTAG tag;
} DebugWait;

extern TSDLLEXPORT void ts_debug_waitpoint_init(DebugWait *waitpoint, const char *tagname);
extern TSDLLEXPORT void ts_debug_waitpoint_wait(DebugWait *waitpoint);

#ifdef TS_DEBUG
#define DEBUG_WAITPOINT(TAG)                                                                       \
	do                                                                                             \
	{                                                                                              \
		DebugWait waitpoint;                                                                       \
		ts_debug_waitpoint_init(&waitpoint, (TAG));                                                \
		ts_debug_waitpoint_wait(&waitpoint);                                                       \
	} while (0)
#else
#define DEBUG_WAITPOINT(TAG)                                                                       \
	do                                                                                             \
	{                                                                                              \
	} while (0)
#endif

#endif /* TIMESCALEDB_DEBUG_WAIT_H_ */
