/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DEBUG_ASSERT_H
#define TIMESCALEDB_DEBUG_ASSERT_H

#include <c.h>
#include <postgres.h>

/*
 * Macro that expands to an assert in debug builds and to an ereport in
 * release builds.
 */
#if DEBUG
#define AssertOr(LEVEL, COND) Assert(COND)
#else
#define AssertOr(LEVEL, COND)                                                                      \
	do                                                                                             \
	{                                                                                              \
		if (!(COND))                                                                               \
			ereport((LEVEL),                                                                       \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("assertion failure"),                                                  \
					 errdetail("Assertion '" #COND "' failed.")));                                 \
	} while (0)
#endif

#endif /* TIMESCALEDB_DEBUG_ASSERT_H */
