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
 *
 * This allow you to start the debugger if internal assumptions are violated
 * in debug builds, but a release build will just print an error and abort the
 * transaction but and not crash the server.
 *
 * The error code is automatically set to ERRCODE_INTERNAL_ERROR and the error
 * details contains the assertion that failed in text format.
 *
 * The macro should be used for checks that are not expected to occur in
 * normal execution, or which can occur in odd corner-cases for conditions out
 * of our control (e.g., unexpected changes to the metadata) so if you have a
 * test that trigger the error, this macro should not be used.
 */
#ifdef USE_ASSERT_CHECKING
#define Ensure(COND, FMT, ...) AssertMacro(COND)
#else
#define Ensure(COND, FMT, ...)                                                                     \
	do                                                                                             \
	{                                                                                              \
		if (!(COND))                                                                               \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errdetail("Assertion '" #COND "' failed."),                                   \
					 errmsg(FMT, ##__VA_ARGS__)));                                                 \
	} while (0)
#endif

#endif /* TIMESCALEDB_DEBUG_ASSERT_H */
