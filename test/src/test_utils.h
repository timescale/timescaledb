/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TEST_UTILS_H
#define TIMESCALEDB_TEST_UTILS_H

#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>

#include "export.h"

static inline const char *
strip_path(const char *filename)
{
	int i = 0, slash = 0;

	while (filename[i] != '\0')
	{
		if (filename[i] == '/' || filename[i] == '\\')
			slash = i;
		i++;
	}

	return &filename[slash + 1];
}

#define TestFailure(fmt, ...)                                                                      \
	do                                                                                             \
	{                                                                                              \
		elog(ERROR, "TestFailure | " fmt "", ##__VA_ARGS__);                                       \
		pg_unreachable();                                                                          \
	} while (0)

#define TestAssertInt64Eq(a, b)                                                                    \
	do                                                                                             \
	{                                                                                              \
		int64 a_i = (a);                                                                           \
		int64 b_i = (b);                                                                           \
		if (a_i != b_i)                                                                            \
			TestFailure("(%s == %s) [" INT64_FORMAT " == " INT64_FORMAT "]", #a, #b, a_i, b_i);    \
	} while (0)

#define TestAssertPtrEq(a, b)                                                                      \
	do                                                                                             \
	{                                                                                              \
		void *a_i = (a);                                                                           \
		void *b_i = (b);                                                                           \
		if (a_i != b_i)                                                                            \
			TestFailure("(%s == %s)", #a, #b);                                                     \
	} while (0)

#define TestAssertDoubleEq(a, b)                                                                   \
	do                                                                                             \
	{                                                                                              \
		double a_i = (a);                                                                          \
		double b_i = (b);                                                                          \
		if (a_i != b_i)                                                                            \
			TestFailure("(%s == %s) [%f == %f]", #a, #b, a_i, b_i);                                \
	} while (0)

#define TestEnsureError(a)                                                                         \
	do                                                                                             \
	{                                                                                              \
		volatile bool this_has_panicked = false;                                                   \
		MemoryContext oldctx = CurrentMemoryContext;                                               \
		BeginInternalSubTransaction("error expected");                                             \
		PG_TRY();                                                                                  \
		{                                                                                          \
			(a);                                                                                   \
		}                                                                                          \
		PG_CATCH();                                                                                \
		{                                                                                          \
			this_has_panicked = true;                                                              \
			RollbackAndReleaseCurrentSubTransaction();                                             \
			FlushErrorState();                                                                     \
		}                                                                                          \
		PG_END_TRY();                                                                              \
		MemoryContextSwitchTo(oldctx);                                                             \
		if (!this_has_panicked)                                                                    \
		{                                                                                          \
			elog(ERROR, "failed to panic");                                                        \
		}                                                                                          \
	} while (0)

#define TestAssertTrue(cond)                                                                       \
	do                                                                                             \
	{                                                                                              \
		if (!(cond))                                                                               \
			TestFailure("(%s)", #cond);                                                            \
	} while (0)

#define TS_TEST_FN(name)                                                                           \
	TS_FUNCTION_INFO_V1(name);                                                                     \
	Datum name(PG_FUNCTION_ARGS)

#endif /* TIMESCALEDB_TEST_UTILS_H */
