/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TEST_UTILS_H
#define TIMESCALEDB_TEST_UTILS_H
#include <postgres.h>

#include <access/xact.h>

#define AssertInt64Eq(a, b)                                                                        \
	do                                                                                             \
	{                                                                                              \
		int64 a_i = (a);                                                                           \
		int64 b_i = (b);                                                                           \
		if (a_i != b_i)                                                                            \
		{                                                                                          \
			elog(ERROR, INT64_FORMAT " != " INT64_FORMAT " @ line %d", a_i, b_i, __LINE__);        \
		}                                                                                          \
	} while (0)

#define AssertPtrEq(a, b)                                                                          \
	do                                                                                             \
	{                                                                                              \
		void *a_i = (a);                                                                           \
		void *b_i = (b);                                                                           \
		if (a_i != b_i)                                                                            \
		{                                                                                          \
			elog(ERROR, "%p != %p @ line %d", a_i, b_i, __LINE__);                                 \
		}                                                                                          \
	} while (0)

#define EnsureError(a)                                                                             \
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
			elog(ERROR, "failed to panic @ line %d", __LINE__);                                    \
		}                                                                                          \
	} while (0)

#endif
