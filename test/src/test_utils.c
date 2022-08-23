/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "test_utils.h"

#include <postgres.h>

#include <commands/dbcommands.h>
#include <compat/compat.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <storage/latch.h>
#include <storage/proc.h>
#include <storage/procarray.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/memutils.h>

#include "debug_point.h"

TS_FUNCTION_INFO_V1(ts_test_error_injection);
TS_FUNCTION_INFO_V1(ts_debug_shippable_error_after_n_rows);
TS_FUNCTION_INFO_V1(ts_debug_shippable_fatal_after_n_rows);

/*
 * Test assertion macros.
 *
 * Errors are expected since we want to test that the macros work. For each
 * macro, test one failing and one non-failing condition. The non-failing must
 * come first since the failing one will abort the function.
 */
TS_TEST_FN(ts_test_utils_condition)
{
	bool true_value = true;
	bool false_value = false;

	TestAssertTrue(true_value == true_value);
	TestAssertTrue(true_value == false_value);

	PG_RETURN_VOID();
}

TS_TEST_FN(ts_test_utils_int64_eq)
{
	int64 big = 32532978;
	int64 small = 3242234;

	TestAssertInt64Eq(big, small);
	TestAssertInt64Eq(big, big);

	PG_RETURN_VOID();
}

TS_TEST_FN(ts_test_utils_ptr_eq)
{
	bool true_value = true;
	bool false_value = false;
	bool *true_ptr = &true_value;
	bool *false_ptr = &false_value;

	TestAssertPtrEq(true_ptr, true_ptr);
	TestAssertPtrEq(true_ptr, false_ptr);

	PG_RETURN_VOID();
}

TS_TEST_FN(ts_test_utils_double_eq)
{
	double big_double = 923423478.3242;
	double small_double = 324.3;

	TestAssertDoubleEq(big_double, big_double);
	TestAssertDoubleEq(big_double, small_double);

	PG_RETURN_VOID();
}

Datum
ts_test_error_injection(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_PP(0);
	DEBUG_ERROR_INJECTION(text_to_cstring(name));
	PG_RETURN_VOID();
}

static int
throw_after_n_rows(int max_rows, int severity)
{
	static LocalTransactionId last_lxid = 0;
	static int rows_seen = 0;

	if (last_lxid != MyProc->lxid)
	{
		/* Reset it for each new transaction for predictable results. */
		rows_seen = 0;
		last_lxid = MyProc->lxid;
	}

	rows_seen++;

	if (max_rows <= rows_seen)
	{
		ereport(severity,
				(errmsg("debug point: requested to error out after %d rows, %d rows seen",
						max_rows,
						rows_seen)));
	}

	return rows_seen;
}

Datum
ts_debug_shippable_error_after_n_rows(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(throw_after_n_rows(PG_GETARG_INT32(0), ERROR));
}

Datum
ts_debug_shippable_fatal_after_n_rows(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(throw_after_n_rows(PG_GETARG_INT32(0), FATAL));
}

/*
 * Broken send/receive functions for int4 that throw after an (arbitrarily
 * chosen prime) number of rows.
 * Use ERROR, not FATAL, because PG versions < 14 are unable to report a FATAL
 * error to the access node before closing the connection, so the test results
 * would be different.
 */
#define ARBITRARY_PRIME_NUMBER 7103

TS_FUNCTION_INFO_V1(ts_debug_broken_int4recv);

Datum
ts_debug_broken_int4recv(PG_FUNCTION_ARGS)
{
	(void) throw_after_n_rows(ARBITRARY_PRIME_NUMBER, ERROR);
	return int4recv(fcinfo);
}

TS_FUNCTION_INFO_V1(ts_debug_broken_int4send);

Datum
ts_debug_broken_int4send(PG_FUNCTION_ARGS)
{
	(void) throw_after_n_rows(ARBITRARY_PRIME_NUMBER, ERROR);
	return int4send(fcinfo);
}

TS_FUNCTION_INFO_V1(ts_debug_sleepy_int4recv);

/* Sleep after some rows. */
Datum
ts_debug_sleepy_int4recv(PG_FUNCTION_ARGS)
{
	static LocalTransactionId last_lxid = 0;
	static int rows_seen = 0;

	if (last_lxid != MyProc->lxid)
	{
		/* Reset it for each new transaction for predictable results. */
		rows_seen = 0;
		last_lxid = MyProc->lxid;
	}

	rows_seen++;

	if (rows_seen >= 1000)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000,
						 /* wait_event_info = */ 0);
		ResetLatch(MyLatch);

		rows_seen = 0;
	}

	return int4recv(fcinfo);
}

TS_FUNCTION_INFO_V1(ts_bgw_wait);
Datum
ts_bgw_wait(PG_FUNCTION_ARGS)
{
	text *datname = PG_GETARG_TEXT_PP(0);
	/* The timeout is given in seconds, so we compute the number of iterations
	 * necessary to get a coverage of that time */
	uint32 iterations = PG_ARGISNULL(1) ? 5 : (PG_GETARG_UINT32(1) + 4) / 5;
	Oid dboid = get_database_oid(text_to_cstring(datname), false);

	/* This function contains a timeout of 5 seconds, so we iterate a few
	 * times to make sure that it really has terminated. */
	int notherbackends = 0;
	int npreparedxacts = 0;
	while (iterations-- > 0)
	{
		if (!CountOtherDBBackends(dboid, &notherbackends, &npreparedxacts))
			PG_RETURN_NULL();
		ereport(NOTICE,
				(errmsg("source database \"%s\" is being accessed by other users",
						text_to_cstring(datname)),
				 errdetail("There are %d other session(s) and %d prepared transaction(s) using the "
						   "database.",
						   notherbackends,
						   npreparedxacts)));
	}
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_IN_USE),
			 errmsg("source database \"%s\" is being accessed by other users",
					text_to_cstring(datname)),
			 errdetail("There are %d other session(s) and %d prepared transaction(s) using the "
					   "database.",
					   notherbackends,
					   npreparedxacts)));
	pg_unreachable();
}

/*
 * Return the number of bytes allocated in a given memory context and its
 * children.
 */
TS_FUNCTION_INFO_V1(ts_debug_allocated_bytes);
Datum
ts_debug_allocated_bytes(PG_FUNCTION_ARGS)
{
	char *context_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	if (strcmp(context_name, "PortalContext") == 0)
	{
#if PG13_GE
		PG_RETURN_UINT64(MemoryContextMemAllocated(PortalContext, /* recurse = */ true));
#else
		/* Don't have this function on PG 12. */
		PG_RETURN_UINT64(1);
#endif
	}
	else
	{
		ereport(ERROR,
				(errmsg("unknown memory context '%s' (search for arbitrary contexts by name is not"
						"implemented)",
						context_name)));
		PG_RETURN_NULL();
	}
}
