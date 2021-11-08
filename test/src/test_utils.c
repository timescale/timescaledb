/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <storage/procarray.h>
#include <commands/dbcommands.h>

#include "test_utils.h"
#include "debug_point.h"

TS_FUNCTION_INFO_V1(ts_test_error_injection);

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
