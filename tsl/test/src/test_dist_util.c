/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <export.h>
#include <access/htup_details.h>

#include <dist_commands.h>
#include "dist_util.h"

TS_FUNCTION_INFO_V1(ts_test_compatible_version);

Datum
ts_test_compatible_version(PG_FUNCTION_ARGS)
{
	const char *checked_version = PG_GETARG_CSTRING(0);
	const char *reference_version = PG_GETARG_CSTRING(1);

	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[2];
	bool nulls[2] = { false };
	bool is_old_version;
	bool is_compatible;

	if (PG_ARGISNULL(1))
		reference_version = TIMESCALEDB_VERSION;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	}

	is_compatible =
		dist_util_is_compatible_version(checked_version, reference_version, &is_old_version);

	values[0] = BoolGetDatum(is_compatible);
	values[1] = BoolGetDatum(is_old_version);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
