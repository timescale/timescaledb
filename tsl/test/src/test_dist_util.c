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
	bool is_compatible;

	if (PG_ARGISNULL(1))
		reference_version = TIMESCALEDB_VERSION;

	is_compatible = dist_util_is_compatible_version(checked_version, reference_version);

	PG_RETURN_BOOL(is_compatible);
}
