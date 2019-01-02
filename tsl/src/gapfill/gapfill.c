/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>

#include "gapfill/gapfill.h"

Datum
gapfill_marker(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s can only be used in an aggregation query with time_bucket_gapfill", get_func_name(fcinfo->flinfo->fn_oid))));
	pg_unreachable();
}
