/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "dist_util.h"

TS_FUNCTION_INFO_V1(ts_test_dist_util_is_access_node_session_on_data_node);

Datum
ts_test_dist_util_is_access_node_session_on_data_node(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(dist_util_is_access_node_session_on_data_node());
}
