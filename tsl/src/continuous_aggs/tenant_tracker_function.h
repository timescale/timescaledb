/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

extern Datum tsl_hypertable_get_tenant_tracking_info(PG_FUNCTION_ARGS);
extern Datum tsl_tenant_tracking_map(PG_FUNCTION_ARGS);
