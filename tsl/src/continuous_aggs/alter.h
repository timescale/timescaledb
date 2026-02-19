/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

extern Datum continuous_agg_add_column(PG_FUNCTION_ARGS);
extern Datum continuous_agg_drop_column(PG_FUNCTION_ARGS);
