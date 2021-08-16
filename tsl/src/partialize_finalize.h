/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_PARTIALIZE_FINALIZE_H
#define TIMESCALEDB_TSL_PARTIALIZE_FINALIZE_H

#include <postgres.h>
#include <fmgr.h>

#include "compat/compat.h"

#define PARTIALIZE_FUNC_NAME "partialize_agg"

extern Datum tsl_finalize_agg_sfunc(PG_FUNCTION_ARGS);
extern Datum tsl_finalize_agg_ffunc(PG_FUNCTION_ARGS);
extern Datum tsl_partialize_agg(PG_FUNCTION_ARGS);

#endif
