/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include "export.h"

extern TSDLLEXPORT Datum ts_invalidation_read_record(PG_FUNCTION_ARGS);
