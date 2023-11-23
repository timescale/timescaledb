/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H
#define TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H

#include <postgres.h>
#include <access/tableam.h>
#include <fmgr.h>
#include <nodes/pathnodes.h>

#include "hypertable.h"

extern const TableAmRoutine *compressionam_routine(void);
extern void compressionam_handler_start_conversion(Oid relid, bool to_other_am);
extern Datum compressionam_handler(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_COMPRESSIONAM_HANDLER_H */
