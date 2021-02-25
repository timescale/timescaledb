/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DIST_BACKUP_H
#define TIMESCALEDB_TSL_DIST_BACKUP_H

#include <postgres.h>

extern Datum create_distributed_restore_point(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_DIST_BACKUP_H */
