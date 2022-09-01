/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_HEALTHCHECK_H

#include <postgres.h>
#include <fmgr.h>

extern Datum ts_dist_health_check(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_REMOTE_HEALTHCHECK_H */
