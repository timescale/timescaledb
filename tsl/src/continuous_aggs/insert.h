/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INSERT_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INSERT_H

#include <postgres.h>

extern Datum continuous_agg_trigfn(PG_FUNCTION_ARGS);

extern void _continuous_aggs_cache_inval_init();
extern void _continuous_aggs_cache_inval_fini();

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INSERT_H */
