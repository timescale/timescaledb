/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_ANALYZE_H
#define TIMESCALEDB_TSL_FDW_ANALYZE_H

#include <postgres.h>

extern int fdw_acquire_sample_rows(Relation relation, Oid serverid, int fetch_size, int elevel,
								   HeapTuple *rows, int targrows, double *totalrows,
								   double *totaldeadrows);

extern bool fdw_analyze_table(Relation relation, Oid serverid, BlockNumber *totalpages);

#endif /* TIMESCALEDB_TSL_FDW_ANALYZE_H */
