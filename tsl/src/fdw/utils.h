/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_UTILS_H
#define TIMESCALEDB_TSL_FDW_UTILS_H

#include <postgres.h>
#include <funcapi.h>
#include <libpq-fe.h>

#include "remote/data_format.h"

extern int set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);
extern Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern HeapTuple make_tuple_from_result_row(PGresult *res, int row, Relation rel,
											AttConvInMetadata *att_conv_metadata,
											List *retrieved_attrs, ScanState *ss,
											MemoryContext temp_context);

#endif /* TIMESCALEDB_TSL_FDW_UTILS_H */
