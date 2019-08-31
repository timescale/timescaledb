/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_INTERPOLATE_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_INTERPOLATE_H

#include "nodes/gapfill/exec.h"

typedef struct GapFillInterpolateSample
{
	int64 time;
	Datum value;
	bool isnull;
} GapFillInterpolateSample;

typedef struct GapFillInterpolateColumnState
{
	GapFillColumnState base;
	Expr *lookup_before;
	Expr *lookup_after;
	GapFillInterpolateSample prev;
	GapFillInterpolateSample next;
} GapFillInterpolateColumnState;

void gapfill_interpolate_initialize(GapFillInterpolateColumnState *, GapFillState *, FuncExpr *);
void gapfill_interpolate_group_change(GapFillInterpolateColumnState *, int64, Datum, bool);
void gapfill_interpolate_tuple_fetched(GapFillInterpolateColumnState *, int64, Datum, bool);
void gapfill_interpolate_tuple_returned(GapFillInterpolateColumnState *, int64, Datum, bool);
void gapfill_interpolate_calculate(GapFillInterpolateColumnState *, GapFillState *, int64, Datum *,
								   bool *);

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_INTERPOLATE_H */
