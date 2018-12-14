/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef TIMESCALEDB_GAPFILL_INTERPOLATE_H
#define TIMESCALEDB_GAPFILL_INTERPOLATE_H

#include "gapfill/exec.h"

typedef struct GapFillInterpolateSample
{
	Datum		time;
	Datum		value;
	bool		isnull;
} GapFillInterpolateSample;

typedef struct GapFillInterpolateColumnState
{
	GapFillColumnState base;
	Expr	   *lookup_before;
	Expr	   *lookup_after;
	GapFillInterpolateSample prev;
	GapFillInterpolateSample next;
} GapFillInterpolateColumnState;

void		gapfill_interpolate_initialize(GapFillInterpolateColumnState *, GapFillState *, FuncExpr *);
void		gapfill_interpolate_group_change(GapFillInterpolateColumnState *, int64, Datum, bool);
void		gapfill_interpolate_tuple_fetched(GapFillInterpolateColumnState *, int64, Datum, bool);
void		gapfill_interpolate_tuple_returned(GapFillInterpolateColumnState *, int64, Datum, bool);
void		gapfill_interpolate_calculate(GapFillInterpolateColumnState *, GapFillState *, int64, Datum *, bool *);

#endif							/* TIMESCALEDB_GAPFILL_INTERPOLATE_H */
