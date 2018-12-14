/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#ifndef TIMESCALEDB_GAPFILL_LOCF_H
#define TIMESCALEDB_GAPFILL_LOCF_H

#include <postgres.h>

#include "gapfill/gapfill.h"
#include "gapfill/exec.h"

typedef struct GapFillLocfColumnState
{
	GapFillColumnState base;
	Expr	   *lookup_last;
	Datum		value;
	bool		isnull;
} GapFillLocfColumnState;

void		gapfill_locf_initialize(GapFillLocfColumnState *, GapFillState *, FuncExpr *);
void		gapfill_locf_group_change(GapFillLocfColumnState *);
void		gapfill_locf_tuple_returned(GapFillLocfColumnState *, Datum, bool);
void		gapfill_locf_calculate(GapFillLocfColumnState *, GapFillState *, int64, Datum *, bool *);

#endif							/* TIMESCALEDB_GAPFILL_LOCF_H */
