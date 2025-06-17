/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "gapfill_internal.h"

typedef struct GapFillLocfColumnState
{
	GapFillColumnState base;
	Expr *lookup_last;
	Datum value;
	bool isnull;
	bool treat_null_as_missing;
} GapFillLocfColumnState;

void gapfill_locf_initialize(GapFillLocfColumnState *, GapFillState *, FuncExpr *);
void gapfill_locf_group_change(GapFillLocfColumnState *);
void gapfill_locf_tuple_returned(GapFillLocfColumnState *, Datum, bool);
void gapfill_locf_calculate(GapFillLocfColumnState *, GapFillState *, TupleTableSlot *, int64,
							Datum *, bool *);
