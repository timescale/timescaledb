/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_LOCF_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_LOCF_H

#include <postgres.h>

#include "nodes/gapfill/gapfill.h"
#include "nodes/gapfill/exec.h"

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
void gapfill_locf_calculate(GapFillLocfColumnState *, GapFillState *, int64, Datum *, bool *);

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_LOCF_H */
