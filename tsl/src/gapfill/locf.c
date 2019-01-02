/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/datum.h>

#include "gapfill/exec.h"
#include "gapfill/locf.h"

/*
 * gapfill_locf_initialize gets called when plan is initialized for every locf column
 */
void
gapfill_locf_initialize(GapFillLocfColumnState *locf, GapFillState *state, FuncExpr *function)
{
	locf->isnull = true;

	if (list_length(function->args) > 1)
		locf->lookup_last = gapfill_adjust_varnos(state, lsecond(function->args));
}

/*
 * gapfill_locf_group_change gets called when a new aggregation group becomes active
 */
void
gapfill_locf_group_change(GapFillLocfColumnState *locf)
{
	locf->isnull = true;
}

/*
 * gapfill_locf_tuple_returned gets called when subplan tuple is returned
 */
void
gapfill_locf_tuple_returned(GapFillLocfColumnState *locf, Datum value, bool isnull)
{
	locf->isnull = isnull;
	if (!isnull)
		locf->value = datumCopy(value, locf->base.typbyval, locf->base.typlen);
}

/*
 * gapfill_locf_calculate gets called for every gapfilled tuple to calculate values
 */
void
gapfill_locf_calculate(GapFillLocfColumnState *locf, GapFillState *state, int64 time, Datum *value, bool *isnull)
{
	/* only evaluate expr for first tuple */
	if (locf->isnull && locf->lookup_last && time == state->gapfill_start)
		locf->value = gapfill_exec_expr(state, locf->lookup_last, &locf->isnull);

	*value = locf->value;
	*isnull = locf->isnull;
}
