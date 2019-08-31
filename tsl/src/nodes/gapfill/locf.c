/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/datum.h>

#include "nodes/gapfill/exec.h"
#include "nodes/gapfill/locf.h"

/*
 * gapfill_locf_initialize gets called when plan is initialized for every locf column
 */
void
gapfill_locf_initialize(GapFillLocfColumnState *locf, GapFillState *state, FuncExpr *function)
{
	locf->isnull = true;

	/* check if out of boundary lookup expression was supplied */
	if (list_length(function->args) > 1)
		locf->lookup_last = gapfill_adjust_varnos(state, lsecond(function->args));

	/* check if treat_null_as_missing was supplied */
	if (list_length(function->args) > 2)
	{
		Const *treat_null_as_missing = lthird(function->args);
		if (!IsA(treat_null_as_missing, Const) || treat_null_as_missing->consttype != BOOLOID)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "invalid locf argument: treat_null_as_missing must be a BOOL literal")));
		if (!treat_null_as_missing->constisnull)
			locf->treat_null_as_missing = DatumGetBool(treat_null_as_missing->constvalue);
	}
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
gapfill_locf_calculate(GapFillLocfColumnState *locf, GapFillState *state, int64 time, Datum *value,
					   bool *isnull)
{
	/* only evaluate expr for first tuple */
	if (locf->isnull && locf->lookup_last && time == state->gapfill_start)
		locf->value = gapfill_exec_expr(state, locf->lookup_last, &locf->isnull);

	*value = locf->value;
	*isnull = locf->isnull;
}
