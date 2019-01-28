/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <utils/datum.h>
#include <utils/typcache.h>

#include "compat.h"
#include "gapfill/interpolate.h"
#include "gapfill/exec.h"

#define INTERPOLATE(x,x0,x1,y0,y1) (((y0) * ((x1) - (x)) + (y1) * ((x) - (x0))) / ((x1) - (x0)))

/*
 * gapfill_interpolate_initialize gets called when plan is initialized for every interpolate column
 */
void
gapfill_interpolate_initialize(GapFillInterpolateColumnState *interpolate, GapFillState *state, FuncExpr *function)
{
	interpolate->prev.isnull = true;
	interpolate->next.isnull = true;
	if (list_length(((FuncExpr *) function)->args) > 1)
		interpolate->lookup_before = gapfill_adjust_varnos(state, lsecond(((FuncExpr *) function)->args));
	if (list_length(((FuncExpr *) function)->args) > 2)
		interpolate->lookup_after = gapfill_adjust_varnos(state, lthird(((FuncExpr *) function)->args));
}

/*
 * gapfill_interpolate_group_change gets called when a new aggregation group becomes active
 */
void
gapfill_interpolate_group_change(GapFillInterpolateColumnState *column, int64 time, Datum value, bool isnull)
{
	column->prev.isnull = true;
	column->next.isnull = isnull;
	if (!isnull)
	{
		column->next.time = time;
		column->next.value = datumCopy(value, column->base.typbyval, column->base.typlen);
	}
}

/*
 * gapfill_interpolate_tuple_fetched gets called when a new tuple is fetched from subplan
 */
void
gapfill_interpolate_tuple_fetched(GapFillInterpolateColumnState *column, int64 time, Datum value, bool isnull)
{
	column->next.isnull = isnull;
	if (!isnull)
	{
		column->next.time = time;
		column->next.value = datumCopy(value, column->base.typbyval, column->base.typlen);
	}
}

/*
 * gapfill_interpolate_tuple_returned gets called when subplan tuple is returned
 */
void
gapfill_interpolate_tuple_returned(GapFillInterpolateColumnState *column, int64 time, Datum value, bool isnull)
{
	column->next.isnull = true;
	column->prev.isnull = isnull;
	if (!isnull)
	{
		column->prev.time = time;
		column->prev.value = datumCopy(value, column->base.typbyval, column->base.typlen);
	}
}

/*
 * Do out of bounds lookup for interpolation
 */
static void
gapfill_fetch_sample(GapFillState *state, GapFillInterpolateColumnState *column, GapFillInterpolateSample *sample, Expr *lookup)
{
	HeapTupleHeader th;
	HeapTupleData tuple;
	TupleDesc	tupdesc;
	Datum		value;
	bool		isnull;
	Datum		datum = gapfill_exec_expr(state, lookup, &isnull);

	if (isnull)
	{
		sample->isnull = true;
		return;
	}

	th = DatumGetHeapTupleHeader(datum);
	if (HeapTupleHeaderGetNatts(th) != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interpolate RECORD arguments must have 2 elements")));

	/* Extract type information from the tuple itself */
	Assert(RECORDOID == HeapTupleHeaderGetTypeId(th));
	tupdesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(th),
									 HeapTupleHeaderGetTypMod(th));

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(th);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = th;

	/* check first element in record matches timestamp datatype */
	if (TupleDescAttr(tupdesc, 0)->atttypid != state->columns[state->time_index]->typid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("first argument of interpolate returned record must match used timestamp datatype")));

	/* check second element in record matches interpolate datatype */
	if (TupleDescAttr(tupdesc, 1)->atttypid != column->base.typid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("second argument of interpolate returned record must match used interpolate datatype")));

	value = heap_getattr(&tuple, 1, tupdesc, &sample->isnull);
	if (!sample->isnull)
	{
		sample->time = gapfill_datum_get_internal(value, state->gapfill_typid);

		value = heap_getattr(&tuple, 2, tupdesc, &sample->isnull);
		if (!sample->isnull)
			sample->value = datumCopy(value, column->base.typbyval, column->base.typlen);
	}

	DecrTupleDescRefCount(tupdesc);
}

/*
 * gapfill_interpolate_calculate gets called for every gapfilled tuple to calculate values
 *
 * Calculate linear interpolation value
 * y = (y0(x1-x) + y1(x-x0))/(x1-x0)
 */
void
gapfill_interpolate_calculate(GapFillInterpolateColumnState *column, GapFillState *state, int64 time, Datum *value, bool *isnull)
{
	int64		x,
				x0,
				x1;
	Datum		y0,
				y1;

	/* only evaluate expr for first tuple */
	if (column->prev.isnull && column->lookup_before && time == state->gapfill_start)
		gapfill_fetch_sample(state, column, &column->prev, column->lookup_before);

	if (column->next.isnull && column->lookup_after && (FETCHED_LAST == state->state || FETCHED_NEXT_GROUP == state->state))
		gapfill_fetch_sample(state, column, &column->next, column->lookup_after);

	*isnull = column->prev.isnull || column->next.isnull;
	if (*isnull)
		return;

	y0 = column->prev.value;
	y1 = column->next.value;

	x  = time;
	x0 = column->prev.time;
	x1 = column->next.time;

	switch (column->base.typid)
	{
		case INT2OID:
			*value = Int16GetDatum(INTERPOLATE(x, x0, x1, DatumGetInt16(y0), DatumGetInt16(y1)));
			break;
		case INT4OID:
			*value = Int32GetDatum(INTERPOLATE(x, x0, x1, DatumGetInt32(y0), DatumGetInt32(y1)));
			break;
		case INT8OID:
			*value = Int64GetDatum(INTERPOLATE(x, x0, x1, DatumGetInt64(y0), DatumGetInt64(y1)));
			break;
		case FLOAT4OID:
			*value = Float4GetDatum(INTERPOLATE(x, x0, x1, DatumGetFloat4(y0), DatumGetFloat4(y1)));
			break;
		case FLOAT8OID:
			*value = Float8GetDatum(INTERPOLATE(x, x0, x1, DatumGetFloat8(y0), DatumGetFloat8(y1)));
			break;
		default:

			/*
			 * should never happen since interpolate is not defined for other
			 * datatypes
			 */
			Assert(false);
			break;
	}
}
