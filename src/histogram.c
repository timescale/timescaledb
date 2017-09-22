#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include <netinet/in.h>

#define HIST_LEN(state) ((VARSIZE(state) - VARHDRSZ ) / sizeof(Datum))

/* aggregate histogram:
 *	 histogram(state, val, min, max, nbuckets) returns the histogram array with nbuckets
 *
 * Usage:
 *	 SELECT grouping_element, histogram(field, min, max, nbuckets) FROM table GROUP BY grouping_element.
 *
 * Description:
 * Histogram generates a histogram array based off of a specified range passed into the function.
 * Values falling outside of this range are bucketed into the 0 or nbucket+1 buckets depending on
 * if they are below or above the range, respectively. The resultant histogram therefore contains
 * nbucket+2 buckets accounting for buckets outside the range.
 */

PGDLLEXPORT Datum hist_sfunc(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hist_combinefunc(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hist_serializefunc(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hist_deserializefunc(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum hist_finalfunc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hist_sfunc);
PG_FUNCTION_INFO_V1(hist_combinefunc);
PG_FUNCTION_INFO_V1(hist_serializefunc);
PG_FUNCTION_INFO_V1(hist_deserializefunc);
PG_FUNCTION_INFO_V1(hist_finalfunc);

/* histogram(state, val, min, max, nbuckets) */
Datum
hist_sfunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	bytea	   *state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	Datum	   *hist;

	double		val = PG_GETARG_FLOAT8(1);
	double		min = PG_GETARG_FLOAT8(2);
	double		max = PG_GETARG_FLOAT8(3);
	int			nbuckets = PG_GETARG_INT32(4);

	int			bucket = DirectFunctionCall4(width_bucket_float8, val, min, max, nbuckets);

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "hist_sfunc called in non-aggregate context");
	}

	if (min > max)
	{
		/* cannot generate a histogram with incompatible bounds */
		elog(ERROR, "lower bound cannot exceed upper bound");
	}

	if (state == NULL)
	{
		/* Allocate memory to a new histogram state array */
		Size		arrsize = sizeof(Datum) * (nbuckets + 2);

		state = MemoryContextAllocZero(aggcontext, VARHDRSZ + arrsize);
		SET_VARSIZE(state, VARHDRSZ + arrsize);
	}

	/* Increment the proper histogram bucket */
	hist = (Datum *) VARDATA(state);
	hist[bucket] = Int32GetDatum(DatumGetInt32(hist[bucket]) + 1);

	PG_RETURN_BYTEA_P(state);
}

/* Make a copy of the bytea state */
static inline bytea *
copy_state(MemoryContext aggcontext, bytea *state)
{
	bytea	   *copy;
	Size		arrsize = VARSIZE(state) - VARHDRSZ;

	copy = MemoryContextAllocZero(aggcontext, VARHDRSZ + arrsize);
	SET_VARSIZE(copy, VARHDRSZ + arrsize);
	memcpy(copy, state, VARHDRSZ + arrsize);

	return copy;
}

/* hist_combinefunc(internal, internal) => internal */
Datum
hist_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;

	bytea	   *state1 = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));
	bytea	   *state2 = (PG_ARGISNULL(1) ? NULL : PG_GETARG_BYTEA_P(1));
	bytea	   *result;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "hist_combinefunc called in non-aggregate context");
	}

	if (state2 == NULL)
	{
		result = copy_state(aggcontext, state1);
	}

	else if (state1 == NULL)
	{
		result = copy_state(aggcontext, state2);
	}

	else
	{
		Size		i;
		Datum	   *hist,
				   *hist_other;

		result = copy_state(aggcontext, state1);
		hist = (Datum *) VARDATA(result);
		hist_other = (Datum *) VARDATA(state2);

		/* Combine values from state1 and state2 when both states are non-null */
		for (i = 0; i < HIST_LEN(state1); i++)
			hist[i] = Int32GetDatum(DatumGetInt32(hist[i]) + DatumGetInt32(hist_other[i]));
	}

	PG_RETURN_BYTEA_P(result);
}

/* hist_serializefunc(internal) => bytea */
Datum
hist_serializefunc(PG_FUNCTION_ARGS)
{
	bytea	   *state;
	Datum	   *hist;
	Size		i;

	Assert(!PG_ARGISNULL(0));
	state = PG_GETARG_BYTEA_P(0);
	hist = (Datum *) VARDATA(state);

	for (i = 0; i < HIST_LEN(state); i++)
	{
		hist[i] = Int32GetDatum(htonl(DatumGetInt32(hist[i])));
	}

	PG_RETURN_BYTEA_P(state);
}

/* hist_deserializefunc(bytea, internal) => internal */
Datum
hist_deserializefunc(PG_FUNCTION_ARGS)
{
	bytea	   *state;
	Datum	   *hist;
	Size		i;

	Assert(!PG_ARGISNULL(0));
	state = PG_GETARG_BYTEA_P(0);
	hist = (Datum *) VARDATA(state);

	for (i = 0; i < HIST_LEN(state); i++)
	{
		hist[i] = Int32GetDatum(ntohl(DatumGetInt32(hist[i])));
	}

	PG_RETURN_BYTEA_P(state);
}

/* hist_funalfunc(internal, val REAL, MIN REAL, MAX REAL, nbuckets INTEGER) => INTEGER[] */
Datum
hist_finalfunc(PG_FUNCTION_ARGS)
{
	bytea	   *state;
	Datum	   *hist;
	int			dims[1];
	int			lbs[1];

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "hist_finalfunc called in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = PG_GETARG_BYTEA_P(0);
	hist = (Datum *) VARDATA(state);

	dims[0] = HIST_LEN(state);
	lbs[0] = 1;

	PG_RETURN_ARRAYTYPE_P(construct_md_array(hist, NULL, 1, dims, lbs, INT4OID, 4, true, 'i'));
}
