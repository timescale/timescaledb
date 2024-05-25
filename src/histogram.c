/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <libpq/pqformat.h>
#include <netinet/in.h>
#include <nodes/makefuncs.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

#include "compat/compat.h"
#include "debug_assert.h"
#include "utils.h"

/* aggregate histogram:
 *	 histogram(state, val, min, max, nbuckets) returns the histogram array with nbuckets
 *
 * Usage:
 *	 SELECT grouping_element, histogram(field, min, max, nbuckets) FROM table GROUP BY
 *grouping_element.
 *
 * Description:
 * Histogram generates a histogram array based off of a specified range passed into the function.
 * Values falling outside of this range are bucketed into the 0 or nbucket+1 buckets depending on
 * if they are below or above the range, respectively. The resultant histogram therefore contains
 * nbucket+2 buckets accounting for buckets outside the range.
 */

TS_FUNCTION_INFO_V1(ts_hist_sfunc);
TS_FUNCTION_INFO_V1(ts_hist_combinefunc);
TS_FUNCTION_INFO_V1(ts_hist_serializefunc);
TS_FUNCTION_INFO_V1(ts_hist_deserializefunc);
TS_FUNCTION_INFO_V1(ts_hist_finalfunc);

#define HISTOGRAM_SIZE(state, nbuckets) (sizeof(*(state)) + (nbuckets) * sizeof(*(state)->buckets))

typedef struct Histogram
{
	int32 nbuckets;
	Datum buckets[FLEXIBLE_ARRAY_MEMBER];
} Histogram;

/* histogram(state, val, min, max, nbuckets) */
Datum
ts_hist_sfunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	Histogram *state = (Histogram *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	Datum val_datum = PG_GETARG_DATUM(1);
	Datum min_datum = PG_GETARG_DATUM(2);
	Datum max_datum = PG_GETARG_DATUM(3);
	double min = DatumGetFloat8(min_datum);
	double max = DatumGetFloat8(max_datum);
	int nbuckets;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_hist_sfunc called in non-aggregate context");
	}

	if (min > max)
	{
		/* cannot generate a histogram with incompatible bounds */
		elog(ERROR, "lower bound cannot exceed upper bound");
	}

	if (state == NULL)
	{
		nbuckets = PG_GETARG_INT32(4) + 2;
		/* Allocate memory to a new histogram state array */
		state = MemoryContextAllocZero(aggcontext, HISTOGRAM_SIZE(state, nbuckets));
		state->nbuckets = nbuckets;
	}

	/* Since the number of buckets is an argument to the calls it might differ
	 * from the number we initialized with so we need to make sure we check
	 * against what we actually have.
	 */
	nbuckets = state->nbuckets - 2;
	if (nbuckets != PG_GETARG_INT32(4))
		elog(ERROR, "number of buckets must not change between calls");

	int32 bucket = DatumGetInt32(DirectFunctionCall4(width_bucket_float8,
													 val_datum,
													 min_datum,
													 max_datum,
													 Int32GetDatum(nbuckets)));

	/* Increment the proper histogram bucket */
	if (bucket < 0 || bucket >= state->nbuckets)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("index %d from \"width_bucket\" out of range", bucket),
				 errhint("You probably have a floating point overflow.")));
	if (DatumGetInt32(state->buckets[bucket]) >= PG_INT32_MAX - 1)
		elog(ERROR, "overflow in histogram");

	state->buckets[bucket] = Int32GetDatum(DatumGetInt32(state->buckets[bucket]) + 1);

	PG_RETURN_POINTER(state);
}

/* Make a copy of the histogram state */
static inline Histogram *
copy_state(MemoryContext aggcontext, Histogram *state)
{
	Histogram *copy;
	Size bucket_bytes = state->nbuckets * sizeof(*copy->buckets);

	copy = MemoryContextAlloc(aggcontext, sizeof(*copy) + bucket_bytes);
	copy->nbuckets = state->nbuckets;
	memcpy(copy->buckets, state->buckets, bucket_bytes);

	return copy;
}

/* ts_hist_combinefunc(internal, internal) => internal */
Datum
ts_hist_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;

	Histogram *state1 = (Histogram *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
	Histogram *state2 = (Histogram *) (PG_ARGISNULL(1) ? NULL : PG_GETARG_POINTER(1));
	Histogram *result;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_hist_combinefunc called in non-aggregate context");
	}

	if (state1 == NULL && state2 == NULL)
	{
		PG_RETURN_NULL();
	}
	else if (state2 == NULL)
	{
		result = copy_state(aggcontext, state1);
	}
	else if (state1 == NULL)
	{
		result = copy_state(aggcontext, state2);
	}
	else
	{
		/* Since number of buckets is part of the aggregation call the initialization
		 * might be different in the partials so we error out if they are not identical. */
		if (state1->nbuckets != state2->nbuckets)
			elog(ERROR, "number of buckets must not change between calls");

		result = copy_state(aggcontext, state1);

		/* Combine values from state1 and state2 when both states are non-null */
		for (int32 i = 0; i < state1->nbuckets; i++)
		{
			/* Perform addition using int64 to check for overflow */
			int64 val = (int64) DatumGetInt32(result->buckets[i]);
			int64 other = (int64) DatumGetInt32(state2->buckets[i]);
			if (val + other >= PG_INT32_MAX)
				elog(ERROR, "overflow in histogram combine");

			result->buckets[i] = Int32GetDatum((int32) (val + other));
		}
	}

	PG_RETURN_POINTER(result);
}

/* ts_hist_serializefunc(internal) => bytea */
Datum
ts_hist_serializefunc(PG_FUNCTION_ARGS)
{
	Histogram *state;
	StringInfoData buf;

	Assert(!PG_ARGISNULL(0));
	state = (Histogram *) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);
	pq_sendint32(&buf, state->nbuckets);

	for (int32 i = 0; i < state->nbuckets; i++)
		pq_sendint32(&buf, DatumGetInt32(state->buckets[i]));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ts_hist_deserializefunc(bytea *, internal) => internal */
Datum
ts_hist_deserializefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	bytea *serialized;
	int32 nbuckets;
	int32 i;
	StringInfoData buf;
	Histogram *state;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "ts_hist_deserializefunc called in non-aggregate context");

	Assert(!PG_ARGISNULL(0));
	serialized = PG_GETARG_BYTEA_P(0);

	buf.data = VARDATA(serialized);
	buf.len = VARSIZE(serialized) - VARHDRSZ;
	buf.maxlen = VARSIZE(serialized) - VARHDRSZ;
	buf.cursor = 0; /* used by pq_getmsgint*/

	nbuckets = pq_getmsgint(&buf, 4);

	state = MemoryContextAllocZero(aggcontext, HISTOGRAM_SIZE(state, nbuckets));
	state->nbuckets = nbuckets;

	for (i = 0; i < state->nbuckets; i++)
		state->buckets[i] = Int32GetDatum(pq_getmsgint(&buf, 4));

	PG_RETURN_POINTER(state);
}

/* hist_finalfunc(internal, val REAL, MIN REAL, MAX REAL, nbuckets INTEGER) => INTEGER[] */
Datum
ts_hist_finalfunc(PG_FUNCTION_ARGS)
{
	Histogram *state;
	int dims[1];
	int lbs[1];

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_hist_finalfunc called in non-aggregate context");
	}

	state = (Histogram *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

	if (state == NULL)
		PG_RETURN_NULL();

	dims[0] = state->nbuckets;
	lbs[0] = 1;

	PG_RETURN_ARRAYTYPE_P(
		construct_md_array(state->buckets, NULL, 1, dims, lbs, INT4OID, 4, true, 'i'));
}
