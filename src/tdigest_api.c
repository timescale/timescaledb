/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * T-digest-based aggregate and distribution functions
 *
 * Originally from PipelineDB 1.0.0
 * Original copyright (c) 2018, PipelineDB, Inc. and released under Apache License 2.0
 * Modifications copyright by Timescale, Inc. per NOTICE
 */

#include <math.h>

#include "postgres.h"

#include "tdigest.h"
#include "tdigest_api.h"

#include "fmgr.h"
#include "export.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/datum.h"
#include "utils/typcache.h"
#include "compat.h"

/*
 * Creates string representation of TDigest
 */
static const char *
ts_tdigest_print(const TDigest *t)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{ Data Points Added = %u, Compression = %d, Centroid Count: %d, Memory = %dB "
					 "}",
					 t->total_weight,
					 (int) t->compression,
					 t->num_centroids,
					 (int) ts_tdigest_size(t));

	return buf.data;
}

/*
 * Returns string representation of TDigest in a transition struct
 */
TS_FUNCTION_INFO_V1(ts_tdigest_print_sql);
Datum
ts_tdigest_print_sql(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_TEXT_P(CStringGetTextDatum(ts_tdigest_print(t)));
}

/*
 * Sets up a TDigest aggregate from scratch
 */
static TDigestTState *
ts_tdigest_startup(uint32 compression)
{
	TDigestTState *tts;
	TDigest *t;

	tts = palloc0(sizeof(TDigestTState));

	/* create tdigest */
	t = ts_tdigest_create_with_compression(compression);

	/* configure fields */
	tts->td = t;
	tts->unmerged_centroids = NIL;

	return tts;
}

/*
 * Sets up a TDigest aggreate from an existing TDigest data structure
 */
static TDigestTState *
ts_tdigest_startup_with_tdigest(TDigest *t)
{
	TDigestTState *tts = palloc0(sizeof(TDigestTState));

	/* configure fields */
	tts->td = t;
	tts->unmerged_centroids = NIL;

	return tts;
}

/*
 * Compresses buffered data in a TDigest aggregate into the TDigest
 * This is the only API function that should directly call TDigest's compress function
 */
static void
ts_tdigest_trans_compress(TDigestTState *tts)
{
	tts->td = ts_tdigest_compress(tts->td, tts->unmerged_centroids);
	tts->unmerged_centroids = NIL;
}

/*
 * Adds a new value x with weight w to a TDigest aggregate
 */
static void
ts_tdigest_add(TDigestTState *tts, float8 x, int64 w)
{
	Centroid *c;

	c = palloc0(sizeof(Centroid));
	c->weight = w;
	c->mean = x;

	/* add data to input buffer */
	tts->unmerged_centroids = lappend(tts->unmerged_centroids, c);

	/* compress if beyond threshold */
	if (list_length(tts->unmerged_centroids) > tts->td->threshold)
		ts_tdigest_trans_compress(tts);
}

/*
 * Transition function for TDigest aggregate. Starts up aggregate if this is first value being
 * added.
 * Args: TDigestTState, Value to add
 */
TS_FUNCTION_INFO_V1(ts_tdigest_sfunc_sql);
Datum
ts_tdigest_sfunc_sql(PG_FUNCTION_ARGS)
{
	MemoryContext old, context;
	TDigestTState *tts;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "ts_tdigest_sfunc_sql called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		tts = ts_tdigest_startup(TDIGEST_DEFAULT_COMPRESSION);
	else
		tts = (TDigestTState *) PG_GETARG_VARLENA_P(0);

	/* add data to TDigest */
	if (!PG_ARGISNULL(1))
		ts_tdigest_add(tts, PG_GETARG_FLOAT8(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(tts);
}

/*
 * Transition function for TDigest aggregate. Starts up aggregate with provided compression
 * parameter if this is first value being added.
 * Args: TDigestTState, Value to add, Compression
 */
TS_FUNCTION_INFO_V1(ts_tdigest_sfunc_comp_sql);
Datum
ts_tdigest_sfunc_comp_sql(PG_FUNCTION_ARGS)
{
	MemoryContext old, context;
	TDigestTState *tts;
	int32 compression = PG_GETARG_INT32(2);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "ts_tdigest_sfunc_comp_sql called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	/* set up or retrieve tdigest */
	if (PG_ARGISNULL(0))
		tts = ts_tdigest_startup(compression);
	else
		tts = (TDigestTState *) PG_GETARG_VARLENA_P(0);

	/* add element to a tdigest */
	if (!PG_ARGISNULL(1))
		ts_tdigest_add(tts, PG_GETARG_FLOAT8(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(tts);
}

/*
 * Merges two TDigest aggregate transition states
 * Data from second arg is merged into first arg
 */
static void
ts_tdigest_trans_merge(TDigestTState *tts1, TDigestTState *tts2)
{
	int i;
	TDigest *t2;

	/* compress before fetching data to merge in */
	ts_tdigest_trans_compress(tts2);
	t2 = tts2->td;

	for (i = 0; i < t2->num_centroids; i++)
	{
		Centroid *c = &t2->centroids[i];
		ts_tdigest_add(tts1, c->mean, c->weight);
	}
}

/*
 * Merges two TDigest aggregates.
 * Args: TDigestTState, TDigestTState
 */
TS_FUNCTION_INFO_V1(ts_tdigest_combinefunc_sql);
Datum
ts_tdigest_combinefunc_sql(PG_FUNCTION_ARGS)
{
	MemoryContext old, context;
	TDigestTState *receiver_tts, *incoming_tts;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "ts_tdigest_combinefunc called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(
			0)) /* compress unmerged centroids and reallocate tdigest in correct memory context */
	{
		incoming_tts = (TDigestTState *) PG_GETARG_VARLENA_P(1);
		ts_tdigest_trans_compress(incoming_tts);

		receiver_tts = ts_tdigest_startup_with_tdigest(ts_tdigest_copy(incoming_tts->td));
	}
	else if (PG_ARGISNULL(1)) /* if nothing to merge in, merged value is current value */
	{
		receiver_tts = (TDigestTState *) PG_GETARG_VARLENA_P(0);
	}
	else /* merge states if both are non-NULL */
	{
		receiver_tts = (TDigestTState *) PG_GETARG_VARLENA_P(0);
		incoming_tts = (TDigestTState *) PG_GETARG_VARLENA_P(1);

		ts_tdigest_trans_merge(receiver_tts, incoming_tts);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(receiver_tts);
}

/*
 * Final function for aggregate
 * Returns: TDigest
 */
TS_FUNCTION_INFO_V1(ts_tdigest_finalfunc_sql);
Datum
ts_tdigest_finalfunc_sql(PG_FUNCTION_ARGS)
{
	TDigestTState *tts;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_tdigest_final_sql called in non-aggregate context");
	}

	/* use getarg_pointer because the transition state is a pointer, only
	 * the internal TDigest is TOAST'ed */
	tts = PG_ARGISNULL(0) ? NULL : (TDigestTState *) PG_GETARG_POINTER(0);

	if (tts == NULL || tts->td == NULL)
		PG_RETURN_NULL();

	/* compress before returning to ensure all centroids merged into tdigest */
	ts_tdigest_trans_compress(tts);

	PG_RETURN_POINTER(tts->td);
}

/********************
 * CLIENT FUNCTIONS *
 ********************/

/*
 * Returns the approximate value for a percentile using a TDigest
 * Args: TDigest structure, Percentile
 */
TS_FUNCTION_INFO_V1(ts_tdigest_percentile_sql);
Datum
ts_tdigest_percentile_sql(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 q;

	t = (TDigest *) PG_GETARG_VARLENA_P(0);
	q = PG_GETARG_FLOAT8(1);

	if (isnan(q))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("percentile parameter cannot be NaN")));

	PG_RETURN_FLOAT8(ts_tdigest_percentile(t, q));
}

/*
 * Returns the approximate value of the CDF using a TDigest
 * Args: TDigest stucture, CDF value
 */
TS_FUNCTION_INFO_V1(ts_tdigest_cdf_sql);
Datum
ts_tdigest_cdf_sql(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 x;

	t = (TDigest *) PG_GETARG_VARLENA_P(0);
	x = PG_GETARG_FLOAT8(1);

	if (isnan(x))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cdf parameter cannot be NaN")));

	PG_RETURN_FLOAT8(ts_tdigest_cdf(t, x));
}

/*
 * Returns the total number of data points put into this TDigest thus far
 * Avoids user needing to call count() on a dataset they've already put into a TDigest
 */
TS_FUNCTION_INFO_V1(ts_tdigest_count_sql);
Datum
ts_tdigest_count_sql(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT32(ts_tdigest_count(t));
}

/***************
 * TDIGEST I/O *
 ***************/

/*
 * Converts a tdigest to a portable binary representation
 */
static bytea *
tdigest_send(StringInfo buf, const TDigest *t)
{
	int i;

	pq_begintypsend(buf);

	/* send memory footprint of t-digest */
	pq_sendint32(buf, VARSIZE(t));

	/* send the tdigest single value fields */
	pq_sendfloat8(buf, t->compression);
	pq_sendint32(buf, t->threshold);
	pq_sendint32(buf, t->max_centroids);

	pq_sendint32(buf, t->total_weight);
	pq_sendfloat8(buf, t->min);
	pq_sendfloat8(buf, t->max);

	pq_sendint32(buf, t->num_centroids);

	/* send centroid array */
	for (i = 0; i < t->num_centroids; i++)
	{
		pq_sendint32(buf, t->centroids[i].weight);
		pq_sendfloat8(buf, t->centroids[i].mean);
	}

	return pq_endtypsend(buf);
}

/*
 * Converts a portable binary representation to a tdigest
 */
static TDigest *
tdigest_recv(StringInfo buf)
{
	int i;
	uint32 size;
	TDigest *t;

	/* read in the memory footprint of the tdigest and allocate appropriate memory amount */
	size = pq_getmsgint32(buf);
	t = palloc0(size);
	SET_VARSIZE(t, size);

	/* receive tdigest single value fields */
	t->compression = pq_getmsgfloat8(buf);
	t->threshold = pq_getmsgint32(buf);
	t->max_centroids = pq_getmsgint32(buf);

	t->total_weight = pq_getmsgint32(buf);
	t->min = pq_getmsgfloat8(buf);
	t->max = pq_getmsgfloat8(buf);

	t->num_centroids = pq_getmsgint32(buf);

	/* receive centroid array */
	for (i = 0; i < t->num_centroids; i++)
	{
		t->centroids[i].weight = pq_getmsgint32(buf);
		t->centroids[i].mean = pq_getmsgfloat8(buf);
	}

	return t;
}

/*
 * Unwraps then serializes a TDigest
 */
TS_FUNCTION_INFO_V1(ts_tdigest_serializefunc_sql);
Datum
ts_tdigest_serializefunc_sql(PG_FUNCTION_ARGS)
{
	TDigestTState *tts;
	StringInfoData buffer;

	tts = (TDigestTState *) PG_GETARG_VARLENA_P(0);
	/* compress unmerged before serializing tdigest */
	ts_tdigest_trans_compress(tts);

	PG_RETURN_BYTEA_P(tdigest_send(&buffer, tts->td));
}

/*
 * Reads in byte representation of TDigest, then wraps it in a TDigestTState struct for future
 * operations
 */
TS_FUNCTION_INFO_V1(ts_tdigest_deserializefunc_sql);
Datum
ts_tdigest_deserializefunc_sql(PG_FUNCTION_ARGS)
{
	TDigestTState *tts;
	TDigest *t;
	StringInfoData buf;
	bytea *input;

	/* receive raw tdigest */
	input = PG_GETARG_BYTEA_P(0);
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(input), VARSIZE(input) - VARHDRSZ);

	/* deserialize tdigest */
	t = tdigest_recv(&buf);

	/* configure aggregate */
	tts = ts_tdigest_startup_with_tdigest(t);

	PG_RETURN_POINTER(tts);
}

/*
 * Re-create TDigest from string representation
 */
TS_FUNCTION_INFO_V1(ts_tdigest_in_sql);
Datum
ts_tdigest_in_sql(PG_FUNCTION_ARGS)
{
	StringInfoData buffer;
	TDigest *t;
	bytea *input;

	/* read in raw TDigest binary string */
	input = (bytea *) DirectFunctionCall1(byteain, (Datum) PG_GETARG_CSTRING(0));
	initStringInfo(&buffer);
	appendBinaryStringInfo(&buffer, VARDATA(input), VARSIZE(input) - VARHDRSZ);

	t = tdigest_recv(&buffer);

	PG_RETURN_POINTER(t);
}

/*
 * Output TDigest in string format
 */
TS_FUNCTION_INFO_V1(ts_tdigest_out_sql);
Datum
ts_tdigest_out_sql(PG_FUNCTION_ARGS)
{
	char *s;
	TDigest *t;
	StringInfoData buffer;

	t = (TDigest *) PG_GETARG_VARLENA_P(0);

	/* convert TDigest to binary and output as binary, likely non-printable string */
	s = (char *) DirectFunctionCall1(byteaout, (Datum) tdigest_send(&buffer, t));

	PG_RETURN_CSTRING(s);
}

/*
 * Output TDigest in binary format
 */
TS_FUNCTION_INFO_V1(ts_tdigest_send_sql);
TSDLLEXPORT Datum
ts_tdigest_send_sql(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	TDigest *t;
	bytea *result;

	/* send the tdigest struct */
	t = (TDigest *) PG_GETARG_VARLENA_P(0);
	tdigest_send(&buf, t);
	result = (bytea *) buf.data;

	PG_RETURN_BYTEA_P(result);
}

/*
 * Reconstruct TDigest from binary representation stored in a StringInfo
 */
TS_FUNCTION_INFO_V1(ts_tdigest_recv_sql);
TSDLLEXPORT Datum
ts_tdigest_recv_sql(PG_FUNCTION_ARGS)
{
	TDigest *t;
	StringInfo buf;

	/* recveive tdigest */
	buf = (StringInfo) PG_GETARG_POINTER(0);

	/* deserialize tdigest */
	t = tdigest_recv(buf);

	PG_RETURN_POINTER(t);
}

/****************************************
 * TDigest Comparison Wrapper Functions *
 ****************************************/

/*
 * Compares TDigest in the total order
 */
TS_FUNCTION_INFO_V1(ts_tdigest_cmp_sql);
Datum
ts_tdigest_cmp_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_INT32(ts_tdigest_cmp(t1, t2));
}

/*
 * Determines if two TDigest's are equal in the total order
 */
TS_FUNCTION_INFO_V1(ts_tdigest_equal_sql);
Datum
ts_tdigest_equal_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(ts_tdigest_equal(t1, t2));
}

/*
 * Determine if one TDigest < another
 */
TS_FUNCTION_INFO_V1(ts_tdigest_lt_sql);
Datum
ts_tdigest_lt_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(ts_tdigest_lt(t1, t2));
}

/*
 * Determine if one TDigest > another
 */
TS_FUNCTION_INFO_V1(ts_tdigest_gt_sql);
Datum
ts_tdigest_gt_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(ts_tdigest_gt(t1, t2));
}

/*
 * Determine if one TDigest >= another
 */
TS_FUNCTION_INFO_V1(ts_tdigest_ge_sql);
Datum
ts_tdigest_ge_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(ts_tdigest_ge(t1, t2));
}

/*
 * Determine if one TDigest <= another
 */
TS_FUNCTION_INFO_V1(ts_tdigest_le_sql);
Datum
ts_tdigest_le_sql(PG_FUNCTION_ARGS)
{
	TDigest *t1, *t2;

	t1 = (TDigest *) PG_GETARG_VARLENA_P(0);
	t2 = (TDigest *) PG_GETARG_VARLENA_P(1);

	PG_RETURN_BOOL(ts_tdigest_le(t1, t2));
}
