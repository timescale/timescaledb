/*-------------------------------------------------------------------------
 *
 * distfucns.c
 *		t-digest-based distribution functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distfuncs.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "tdigest.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/datum.h"
#include "utils/typcache.h"

/*
 * tdigest_compress
 */
PG_FUNCTION_INFO_V1(tdigest_compress);
Datum
tdigest_compress(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);
	MemoryContext old;
	MemoryContext context;
	bool in_agg_cxt = fcinfo->context && (IsA(fcinfo->context, AggState) || IsA(fcinfo->context, WindowAggState));

	if (in_agg_cxt)
	{
		if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "dist_compress called in non-aggregate context");

		old = MemoryContextSwitchTo(context);
	}

	t = TDigestCompress(t);

	if (in_agg_cxt)
		MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(t);
}

/*
 * tdigest_print
 */
PG_FUNCTION_INFO_V1(tdigest_print);
Datum
tdigest_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	TDigest *t;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	t = (TDigest *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ count = %ld, k = %d, centroids: %d, size = %dkB }",
			t->total_weight, (int) t->compression, t->num_centroids, (int) TDigestSize(t) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
}

/*
 * tdigest_create
 */
static TDigest *
tdigest_create(uint32_t k)
{
	if (k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("k must be non-zero")));
	return TDigestCreateWithCompression(k);
}

/*
 * tdigest_startup
 */
static TDigest *
tdigest_startup(FunctionCallInfo fcinfo, uint32_t k)
{
	TDigest *t;

	if (k)
		t = tdigest_create(k);
	else
		t = TDigestCreate();

	return t;
}

/*
 * distribution_agg transition function -
 * 	adds the given element to the transition tdigest
 */
PG_FUNCTION_INFO_V1(dist_agg_trans);
Datum
dist_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "dist_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = tdigest_startup(fcinfo, 0);
	else
		state = (TDigest *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = TDigestAdd(state, PG_GETARG_FLOAT8(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * dist_agg transition function -
 *
 * 	adds the given element to the transition tdigest using the given value for compression
 */
PG_FUNCTION_INFO_V1(dist_agg_transp);
Datum
dist_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;
	uint64_t k = PG_GETARG_INT32(2);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "dist_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = tdigest_startup(fcinfo, k);
	else
		state = (TDigest *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = TDigestAdd(state, PG_GETARG_FLOAT8(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * dist_combine combine function -
 *
 * 	returns the union of the transition state and the given tdigest
 */
PG_FUNCTION_INFO_V1(dist_combine);
Datum
dist_combine(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	TDigest *state;
	TDigest *incoming;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "dist_combine called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		incoming = (TDigest *) PG_GETARG_VARLENA_P(1);
		state = TDigestCopy(incoming);
	}
	else if (PG_ARGISNULL(1))
		state = (TDigest *) PG_GETARG_VARLENA_P(0);
	else
	{
		state = (TDigest *) PG_GETARG_VARLENA_P(0);
		incoming = (TDigest *) PG_GETARG_VARLENA_P(1);
		state = TDigestMerge(state, incoming);
	}

	state = TDigestCompress(state);
	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * dist_quantile
 */
PG_FUNCTION_INFO_V1(dist_quantile);
Datum
dist_quantile(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 q;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	t = (TDigest *) PG_GETARG_VARLENA_P(0);
	q = PG_GETARG_FLOAT8(1);

	PG_RETURN_FLOAT8(TDigestQuantile(t, q));
}

/*
 * dist_cdf
 */
PG_FUNCTION_INFO_V1(dist_cdf);
Datum
dist_cdf(PG_FUNCTION_ARGS)
{
	TDigest *t;
	float8 x;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	t = (TDigest *) PG_GETARG_VARLENA_P(0);
	x = PG_GETARG_FLOAT8(1);

	PG_RETURN_FLOAT8(TDigestCDF(t, x));
}

/*
 * tdigest_empty
 */
PG_FUNCTION_INFO_V1(tdigest_empty);
Datum
tdigest_empty(PG_FUNCTION_ARGS)
{
	TDigest *t = TDigestCreate();
	PG_RETURN_POINTER(t);
}

/*
 * tdigest_emptyp
 */
PG_FUNCTION_INFO_V1(tdigest_emptyp);
Datum
tdigest_emptyp(PG_FUNCTION_ARGS)
{
	uint64_t k = PG_GETARG_INT32(0);
	TDigest *t = tdigest_create(k);
	PG_RETURN_POINTER(t);
}

/*
 * dist_add
 */
PG_FUNCTION_INFO_V1(dist_add);
Datum
dist_add(PG_FUNCTION_ARGS)
{
	TDigest *t;

	if (PG_ARGISNULL(0))
		t = TDigestCreate();
	else
		t = (TDigest *) PG_GETARG_VARLENA_P(0);

	t = TDigestAdd(t, PG_GETARG_FLOAT8(1), 1);
	t = TDigestCompress(t);
	PG_RETURN_POINTER(t);
}

/*
 * dist_addn
 */
PG_FUNCTION_INFO_V1(dist_addn);
Datum
dist_addn(PG_FUNCTION_ARGS)
{
	TDigest *t;
	int32 n = PG_GETARG_INT32(2);

	if (n < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("n must be non-negative")));

	if (PG_ARGISNULL(0))
		t = TDigestCreate();
	else
		t = (TDigest *) PG_GETARG_VARLENA_P(0);

	if (n)
	{
		t = TDigestAdd(t, PG_GETARG_FLOAT8(1), n);
		t = TDigestCompress(t);
	}

	PG_RETURN_POINTER(t);
}

/*
 * dist_agg_final
 */
PG_FUNCTION_INFO_V1(dist_agg_final);
Datum
dist_agg_final(PG_FUNCTION_ARGS)
{
	TDigest *t = PG_ARGISNULL(0) ? NULL : (TDigest *) PG_GETARG_VARLENA_P(0);

	if (!t)
		PG_RETURN_NULL();

	PG_RETURN_POINTER(t);
}

/*
 * tdigest_serialize
 */
PG_FUNCTION_INFO_V1(tdigest_serialize);
Datum
tdigest_serialize(PG_FUNCTION_ARGS)
{
	return tdigest_compress(fcinfo);
}

/*
 * tdigest_deserialize
 */
PG_FUNCTION_INFO_V1(tdigest_deserialize);
Datum
tdigest_deserialize(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);

	/*
	 * Our deserializan function is a noop, since we basically only need the serialization
	 * function to ensure stored TDigests are as compact as possible but we must provide both.
	 */
	PG_RETURN_POINTER(t);
}

/*
 * tdigest_in
 */
PG_FUNCTION_INFO_V1(tdigest_in);
Datum
tdigest_in(PG_FUNCTION_ARGS)
{
	char *raw = PG_GETARG_CSTRING(0);
	Datum result = DirectFunctionCall1(byteain, (Datum) raw);

	PG_RETURN_POINTER(result);
}

/*
 * tdigest_out
 */
PG_FUNCTION_INFO_V1(tdigest_out);
Datum
tdigest_out(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);
	char *s = (char *) DirectFunctionCall1(byteaout, (Datum) t);

	PG_RETURN_CSTRING(s);
}

/*
 * tdigest_send
 */
PG_FUNCTION_INFO_V1(tdigest_send);
Datum
tdigest_send(PG_FUNCTION_ARGS)
{
	TDigest *t = (TDigest *) PG_GETARG_VARLENA_P(0);
	bytea *result = (bytea *) DirectFunctionCall1(byteasend, (Datum) t);

	PG_RETURN_BYTEA_P(result);
}

/*
 * tdigest_recv
 */
PG_FUNCTION_INFO_V1(tdigest_recv);
Datum
tdigest_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea *result = (bytea *) DirectFunctionCall1(bytearecv, (Datum) buf);

	PG_RETURN_BYTEA_P(result);
}
