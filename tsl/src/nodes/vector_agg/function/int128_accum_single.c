/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vectorized implementation of a single transition function that uses the
 * Int128AggState transition state. Note that the serialization format differs
 * based on whether the sum of squares is needed.
 */
#ifdef GENERATE_DISPATCH_TABLE
AGG_CASES
return &FUNCTION_NAME(argdef);
#else

typedef struct
{
	int64 N;
	int128 sumX;
#ifdef NEED_SUMX2
	int128 sumX2;
#endif
} FUNCTION_NAME(state);

static void
FUNCTION_NAME(init)(void *agg_state)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	state->N = 0;
	state->sumX = 0;
#ifdef NEED_SUMX2
	state->sumX2 = 0;
#endif
}

static void
FUNCTION_NAME(emit)(void *agg_state, Datum *out_result, bool *out_isnull)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;

	PgInt128AggState result = {
		.N = state->N,
		.sumX = state->sumX,
#ifdef NEED_SUMX2
		.sumX2 = state->sumX2,
#endif
	};

	LOCAL_FCINFO(fcinfo, 1);
	AggState agg_context = { .ss.ps.type = T_AggState };
	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, (Node *) &agg_context, NULL);

	fcinfo->args[0].value = PointerGetDatum(&result);
	fcinfo->args[0].isnull = false;

#ifdef NEED_SUMX2
	*out_result = numeric_poly_serialize(fcinfo);
#else
	*out_result = int8_avg_serialize(fcinfo);
#endif
	*out_isnull = false;
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *valid1,
						   const uint64 *valid2, MemoryContext agg_extra_mctx)
{
	int64 N = 0;
	int128 sumX = 0;
#ifdef NEED_SUMX2
	int128 sumX2 = 0;
#endif
	for (int row = 0; row < n; row++)
	{
		const bool row_ok = arrow_both_valid(valid1, valid2, row);
		const CTYPE value = values[row];
		N += row_ok;
		sumX += value * row_ok;
#ifdef NEED_SUMX2
		sumX2 += ((int128) value) * ((int128) value) * row_ok;
#endif
	}

	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	state->N += N;
	state->sumX += sumX;
#ifdef NEED_SUMX2
	state->sumX2 += sumX2;
#endif
}

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

static VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(FUNCTION_NAME(state)),
	.agg_init = FUNCTION_NAME(init),
	.agg_emit = FUNCTION_NAME(emit),
	.agg_const = FUNCTION_NAME(const),
	.agg_vector = FUNCTION_NAME(vector),
};

#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef AGG_CASES
#undef AGG_NAME
#undef NEED_SUMX2
