/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define PG_AGG_OID(AGG_NAME, ARGUMENT_TYPE) F_##AGG_NAME##_##ARGUMENT_TYPE
#define PG_AGG_OID_HELPER(X, Y) PG_AGG_OID(X, Y)

#define FUNCTION_NAME_HELPER2(X, Y, Z) X##_##Y##_##Z
#define FUNCTION_NAME_HELPER(X, Y, Z) FUNCTION_NAME_HELPER2(X, Y, Z)
#define FUNCTION_NAME(Z) FUNCTION_NAME_HELPER(AGG_NAME, PG_TYPE, Z)

/*
 * Common parts for vectorized min(), max().
 */
#ifndef GENERATE_DISPATCH_TABLE
typedef struct
{
	bool isvalid;
	Datum value;
} MinMaxState;

static void
minmax_init(void *agg_state)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	state->isvalid = false;
	state->value = 0;
}

static void
minmax_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	*out_result = state->value;
	*out_isnull = !state->isvalid;
}
#endif

/*
 * Templated parts for vectorized min(), max().
 */
#define AGG_NAME MIN
#define PREDICATE(CURRENT, NEW) ((CURRENT) > (NEW))
#include "minmax_arithmetic_types.c"
#undef PREDICATE
#undef AGG_NAME

#define AGG_NAME MAX
#define PREDICATE(CURRENT, NEW) ((CURRENT) < (NEW))
#include "minmax_arithmetic_types.c"
#undef PREDICATE
#undef AGG_NAME

/*
 * Common parts for vectorized sum(int).
 */
#ifndef GENERATE_DISPATCH_TABLE
typedef struct
{
	int64 result;
	bool isnull;
} IntSumState;

static void
int_sum_init(void *agg_state)
{
	IntSumState *state = (IntSumState *) agg_state;
	state->result = 0;
	state->isnull = true;
}

static void
int_sum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	IntSumState *state = (IntSumState *) agg_state;
	*out_result = Int64GetDatum(state->result);
	*out_isnull = state->isnull;
}
#endif

/*
 * Templated parts for vectorized sum(int).
 */
#define AGG_NAME SUM

#define PG_TYPE INT4
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#include "sum_int_single.c"

#define PG_TYPE INT2
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#include "sum_int_single.c"

#undef AGG_NAME

/*
 * Common parts for vectorized sum(float).
 */
#ifndef GENERATE_DISPATCH_TABLE
typedef struct
{
	double result;
	bool isnull;
} FloatSumState;

static void
float_sum_init(void *agg_state)
{
	FloatSumState *state = (FloatSumState *) agg_state;
	state->result = 0;
	state->isnull = true;
}
#endif

/*
 * Templated parts for vectorized sum(float).
 */
#define AGG_NAME SUM

#define PG_TYPE FLOAT4
#define CTYPE float
#define CTYPE_TO_DATUM Float4GetDatum
#define DATUM_TO_CTYPE DatumGetFloat4
#include "sum_float_single.c"

#define PG_TYPE FLOAT8
#define CTYPE double
#define CTYPE_TO_DATUM Float8GetDatum
#define DATUM_TO_CTYPE DatumGetFloat8
#include "sum_float_single.c"

#undef AGG_NAME

/*
 * Functions handled by *accum() aggregate functions states.
 */
#define AVG_CASE(PG_TYPE) AVG_CASE_HELPER(PG_TYPE)
#define AVG_CASE_HELPER(PG_TYPE)                                                                   \
	case F_STDDEV_##PG_TYPE:                                                                       \
	case F_STDDEV_SAMP_##PG_TYPE:                                                                  \
	case F_STDDEV_POP_##PG_TYPE:                                                                   \
	case F_VARIANCE_##PG_TYPE:                                                                     \
	case F_VAR_SAMP_##PG_TYPE:                                                                     \
	case F_VAR_POP_##PG_TYPE:                                                                      \
	case F_AVG_##PG_TYPE:

/*
 * Common parts for vectorized avg(float).
 */
#ifndef GENERATE_DISPATCH_TABLE
/*
 * State of Youngs-Cramer algorithm, see the comments for float8_accum().
 */
typedef struct
{
	double N;
	double Sx;
	double Sxx;
} FloatAvgState;

static void
avg_float_init(void *agg_state)
{
	FloatAvgState *state = (FloatAvgState *) agg_state;
	*state = (FloatAvgState){ 0 };
}

static void
avg_float_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	FloatAvgState *state = (FloatAvgState *) agg_state;

	Datum transdatums[3] = {

		Float8GetDatumFast(state->N),
		Float8GetDatumFast(state->Sx),
		Float8GetDatumFast(state->Sxx),
	};

	ArrayType *result = construct_array(transdatums,
										3,
										FLOAT8OID,
										sizeof(float8),
										FLOAT8PASSBYVAL,
										TYPALIGN_DOUBLE);

	*out_result = PointerGetDatum(result);
	*out_isnull = false;
}
#endif

/*
 * Templated parts for vectorized avg(float).
 */
#define AGG_NAME AVG

#define PG_TYPE FLOAT4
#define CTYPE float
#define CTYPE_TO_DATUM Float4GetDatum
#define DATUM_TO_CTYPE DatumGetFloat4
#include "avg_float_single.c"

#define PG_TYPE FLOAT8
#define CTYPE double
#define CTYPE_TO_DATUM Float8GetDatum
#define DATUM_TO_CTYPE DatumGetFloat8
#include "avg_float_single.c"

#undef AGG_NAME

#undef AVG_CASE
#undef AVG_CASE_HELPER

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME_HELPER2

#undef PG_AGG_OID_HELPER
#undef PG_AGG_OID
