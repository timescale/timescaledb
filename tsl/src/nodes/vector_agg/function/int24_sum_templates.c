/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Common parts for vectorized sum(int).
 */
#ifndef GENERATE_DISPATCH_TABLE
typedef struct
{
	int64 result;
	bool isnull;
} Int24SumState;

static void
int_sum_init(void *agg_state)
{
	Int24SumState *state = (Int24SumState *) agg_state;
	state->result = 0;
	state->isnull = true;
}

static void
int_sum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	Int24SumState *state = (Int24SumState *) agg_state;
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
#include "int24_sum_single.c"

#define PG_TYPE INT2
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#include "int24_sum_single.c"

#undef AGG_NAME
