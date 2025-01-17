/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vectorized implementation for int{2,4}_avg_accum transition functions.
 */

#include <postgres.h>

#include <catalog/pg_type_d.h>
#include <utils/array.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>

#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>

#ifndef GENERATE_DISPATCH_TABLE
typedef struct
{
	int64 count;
	int64 sum;
} Int24AvgAccumState;

static void
int24_avg_accum_init(void *restrict agg_states, int n)
{
	Int24AvgAccumState *states = (Int24AvgAccumState *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i].count = 0;
		states[i].sum = 0;
	}
}

static void
int24_avg_accum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	Int24AvgAccumState *state = (Int24AvgAccumState *) agg_state;

	const size_t nbytes = 2 * sizeof(int64) + ARR_OVERHEAD_NONULLS(/* ndims = */ 1);
	ArrayType *result = palloc(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = 1;
	result->dataoffset = 0;
	result->elemtype = INT8OID;
	ARR_DIMS(result)[0] = 2;
	ARR_LBOUND(result)[0] = 1;

	/*
	 * The array elements are stored by value, regardless of if the int8 itself
	 * is by-value on this platform.
	 */
	((int64 *) ARR_DATA_PTR(result))[0] = state->count;
	((int64 *) ARR_DATA_PTR(result))[1] = state->sum;

	*out_result = PointerGetDatum(result);
	*out_isnull = false;
}
#endif

#define AGG_NAME AVG

#define PG_TYPE INT2
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#include "int24_avg_accum_single.c"

#define PG_TYPE INT4
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#include "int24_avg_accum_single.c"

#undef AGG_NAME
