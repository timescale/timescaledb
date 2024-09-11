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
int24_avg_accum_init(void *agg_state)
{
	Int24AvgAccumState *state = (Int24AvgAccumState *) agg_state;
	state->count = 0;
	state->sum = 0;
}

static void
int24_avg_accum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	Int24AvgAccumState *state = (Int24AvgAccumState *) agg_state;

	Datum transdatums[2] = {
		Int64GetDatumFast(state->count),
		Int64GetDatumFast(state->sum),
	};

	ArrayType *result =
		construct_array(transdatums, 2, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
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
