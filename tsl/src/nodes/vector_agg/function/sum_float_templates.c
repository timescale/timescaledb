/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Common parts for vectorized sum(float).
 */

#include <postgres.h>
#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>

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
#define MASKTYPE uint32
#define CTYPE_TO_DATUM Float4GetDatum
#define DATUM_TO_CTYPE DatumGetFloat4
#include "sum_float_single.c"

#define PG_TYPE FLOAT8
#define CTYPE double
#define MASKTYPE uint64
#define CTYPE_TO_DATUM Float8GetDatum
#define DATUM_TO_CTYPE DatumGetFloat8
#include "sum_float_single.c"

#undef AGG_NAME
