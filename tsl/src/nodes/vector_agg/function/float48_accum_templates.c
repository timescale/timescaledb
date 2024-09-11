/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Common parts for aggregate functions that use the float{4,8}_accum transition.
 */

#include <postgres.h>

#include <catalog/pg_type_d.h>
#include <utils/array.h>
#include <utils/float.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>

#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>

#ifndef GENERATE_DISPATCH_TABLE
/*
 * State of Youngs-Cramer algorithm, see the comments for float8_accum().
 */
typedef struct
{
	double N;
	double Sx;
	double Sxx;
} Float48AccumState;

static void
float48_accum_init(void *agg_state)
{
	Float48AccumState *state = (Float48AccumState *) agg_state;
	*state = (Float48AccumState){ 0 };
}

static void
float48_accum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	Float48AccumState *state = (Float48AccumState *) agg_state;

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

/*
 * Combine two Youngs-Cramer states following the float8_combine() function.
 */
static pg_attribute_always_inline void
youngs_cramer_combine(double *inout_N, double *inout_Sx, double *inout_Sxx, double N2, double Sx2,
					  double Sxx2)
{
	const double N1 = *inout_N;
	const double Sx1 = *inout_Sx;
	const double Sxx1 = *inout_Sxx;

	if (unlikely(N1 == 0))
	{
		*inout_N = N2;
		*inout_Sx = Sx2;
		*inout_Sxx = Sxx2;
		return;
	}

	if (unlikely(N2 == 0))
	{
		*inout_N = N1;
		*inout_Sx = Sx1;
		*inout_Sxx = Sxx1;
		return;
	}

	const double combinedN = N1 + N2;
	const double combinedSx = Sx1 + Sx2;
	const double tmp = Sx1 / N1 - Sx2 / N2;
	const double combinedSxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / combinedN;

	*inout_N = combinedN;
	*inout_Sx = combinedSx;
	*inout_Sxx = combinedSxx;
}

#endif

/*
 * Templated parts for vectorized avg(float).
 */
#define AGG_NAME accum_no_squares
#include "float48_accum_types.c"

/*
 * Templated parts for vectorized functions that use the Sxx state (stddev etc).
 */
#define AGG_NAME accum_with_squares
#define NEED_SXX
#include "float48_accum_types.c"
