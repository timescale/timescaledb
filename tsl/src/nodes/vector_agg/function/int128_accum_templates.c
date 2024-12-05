/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vectorized transition functions that use the Int128AggState transition state.
 */

#include <postgres.h>

#include <nodes/execnodes.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>

#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>

#ifdef HAVE_INT128
#ifndef GENERATE_DISPATCH_TABLE
/*
 * The PG aggregation state that we have to serialize. Copied from numeric.c.
 */
typedef struct
{
	bool calcSumX2; /* if true, calculate sumX2 */
	int64 N;		/* count of processed numbers */
	int128 sumX;	/* sum of processed numbers */
	int128 sumX2;	/* sum of squares of processed numbers */
} PgInt128AggState;
#endif

/*
 * Vectorized implementation of int8_avg_accum() function.
 */
#define AGG_NAME accum_no_squares
#define AGG_CASES                                                                                  \
	case PG_AGG_OID_HELPER(SUM, PG_TYPE):                                                          \
	case PG_AGG_OID_HELPER(AVG, PG_TYPE):

#define PG_TYPE INT8
#define CTYPE int64
#define DATUM_TO_CTYPE DatumGetInt64
#include "int128_accum_single.c"

/*
 * Vectorized implementation of int2_accum() function.
 */
#define NEED_SUMX2
#define AGG_NAME accum_with_squares
#define AGG_CASES                                                                                  \
	case PG_AGG_OID_HELPER(STDDEV, PG_TYPE):                                                       \
	case PG_AGG_OID_HELPER(STDDEV_SAMP, PG_TYPE):                                                  \
	case PG_AGG_OID_HELPER(STDDEV_POP, PG_TYPE):                                                   \
	case PG_AGG_OID_HELPER(VARIANCE, PG_TYPE):                                                     \
	case PG_AGG_OID_HELPER(VAR_SAMP, PG_TYPE):                                                     \
	case PG_AGG_OID_HELPER(VAR_POP, PG_TYPE):

#define PG_TYPE INT2
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#include "int128_accum_single.c"

/*
 * Vectorized implementation of int4_accum() function.
 */
#define NEED_SUMX2
#define AGG_NAME accum_with_squares
#define AGG_CASES                                                                                  \
	case PG_AGG_OID_HELPER(STDDEV, PG_TYPE):                                                       \
	case PG_AGG_OID_HELPER(STDDEV_SAMP, PG_TYPE):                                                  \
	case PG_AGG_OID_HELPER(STDDEV_POP, PG_TYPE):                                                   \
	case PG_AGG_OID_HELPER(VARIANCE, PG_TYPE):                                                     \
	case PG_AGG_OID_HELPER(VAR_SAMP, PG_TYPE):                                                     \
	case PG_AGG_OID_HELPER(VAR_POP, PG_TYPE):

#define PG_TYPE INT4
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#include "int128_accum_single.c"

#endif
