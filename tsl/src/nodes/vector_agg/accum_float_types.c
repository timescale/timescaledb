/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions handled by *accum() aggregate functions states, implementation for
 * all types. They use the same Youngs-Cramer state, but for AVG we can skip
 * calculating the Sxx variable.
 */

#define PG_TYPE FLOAT4
#define CTYPE float
#define CTYPE_TO_DATUM Float4GetDatum
#define DATUM_TO_CTYPE DatumGetFloat4
#include "accum_float_single.c"

#define PG_TYPE FLOAT8
#define CTYPE double
#define CTYPE_TO_DATUM Float8GetDatum
#define DATUM_TO_CTYPE DatumGetFloat8
#include "accum_float_single.c"

#undef AGG_NAME
#undef NEED_SXX
