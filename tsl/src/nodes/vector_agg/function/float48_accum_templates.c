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
