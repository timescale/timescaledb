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

/*
 * avg() uses only N and Sx of the float8_accum() state, so it has a cheaper
 * implementation. It can be used only when the transition state is not shared
 * with stddev() or variance(), so it is not in the general dispatch table. See
 * get_unshared_vector_aggregate().
 */
#define AGG_NAME accum_no_squares
#include "float48_accum_types.c"

#define NEED_SXX
#define AGG_NAME accum_with_squares
#include "float48_accum_types.c"
