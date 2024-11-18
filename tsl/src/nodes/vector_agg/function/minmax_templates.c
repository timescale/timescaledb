/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <utils/date.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/timestamp.h>

#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>

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
minmax_init(void *restrict agg_states, int n)
{
	MinMaxState *states = (MinMaxState *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i].isvalid = false;
		states[i].value = 0;
	}
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

#define AGG_NAME MAX
#define PREDICATE(CURRENT, NEW) ((CURRENT) < (NEW))
#include "minmax_arithmetic_types.c"

#undef AGG_NAME
