/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <port/pg_bitutils.h>
#include <utils/date.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/timestamp.h>

#include "functions.h"
#include "template_helper.h"
#include <compression/arrow_c_data_interface.h>

#include "compat/compat.h"

#if PG16_GE
#include <varatt.h>
#endif

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
 *
 * NaN handled similar to equivalent PG functions.
 */
#define AGG_NAME MIN
#define PREDICATE(CURRENT, NEW)                                                                    \
	(unlikely(!isnan((double) (NEW))) && (isnan((double) (CURRENT)) || (CURRENT) > (NEW)))
#include "minmax_arithmetic_types.c"

#define AGG_NAME MAX
#define PREDICATE(CURRENT, NEW)                                                                    \
	(unlikely(!isnan((double) (CURRENT))) && (isnan((double) (NEW)) || (CURRENT) < (NEW)))
#include "minmax_arithmetic_types.c"

#ifndef GENERATE_DISPATCH_TABLE

/*
 * Common parts for vectorized min(text), max(text).
 */

typedef struct
{
	uint32 capacity;
	struct varlena *data;
} MinMaxBytesState;

typedef struct BytesView
{
	const uint8 *data;
	uint32 len;
} BytesView;

static void
minmax_bytes_init(void *restrict agg_states, int n)
{
	MinMaxBytesState *restrict states = (MinMaxBytesState *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i].capacity = 0;
		states[i].data = NULL;
	}
}

static void
minmax_bytes_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	MinMaxBytesState *state = (MinMaxBytesState *) agg_state;
	*out_isnull = state->capacity == 0;
	*out_result = PointerGetDatum(state->data);
}
#endif

/*
 * Templated parts for vectorized min(text), max(text).
 */
#define PG_TYPE TEXT
#define AGG_NAME MIN
#define PREDICATE(CURRENT, NEW) ((CURRENT) > (NEW))
#include "minmax_text.c"

#define PG_TYPE TEXT
#define AGG_NAME MAX
#define PREDICATE(CURRENT, NEW) ((CURRENT) < (NEW))
#include "minmax_text.c"

#undef AGG_NAME
