/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <limits.h>

#include <postgres.h>

#include <common/int.h>
#include <utils/date.h>
#include <utils/float.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>

#include "functions.h"

#include "compat/compat.h"

/*
 * Aggregate function count(*).
 */
typedef struct
{
	int64 count;
} CountState;

static void
count_init(void *restrict agg_states, int n)
{
	CountState *states = (CountState *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i].count = 0;
	}
}

static void
count_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	CountState *state = (CountState *) agg_state;
	*out_result = Int64GetDatum(state->count);
	*out_isnull = false;
}

static void
count_star_scalar(void *agg_state, Datum constvalue, bool constisnull, int n,
				  MemoryContext agg_extra_mctx)
{
	CountState *state = (CountState *) agg_state;
	state->count += n;
}

static void
count_star_many_scalar(void *restrict agg_states, uint32 *restrict offsets, int start_row,
					   int end_row, Datum constvalue, bool constisnull,
					   MemoryContext agg_extra_mctx)
{
	CountState *states = (CountState *) agg_states;
	for (int row = start_row; row < end_row; row++)
	{
		if (offsets[row] == 0)
		{
			continue;
		}

		states[offsets[row]].count++;
	}
}

VectorAggFunctions count_star_agg = {
	.state_bytes = sizeof(CountState),
	.agg_init = count_init,
	.agg_scalar = count_star_scalar,
	.agg_emit = count_emit,
	.agg_many_scalar = count_star_many_scalar,
};

/*
 * Aggregate function count(x).
 */
static void
count_any_scalar(void *agg_state, Datum constvalue, bool constisnull, int n,
				 MemoryContext agg_extra_mctx)
{
	if (constisnull)
	{
		return;
	}

	CountState *state = (CountState *) agg_state;
	state->count += n;
}

static void
count_any_many_vector(void *agg_state, const ArrowArray *vector, const uint64 *filter,
					  MemoryContext agg_extra_mctx)
{
	CountState *state = (CountState *) agg_state;
	const int n = vector->length;
	/* First, process the full words. */
	for (int i = 0; i < n / 64; i++)
	{
		const uint64 filter_word = filter ? filter[i] : ~0ULL;

#ifdef HAVE__BUILTIN_POPCOUNT
		state->count += __builtin_popcountll(filter_word);
#else
		/*
		 * Unfortunately, we have to have this fallback for Windows.
		 */
		for (uint16 i = 0; i < 64; i++)
		{
			const bool this_bit = (filter_word >> i) & 1;
			state->count += this_bit;
		}
#endif
	}

	/*
	 * The tail word needs special handling because not all rows there are valid
	 * (some are past-the-end) even when the bitmap is null.
	 */
	for (int i = 64 * (n / 64); i < n; i++)
	{
		state->count += arrow_row_is_valid(filter, i);
	}
}

static void
count_any_many(void *restrict agg_states, uint32 *restrict offsets, int start_row, int end_row,
			   const ArrowArray *vector, MemoryContext agg_extra_mctx)
{
	const uint64 *valid = vector->buffers[0];
	for (int row = start_row; row < end_row; row++)
	{
		CountState *state = (offsets[row] + (CountState *) agg_states);
		const bool row_passes = (offsets[row] != 0);
		const bool value_notnull = arrow_row_is_valid(valid, row);
		if (row_passes && value_notnull)
		{
			state->count++;
		}
	}
}

VectorAggFunctions count_any_agg = {
	.state_bytes = sizeof(CountState),
	.agg_init = count_init,
	.agg_emit = count_emit,
	.agg_scalar = count_any_scalar,
	.agg_vector = count_any_vector,
};

/*
 * Return the vector aggregate definition corresponding to the given
 * PG aggregate function Oid.
 */
VectorAggFunctions *
get_vector_aggregate(Oid aggfnoid)
{
	switch (aggfnoid)
	{
		case F_COUNT_:
			return &count_star_agg;
		case F_COUNT_ANY:
			return &count_any_agg;
#define GENERATE_DISPATCH_TABLE 1
#include "float48_accum_templates.c"
#include "int128_accum_templates.c"
#include "int24_avg_accum_templates.c"
#include "int24_sum_templates.c"
#include "minmax_templates.c"
#include "sum_float_templates.c"
#undef GENERATE_DISPATCH_TABLE
		default:
			return NULL;
	}
}
