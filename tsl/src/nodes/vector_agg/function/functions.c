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
count_init(void *agg_state)
{
	CountState *state = (CountState *) agg_state;
	state->count = 0;
}

static void
count_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	CountState *state = (CountState *) agg_state;
	*out_result = Int64GetDatum(state->count);
	*out_isnull = false;
}

static void
count_star_const(void *agg_state, Datum constvalue, bool constisnull, int n,
				 MemoryContext agg_extra_mctx)
{
	CountState *state = (CountState *) agg_state;
	state->count += n;
}

VectorAggFunctions count_star_agg = {
	.state_bytes = sizeof(CountState),
	.agg_init = count_init,
	.agg_const = count_star_const,
	.agg_emit = count_emit,
};

/*
 * Aggregate function count(x).
 */
static void
count_any_const(void *agg_state, Datum constvalue, bool constisnull, int n,
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
count_any_vector(void *agg_state, const ArrowArray *vector, const uint64 *filter,
				 MemoryContext agg_extra_mctx)
{
	CountState *state = (CountState *) agg_state;
	const int n = vector->length;
	const uint64 *restrict validity = (uint64 *) vector->buffers[0];
	/* First, process the full words. */
	for (int i = 0; i < n / 64; i++)
	{
		const uint64 validity_word = validity ? validity[i] : ~0ULL;
		const uint64 filter_word = filter ? filter[i] : ~0ULL;
		const uint64 resulting_word = validity_word & filter_word;

#ifdef HAVE__BUILTIN_POPCOUNT
		state->count += __builtin_popcountll(resulting_word);
#else
		/*
		 * Unfortunately, we have to have this fallback for Windows.
		 */
		for (uint16 i = 0; i < 64; i++)
		{
			const bool this_bit = (resulting_word >> i) & 1;
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
		state->count += arrow_row_is_valid(validity, i) * arrow_row_is_valid(filter, i);
	}
}

VectorAggFunctions count_any_agg = {
	.state_bytes = sizeof(CountState),
	.agg_init = count_init,
	.agg_emit = count_emit,
	.agg_const = count_any_const,
	.agg_vector = count_any_vector,
};

#include "function_templates.c"

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
#include "function_templates.c"
#undef GENERATE_DISPATCH_TABLE
		default:
			return NULL;
	}
}
