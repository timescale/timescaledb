/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */

#include <postgres.h>

#include <utils/date.h>
#include <utils/fmgroids.h>

#include "compression/arrow_c_data_interface.h"

#include "vector_predicates.h"

#include "compat/compat.h"
#include "compression/compression.h"
#include "debug_assert.h"

/*
 * We include all implementations of vector-const predicates here. No separate
 * declarations for them to reduce the amount of macro template magic.
 */
#include "pred_vector_const_arithmetic_all.c"

#include "compression/compression.h"

static void
vector_const_texteq(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	Assert(!arrow->dictionary);

	text *consttext = (text *) DatumGetPointer(constdatum);
	const size_t textlen = VARSIZE_ANY_EXHDR(consttext);
	const uint8 *cstring = (uint8 *) VARDATA_ANY(consttext);
	const uint32 *offsets = (uint32 *) arrow->buffers[1];
	const uint8 *values = (uint8 *) arrow->buffers[2];

	const size_t n = arrow->length;
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const size_t row = outer * 64 + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const uint32 start = offsets[row];                                                             \
	const uint32 end = offsets[row + 1];                                                           \
	const uint32 veclen = end - start;                                                             \
	bool valid = veclen != textlen ?                                                               \
					 false :                                                                       \
					 (strncmp((char *) &values[start], (char *) cstring, textlen) == 0);           \
	word |= ((uint64) valid) << bit_index;                                                         \
	//	fprintf(stderr, "plain row %ld: valid %d\n", row, valid);

			INNER_LOOP
		}
		result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 word = 0;
		for (size_t row = (n / 64) * 64; row < n; row++)
		{
			const size_t bit_index = row % 64;
			INNER_LOOP
		}
		result[n / 64] &= word;
	}

#undef INNER_LOOP
}

/*
 * Look up the vectorized implementation for a Postgres predicate, specified by
 * its Oid in pg_proc. Note that this Oid is different from the opcode.
 */
VectorPredicate *
get_vector_const_predicate(Oid pg_predicate)
{
	switch (pg_predicate)
	{
#define GENERATE_DISPATCH_TABLE
#include "pred_vector_const_arithmetic_all.c"
#undef GENERATE_DISPATCH_TABLE

		case F_TEXTEQ:
			return vector_const_texteq;
	}

	return NULL;
}

void
vector_nulltest(const ArrowArray *arrow, int test_type, uint64 *restrict result)
{
	const bool should_be_null = test_type == IS_NULL;

	const uint16 bitmap_words = (arrow->length + 63) / 64;
	const uint64 *restrict validity = (const uint64 *) arrow->buffers[0];
	for (uint16 i = 0; i < bitmap_words; i++)
	{
		if (should_be_null)
		{
			result[i] &= ~validity[i];
		}
		else
		{
			result[i] &= validity[i];
		}
	}
}
