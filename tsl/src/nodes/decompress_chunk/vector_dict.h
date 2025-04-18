/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "compression/arrow_c_data_interface.h"

/*
 * When we have a dictionary-encoded Arrow Array, and have run a predicate on
 * the dictionary, this function is used to translate the dictionary predicate
 * result to the final predicate result.
 */
static void
translate_bitmap_from_dictionary(const ArrowArray *arrow, const uint64 *dict_result,
								 uint64 *restrict final_result)
{
	Assert(arrow->dictionary != NULL);

	const size_t n = arrow->length;
	const int16 *indices = (int16 *) arrow->buffers[1];
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const size_t row = (outer * 64) + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const int16 index = indices[row];                                                              \
	const bool valid = arrow_row_is_valid(dict_result, index);                                     \
	word |= ((uint64) valid) << bit_index;

			INNER_LOOP
		}
		final_result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 word = 0;
		for (size_t row = (n / 64) * 64; row < n; row++)
		{
			const size_t bit_index = row % 64;

			INNER_LOOP
		}
		final_result[n / 64] &= word;
	}
#undef INNER_LOOP
}
