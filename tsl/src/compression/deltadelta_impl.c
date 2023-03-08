/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

#define INNER_SIZE 8

static ArrowArray *
FUNCTION_NAME(delta_delta_decompress_all, ELEMENT_TYPE)(DecompressionIterator *iter_base)
{
	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA);
	DeltaDeltaDecompressionIterator *iter = (DeltaDeltaDecompressionIterator *) iter_base;

	const bool has_nulls = iter->has_nulls;
	const int n_total = has_nulls ? iter->nulls.num_elements : iter->delta_deltas.num_elements;
	const int n_total_padded = ((n_total * sizeof(ELEMENT_TYPE) + 63) / 64 ) * 64 / sizeof(ELEMENT_TYPE);
	const int n_notnull = iter->delta_deltas.num_elements;
	const int n_notnull_padded = ((n_notnull * sizeof(ELEMENT_TYPE) + 63) / 64 ) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);
	Assert(n_notnull_padded >= n_notnull);
	Assert(n_total >= n_notnull);
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	Assert(iter->delta_deltas.num_elements_returned == 0);

	const uint32 validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total_padded);

	/* All data valid by default, we will fill in the nulls later. */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	/* Now fill the data w/o nulls. */
	ELEMENT_TYPE current_delta = 0;
	ELEMENT_TYPE current_element = 0;
	Assert(n_notnull_padded % INNER_SIZE == 0);
	uint64_t *restrict source = iter->delta_deltas.decompressed_values;
	for (int outer = 0; outer < n_notnull_padded; outer+= INNER_SIZE)
	{
		for (int inner = 0; inner < INNER_SIZE; inner++)
		{
			/*
			 * Manual unrolling speeds up this function by about 10%, but my
			 * attempts to get clang to vectorize the double-prefix-sum part
			 * have failed. Also tried prefix sum from here:
			 * https://en.algorithmica.org/hpc/algorithms/prefix/
			 * Only makes it slower.
			 */
			current_delta += zig_zag_decode(source[outer + inner]);
			current_element += current_delta;
			decompressed_values[outer + inner] = current_element;
		}
	}


	/* Now move the data to account for nulls, and fill the validity bitmap. */
	if (has_nulls)
	{
		simple8brle_bitmap_rewind(&iter->nulls);
		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			Simple8bRleDecompressResult res = simple8brle_bitmap_get_next_reverse(&iter->nulls);
			Assert(!res.is_done);
			if (res.val)
			{
				decompressed_values[i] = 0;
				arrow_validity_bitmap_set(validity_bitmap, i, false);
			}
			else
			{
				decompressed_values[i] = decompressed_values[current_notnull_element--];
			}
		}

		Assert(current_notnull_element == -1);
	}

	/* Return the result. */
	ArrowArray *result = palloc0(sizeof(ArrowArray));
	const void **buffers = palloc(sizeof(void *) * 2);
	buffers[0] = validity_bitmap;
	buffers[1] = decompressed_values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = -1;
	return result;
}

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
