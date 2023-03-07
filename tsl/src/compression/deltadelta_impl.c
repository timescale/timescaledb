
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X) delta_delta_decompress_all_##X
#define FUNCTION_NAME(X) FUNCTION_NAME_HELPER(X)

static ArrowArray *
FUNCTION_NAME(ELEMENT_TYPE)(DecompressionIterator *iter_base)
{
	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_DELTADELTA);
	DeltaDeltaDecompressionIterator *iter = (DeltaDeltaDecompressionIterator *) iter_base;

	const int n_total =
		iter->has_nulls ? iter->nulls.num_elements : iter->delta_deltas.num_elements;
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	Assert(iter->delta_deltas.num_elements_returned == 0);

	uint64 *restrict validity_bitmap = palloc0(sizeof(uint64) * ((n_total + 64 - 1) / 64));
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total);

	uint64 prev_val = 0;
	uint64 prev_delta = 0;
	for (int i = 0; i < n_total; i++)
	{
		if (iter->has_nulls)
		{
			Simple8bRleDecompressResult res = simple8brle_bitmap_get_next(&iter->nulls);
			Assert(!res.is_done);
			if (res.val)
			{
				/* Null. */
				decompressed_values[i] = 0;
				arrow_validity_bitmap_set(validity_bitmap, i, false);
				continue;
			}
		}

		/*
		 * We use the interator API here because the counter for deltadeltas is
		 * different from `i` in the presence of nulls.
		 */
		const Simple8bRleDecompressResult result =
			simple8brle_decompression_iterator_try_next_forward(&iter->delta_deltas);
		Assert(!result.is_done);

		const uint64_t delta_delta = zig_zag_decode(result.val);
		prev_delta += delta_delta;
		prev_val += prev_delta;

		decompressed_values[i] = prev_val;
		arrow_validity_bitmap_set(validity_bitmap, i, true);
	}

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
