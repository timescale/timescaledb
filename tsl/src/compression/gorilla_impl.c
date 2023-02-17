#define FUNCTION_NAME_HELPER(X) gorilla_decompress_all_##X
#define FUNCTION_NAME(X) FUNCTION_NAME_HELPER(X)

static ArrowArray
FUNCTION_NAME(ELEMENT_TYPE)(DecompressionIterator *iter_base)
{
	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_GORILLA);
	GorillaDecompressionIterator *iter = (GorillaDecompressionIterator *) iter_base;

	const int n_total = iter->has_nulls ? iter->nulls.num_elements : iter->tag0s.num_elements;
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	Assert(iter->tag0s.num_elements_returned == 0);

/*
	Assert(iter->num_elements_returned = 0);
	iter->num_elements = n_total;
	iter->num_elements_returned = n_total;

	Compare it with the working version:
	git difftool 88c7a8ee05d60971fa9c886540d2b6fdd6a9118d:tsl/src/compression/gorilla.c tsl/src/compression/gorilla_impl.c

	But it's the same...
*/

	uint64 *restrict validity_bitmap = palloc0(sizeof(uint64) * ((n_total + 64 - 1) / 64));
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total);

	ELEMENT_TYPE prev_value = 0;
	uint8_t prev_xor_bits_used = 0;
	uint8_t prev_leading_zeros = 0;
	for (int i = 0; i < n_total; i++)
	{
		if (iter->has_nulls && iter->nulls.decompressed_values[i])
		{
			/* Placeholder. */
			decompressed_values[i] = 0;
			arrow_validity_bitmap_set(validity_bitmap, i, false);
			continue;
		}

		const Simple8bRleDecompressResult tag0 =
			simple8brle_decompression_iterator_try_next_forward(&iter->tag0s);
		Assert(!tag0.is_done);

		if (tag0.val == 0)
		{
			Assert(i > 0);
			decompressed_values[i] = prev_value;
			arrow_validity_bitmap_set(validity_bitmap, i, true);
			continue;
		}
		else
		{
			Assert(tag0.val == 1);
		}

		const Simple8bRleDecompressResult tag1 =
			simple8brle_decompression_iterator_try_next_forward(&iter->tag1s);
		Assert(!tag1.is_done);

		if (tag1.val != 0)
		{
			Assert(tag1.val == 1);
			const Simple8bRleDecompressResult num_xor_bits =
				simple8brle_decompression_iterator_try_next_forward(&iter->num_bits_used);
			Assert(!num_xor_bits.is_done);
			prev_xor_bits_used = num_xor_bits.val;
			prev_leading_zeros = bit_array_iter_next(&iter->leading_zeros, BITS_PER_LEADING_ZEROS);
		}
		else
		{
			Assert(i > 0);
		}

		const ELEMENT_TYPE xored_value = bit_array_iter_next(&iter->xors, prev_xor_bits_used)
										 << (64 - (prev_leading_zeros + prev_xor_bits_used));
		prev_value ^= xored_value;

		decompressed_values[i] = prev_value;
		arrow_validity_bitmap_set(validity_bitmap, i, true);
	}

	const void **buffers = palloc(sizeof(void *) * 2);
	buffers[0] = validity_bitmap;
	buffers[1] = decompressed_values;
	return (ArrowArray){
		.n_buffers = 2,
		.buffers = buffers,
		.length = n_total,
		.null_count = -1,
	};
}

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
