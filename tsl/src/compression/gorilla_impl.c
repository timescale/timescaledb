/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

static ArrowArray *
FUNCTION_NAME(gorilla_decompress_all, ELEMENT_TYPE)(CompressedGorillaData *gorilla_data)
{
	const bool has_nulls = gorilla_data->nulls != NULL;
	const int n_total =
		has_nulls ? gorilla_data->nulls->num_elements : gorilla_data->tag0s->num_elements;
	const int n_total_padded =
		((n_total * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	const int n_notnull = gorilla_data->tag0s->num_elements;
	Assert(n_total_padded >= n_total);
	Assert(n_total >= n_notnull);
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const uint32 validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total_padded);

	/* All data valid by default, we will fill in the nulls later. */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	/* Unpack the basic compressed data parts. */
	Simple8bRleBitmap tag0s = simple8brle_decompress_bitmap(gorilla_data->tag0s);
	Simple8bRleBitmap tag1s = simple8brle_decompress_bitmap(gorilla_data->tag1s);

	BitArray leading_zeros_bitarray = gorilla_data->leading_zeros;
	BitArrayIterator leading_zeros_iterator;
	bit_array_iterator_init(&leading_zeros_iterator, &leading_zeros_bitarray);

	Simple8bRleDecompressionIterator num_bits_used;
	simple8brle_decompression_iterator_init_forward(&num_bits_used,
													gorilla_data->num_bits_used_per_xor);

	BitArray xors_bitarray = gorilla_data->xors;
	BitArrayIterator xors_iterator;
	bit_array_iterator_init(&xors_iterator, &xors_bitarray);

	/*
	 * Now fill the data w/o nulls.
	 *
	 * The init value of num_xor_bits is to ward off the clang-tidy false
	 * positive. clang-tidy doesn't understand it will be re-initialized on the
	 * first step.
	 */
	Simple8bRleDecompressResult num_xor_bits = { .val = 64 };
	ELEMENT_TYPE prev = 0;
	uint8_t prev_leading_zeros = 0;
	for (int i = 0; i < n_notnull; i++)
	{
		Simple8bRleDecompressResult tag0 = simple8brle_bitmap_get_next(&tag0s);
		Assert(!tag0.is_done);
		if (tag0.val == 0)
		{
			/* Repeat previous value. */
			Assert(i > 0);
			decompressed_values[i] = prev;
			continue;
		}

		Simple8bRleDecompressResult tag1 = simple8brle_bitmap_get_next(&tag1s);
		Assert(!tag1.is_done);
		if (unlikely(tag1.val == 1))
		{
			/* Load new bit widths. */
			num_xor_bits = simple8brle_decompression_iterator_try_next_forward(&num_bits_used);
			Assert(!num_xor_bits.is_done);
			Assert(num_xor_bits.val <= 64);

			prev_leading_zeros =
				bit_array_iter_next(&leading_zeros_iterator, BITS_PER_LEADING_ZEROS);
		}
		else
		{
			Assert(i > 0);
		}

		/* Reuse previous bit widths. */
		prev ^= bit_array_iter_next(&xors_iterator, num_xor_bits.val)
				<< (64 - (prev_leading_zeros + num_xor_bits.val));
		decompressed_values[i] = prev;
	}
	Assert(simple8brle_decompression_iterator_try_next_forward(&num_bits_used).is_done);

	/* Now move the data to account for nulls, and fill the validity bitmap. */
	if (has_nulls)
	{
		Simple8bRleBitmap nulls = simple8brle_decompress_bitmap(gorilla_data->nulls);
		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			if (nulls.bitmap[i])
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

#undef INNER_SIZE

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
