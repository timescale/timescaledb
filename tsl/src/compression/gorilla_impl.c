/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Decompress the entire batch of gorilla-compressed rows into an Arrow array.
 * Specialized for each supported data type.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

static ArrowArray *
FUNCTION_NAME(gorilla_decompress_all, ELEMENT_TYPE)(CompressedGorillaData *gorilla_data)
{
	const bool has_nulls = gorilla_data->nulls != NULL;
	const int n_total =
		has_nulls ? gorilla_data->nulls->num_elements : gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const int n_total_padded =
		((n_total * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);

	const int n_notnull = gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total >= n_notnull);

	/* Unpack the basic compressed data parts. */
	Simple8bRleBitmap tag0s = simple8brle_bitmap_decompress(gorilla_data->tag0s);
	Simple8bRleBitmap tag1s = simple8brle_bitmap_decompress(gorilla_data->tag1s);

	BitArray leading_zeros_bitarray = gorilla_data->leading_zeros;
	BitArrayIterator leading_zeros_iterator;
	bit_array_iterator_init(&leading_zeros_iterator, &leading_zeros_bitarray);

	uint8 all_leading_zeros[MAX_NUM_LEADING_ZEROS_PADDED];
	const int16 leading_zeros_padded =
		unpack_leading_zeros_array(&gorilla_data->leading_zeros, all_leading_zeros);

	int16 num_bit_widths;
	const uint8 *restrict bit_widths =
		simple8brle_decompress_all_uint8(gorilla_data->num_bits_used_per_xor, &num_bit_widths);

	BitArray xors_bitarray = gorilla_data->xors;
	BitArrayIterator xors_iterator;
	bit_array_iterator_init(&xors_iterator, &xors_bitarray);

	/*
	 * Now decompress the non-null data.
	 *
	 * 1) unpack only the different elements (tag0 = 1) based on the tag1 array.
	 *
	 * 1a) Sanity check: the number of bit widths we have matches the
	 * number of 1s in the tag1s array.
	 */
	CheckCompressedData(simple8brle_bitmap_num_ones(&tag1s) == num_bit_widths);
	CheckCompressedData(simple8brle_bitmap_num_ones(&tag1s) <= leading_zeros_padded);

	/*
	 * 1b) Sanity check: the first tag1 must be 1, so that we initialize the bit
	 * widths.
	 */
	CheckCompressedData(simple8brle_bitmap_get_at(&tag1s, 0) == 1);

	/*
	 * 1c) Sanity check: can't have more different elements than notnull elements.
	 */
	const int n_different = tag1s.num_elements;
	CheckCompressedData(n_different <= n_notnull);

	/*
	 * 1d) Unpack.
	 *
	 * Note that the bit widths change often, so there's no sense in
	 * having a fast path for stretches of tag1 == 0.
	 */
	ELEMENT_TYPE prev = 0;
	int next_bit_widths_index = 0;
	int16 next_leading_zeros_index = 0;
	uint8 current_leading_zeros = 0;
	uint8 current_xor_bits = 0;
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total_padded);
	for (int i = 0; i < n_different; i++)
	{
		if (simple8brle_bitmap_get_at(&tag1s, i) != 0)
		{
			/* Load new bit widths. */
			Assert(next_bit_widths_index < num_bit_widths);
			current_xor_bits = bit_widths[next_bit_widths_index++];

			Assert(next_leading_zeros_index < MAX_NUM_LEADING_ZEROS_PADDED);
			current_leading_zeros = all_leading_zeros[next_leading_zeros_index++];

			/*
			 * More than 64 significant bits don't make sense. Exactly 64 we get for
			 * the first encoded number.
			 */
			CheckCompressedData(current_leading_zeros + current_xor_bits <= 64);

			/*
			 * Theoretically, leading zeros + xor bits == 0 would mean that the
			 * number is the same as the previous one, and it should have been
			 * encoded as tag0s == 0. Howewer, we can encounter it in the corrupt
			 * data. Shifting by 64 bytes left would be undefined behavior.
			 */
			CheckCompressedData(current_leading_zeros + current_xor_bits > 0);
		}

		const uint64 current_xor = bit_array_iter_next(&xors_iterator, current_xor_bits);
		prev ^= current_xor << (64 - (current_leading_zeros + current_xor_bits));
		decompressed_values[i] = prev;
	}
	Assert(next_bit_widths_index == num_bit_widths);

	/*
	 * 2) Fill out the stretches of repeated elements, encoded with tag0 = 0.
	 *
	 * 2a) Sanity check: number of different elements according to tag0s must be
	 * the same as number of different elements according to tag1s, so that the
	 * current_element doesn't underrun.
	 */
	CheckCompressedData(simple8brle_bitmap_num_ones(&tag0s) == n_different);

	/*
	 * 2b) Sanity check: tag0s[0] == 1 -- the first element of the sequence is
	 * always "different from the previous one".
	 */
	CheckCompressedData(simple8brle_bitmap_get_at(&tag0s, 0) == 1);

	/*
	 * 2b) Fill the repeated elements.
	 */
	int current_element = n_different - 1;
	for (int i = n_notnull - 1; i >= 0; i--)
	{
		Assert(current_element >= 0);
		Assert(current_element <= i);
		decompressed_values[i] = decompressed_values[current_element];

		if (simple8brle_bitmap_get_at(&tag0s, i))
		{
			current_element--;
		}
	}
	Assert(current_element == -1);

	/*
	 * We have unpacked the non-null data. Now reshuffle it to account for nulls,
	 * and fill the validity bitmap.
	 */
	const int validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);

	/*
	 * For starters, set the validity bitmap to all ones. We probably have less
	 * nulls than values, so this is faster.
	 */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	if (has_nulls)
	{
		/*
		 * We have decompressed the data with nulls skipped, reshuffle it
		 * according to the nulls bitmap.
		 */
		Simple8bRleBitmap nulls = simple8brle_bitmap_decompress(gorilla_data->nulls);
		CheckCompressedData(n_notnull + simple8brle_bitmap_num_ones(&nulls) == n_total);

		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			if (simple8brle_bitmap_get_at(&nulls, i))
			{
				arrow_set_row_validity(validity_bitmap, i, false);
			}
			else
			{
				Assert(current_notnull_element >= 0);
				decompressed_values[i] = decompressed_values[current_notnull_element];
				current_notnull_element--;
			}
		}

		Assert(current_notnull_element == -1);
	}
	else
	{
		/*
		 * The validity bitmap is padded at the end to a multiple of 64 bytes.
		 * Fill the padding with zeros, because the elements corresponding to
		 * the padding bits are not valid.
		 */
		for (int i = n_total; i < validity_bitmap_bytes * 8; i++)
		{
			arrow_set_row_validity(validity_bitmap, i, false);
		}
	}

	/* Return the result. */
	ArrowArray *result = palloc0(sizeof(ArrowArray));
	const void **buffers = palloc(sizeof(void *) * 2);
	buffers[0] = validity_bitmap;
	buffers[1] = decompressed_values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = n_total - n_notnull;
	return result;
}

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
