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
FUNCTION_NAME(gorilla_decompress_all, ELEMENT_TYPE)(CompressedGorillaData *gorilla_data,
													MemoryContext dest_mctx)
{
	const bool has_nulls = gorilla_data->nulls != NULL;
	const uint16 n_total =
		has_nulls ? gorilla_data->nulls->num_elements : gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	/*
	 * Pad the number of elements to multiple of 64 bytes if needed, so that we
	 * can work in 64-byte blocks.
	 */
	const uint16 n_total_padded =
		((n_total * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);

	/*
	 * We need additional padding at the end of buffer, because the code that
	 * converts the elements to postres Datum always reads in 8 bytes.
	 */
	const int buffer_bytes = n_total_padded * sizeof(ELEMENT_TYPE) + 8;
	ELEMENT_TYPE *restrict decompressed_values = MemoryContextAlloc(dest_mctx, buffer_bytes);

	const uint16 n_notnull = gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total >= n_notnull);

	/* Unpack the basic compressed data parts. */
	Simple8bRleBitmap tag0s = simple8brle_bitmap_prefixsums(gorilla_data->tag0s);
	Simple8bRleBitmap tag1s = simple8brle_bitmap_prefixsums(gorilla_data->tag1s);

	BitArray leading_zeros_bitarray = gorilla_data->leading_zeros;
	BitArrayIterator leading_zeros_iterator;
	bit_array_iterator_init(&leading_zeros_iterator, &leading_zeros_bitarray);

	uint8 all_leading_zeros[MAX_NUM_LEADING_ZEROS_PADDED_N64];
	const uint16 leading_zeros_padded =
		unpack_leading_zeros_array(&gorilla_data->leading_zeros, all_leading_zeros);

	uint8 bit_widths[MAX_NUM_LEADING_ZEROS_PADDED_N64];
	const uint16 num_bit_widths =
		simple8brle_decompress_all_buf_uint8(gorilla_data->num_bits_used_per_xor,
											 bit_widths,
											 MAX_NUM_LEADING_ZEROS_PADDED_N64);

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
	CheckCompressedData(simple8brle_bitmap_prefix_sum(&tag1s, 0) == 1);

	/*
	 * 1c) Sanity check: can't have more different elements than notnull elements.
	 */
	const uint16 n_different = tag1s.num_elements;
	CheckCompressedData(n_different <= n_notnull);

	/*
	 * 1d) Unpack.
	 *
	 * Note that the bit widths change often, so there's no sense in
	 * having a fast path for stretches of tag1 == 0.
	 */
	ELEMENT_TYPE prev = 0;
	for (uint16 i = 0; i < n_different; i++)
	{
		const uint8 current_xor_bits = bit_widths[simple8brle_bitmap_prefix_sum(&tag1s, i) - 1];
		const uint8 current_leading_zeros =
			all_leading_zeros[simple8brle_bitmap_prefix_sum(&tag1s, i) - 1];

		/*
		 * Truncate the shift here not to cause UB on the corrupt data.
		 */
		const uint8 shift = (64 - (current_xor_bits + current_leading_zeros)) & 63;

		const uint64 current_xor = bit_array_iter_next(&xors_iterator, current_xor_bits);
		prev ^= current_xor << shift;
		decompressed_values[i] = prev;
	}

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
	CheckCompressedData(simple8brle_bitmap_prefix_sum(&tag0s, 0) == 1);

	/*
	 * 2b) Fill the repeated elements.
	 */
	for (int i = n_notnull - 1; i >= 0; i--)
	{
		decompressed_values[i] = decompressed_values[simple8brle_bitmap_prefix_sum(&tag0s, i) - 1];
	}

	/*
	 * We have unpacked the non-null data. Now reshuffle it to account for nulls,
	 * and fill the validity bitmap.
	 */
	const int validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = MemoryContextAlloc(dest_mctx, validity_bitmap_bytes);

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
		 * The validity bitmap size is a multiple of 64 bits. Fill the tail bits
		 * with zeros, because the corresponding elements are not valid.
		 */
		if (n_total % 64)
		{
			const uint64 tail_mask = -1ULL >> (64 - n_total % 64);
			validity_bitmap[n_total / 64] &= tail_mask;

#ifdef USE_ASSERT_CHECKING
			for (int i = 0; i < 64; i++)
			{
				Assert(arrow_row_is_valid(validity_bitmap, (n_total / 64) * 64 + i) ==
					   (i < n_total % 64));
			}
#endif
		}
	}

	/* Return the result. */
	ArrowArray *result = MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + sizeof(void *) * 2);
	const void **buffers = (const void **) &result[1];
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
