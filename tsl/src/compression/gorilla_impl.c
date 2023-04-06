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
	if (n_total > GLOBAL_MAX_ROWS_PER_COMPRESSION)
	{
		/* Don't allocate too much if we got corrupt data or something. */
		ereport(ERROR,
				(errmsg("the number of elements in compressed data %d is larger than the maximum "
						"allowed %d",
						n_total,
						GLOBAL_MAX_ROWS_PER_COMPRESSION)));
	}

	/* Unpack the basic compressed data parts. */
	Simple8bRleBitmap tag0s = simple8brle_decompress_bitmap(gorilla_data->tag0s);
	Simple8bRleBitmap tag1s = simple8brle_decompress_bitmap(gorilla_data->tag1s);

	BitArray leading_zeros_bitarray = gorilla_data->leading_zeros;
	BitArrayIterator leading_zeros_iterator;
	bit_array_iterator_init(&leading_zeros_iterator, &leading_zeros_bitarray);

	uint8_t all_leading_zeros[((GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64) * 64];
	unpack_leading_zeros_array(&gorilla_data->leading_zeros, all_leading_zeros);

	Simple8bRleDecompressionIterator num_bits_used;
	simple8brle_decompression_iterator_init_forward(&num_bits_used,
													gorilla_data->num_bits_used_per_xor);

	BitArray xors_bitarray = gorilla_data->xors;
	BitArrayIterator xors_iterator;
	bit_array_iterator_init(&xors_iterator, &xors_bitarray);

	//	/* Definitely slows things down, but why? Generated code is more simple. */
	//	SimpleBitArrayIterator simple_xors_iterator = {
	//		.start_bit_absolute = 0,
	//		.data = gorilla_data->xors.buckets.data,
	//		.this_word = gorilla_data->xors.buckets.data[0],
	//	};

	/* The first tag1 must be 1, so that we initialize the bit widths. */
	if (simple8brle_bitmap_get_at(&tag1s, 0) == 0)
	{
		ereport(ERROR, (errmsg("the compressed data is corrupt")));
	}

	/*
	 * Now decompress the non-null data.
	 *
	 * First, unpack only the different elements (tag0 = 1) based on the tag1
	 * array. Note that the bit widths change often, so there's no sense in
	 * having a fast path for stretches of tag1 == 0.
	 */
	ELEMENT_TYPE prev = 0;
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total_padded);
	int next_leading_zeros_index = 0;
	uint8 current_leading_zeros = 0;
	uint8 current_xor_bits = 0;
	const int n_different = tag1s.num_elements;
	for (int i = 0; i < n_different; i++)
	{
		if (simple8brle_bitmap_get_at(&tag1s, i) != 0)
		{
			/* Load new bit widths. */
			Simple8bRleDecompressResult num_xor_bits =
				simple8brle_decompression_iterator_try_next_forward(&num_bits_used);
			Assert(!num_xor_bits.is_done);
			Assert(num_xor_bits.val <= 64);

			current_xor_bits = num_xor_bits.val;
			current_leading_zeros = all_leading_zeros[next_leading_zeros_index++];
		}

		const uint64 current_xor = bit_array_iter_next(&xors_iterator, current_xor_bits);
		prev ^= current_xor << (64 - (current_leading_zeros + current_xor_bits));
		decompressed_values[i] = prev;
	}
	Assert(simple8brle_decompression_iterator_try_next_forward(&num_bits_used).is_done);

	/*
	 * Fill out the stretches of repeated elements, encoded with tag0 = 0.
	 *
	 * FIXME need a sanity check on number of set bits in tag0s, so that the
	 * current_element doesn't underrun.
	 */
	int current_element = n_different - 1;
	for (int i = n_notnull - 1; i >= 0; i--)
	{
		Assert(i >= current_element);
		if (simple8brle_bitmap_get_at(&tag0s, i) == 0)
		{
			/* Repeat this element. */
			decompressed_values[i] = decompressed_values[current_element];
		}
		else
		{
			/* Move to another element. */
			decompressed_values[i] = decompressed_values[current_element--];
		}
	}
	Assert(current_element == -1);

	/*
	 * We have unpacked the non-null data. Now reshuffle it to account for nulls,
	 * and fill the validity bitmap.
	 */
	const int validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);

	/* For starters, set the validity bitmap to all ones. We probably have less
	 * nulls than values, so this is faster. */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	if (has_nulls)
	{
		/*
		 * We have decompressed the data with nulls skipped, reshuffle it
		 * according to the nulls bitmap.
		 */
		Simple8bRleBitmap nulls = simple8brle_decompress_bitmap(gorilla_data->nulls);
		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			if (simple8brle_bitmap_get_at(&nulls, i))
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
	else
	{
		/*
		 * The validity bitmap is padded at the end to a multiple of 64 bytes.
		 * Fill the padding with zeros, because the elements corresponding to
		 * the padding bits are not valid.
		 */
		for (int i = n_total; i < validity_bitmap_bytes * 8; i++)
		{
			arrow_validity_bitmap_set(validity_bitmap, i, false);
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
	result->null_count = -1;
	return result;
}

#undef INNER_SIZE

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
