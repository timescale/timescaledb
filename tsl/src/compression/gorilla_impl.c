/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

#define INNER_SIZE 8

pg_attribute_always_inline static void FUNCTION_NAME(prefix_xor_per_segment, ELEMENT_TYPE)(ELEMENT_TYPE *a, int n)
{
	const int logn = 31 - __builtin_clz(INNER_SIZE);

	Assert(n % INNER_SIZE == 0);
	for (int segment = 0; segment < n; segment += INNER_SIZE)
	{
//		ELEMENT_TYPE tmp[64 / sizeof(ELEMENT_TYPE)];
//		for (int i = 0; i < inner_size; i++)
//		{
//			tmp[i] = a[segment + i];
//		}
//
//		for (int i = 1; i < inner_size; i++)
//		{
//			tmp[i] ^= tmp[i - 1];
//		}

		for (int l = 0; l < logn; l++)
		{
			ELEMENT_TYPE accu[INNER_SIZE] = {0};

////			for (int i = 0; i < (1 << l); i++)
////			{
////				// prefix should be ready?
////				Assert(a[segment + i] == tmp[i]);
////			}
//
//			for (int i = (1 << l); i < inner_size; i++)
//			{
////				if (segment == 0)
////				{
////					fprintf(stderr, "a[%d] += a[%d] (%16lX ^ %16lX = %16lX)\n",
////							segment + i,
////							segment + i - (1 << l),
////						(unsigned long) a[segment + i],
////						(unsigned long) a[segment + i - (1 << l)],
////						(unsigned long) a[segment + i] ^ a[segment + i - (1 << l)]);
////				}
//				accu[i] = a[segment + i] ^ a[segment + i - (1 << l)];
//			}
//
//			for (int i = (1 << l); i < inner_size; i++)
//			{
//				a[segment + i] ^= accu[i];
//			}

// #define LOOP \
// 			for (int i = (1 << l); i < INNER_SIZE; i++) accu[i] = a[segment + i - (1 << l)]; \
// 			for (int i = 0; i < INNER_SIZE; i++) a[segment + i] ^= accu[i]; \
// 			break;

#define LOOP \
			for (int i = 0; i < INNER_SIZE; i++) accu[i] = a[segment + i]; \
			for (int i = INNER_SIZE - 1; i >= 0; i--) accu[i] = (i >= (1 << l)) ? accu[i - (1 << l)] : 0; \
			for (int i = 0; i < INNER_SIZE; i++) a[segment + i] ^= accu[i]; \
			break;

			switch (l)
			{
				case 0: { LOOP }
				case 1: { LOOP }
				case 2: { LOOP }
				case 3: { LOOP }
				default: Assert(false);
			}
#undef LOOP
		}

//		for (int i = 0; i < inner_size; i++)
//		{
//			Assert(a[segment + i] == tmp[i]);
//		}
	}

//	fprintf(stderr, "\n\n");
}

pg_attribute_always_inline static void FUNCTION_NAME(prefix_xor_accumulate, ELEMENT_TYPE)(ELEMENT_TYPE *a, int n)
{
	Assert(n % INNER_SIZE == 0);
	for (int segment = INNER_SIZE; segment < n; segment += INNER_SIZE)
	{
		ELEMENT_TYPE accu[INNER_SIZE];
		for (int i = 0; i < INNER_SIZE; i++)
		{
			accu[i] = a[segment - 1];
		}

		for (int i = 0; i < INNER_SIZE; i++)
		{
			a[segment + i] ^= accu[i];
		}
	}
}

void FUNCTION_NAME(prefix_xor_combine, ELEMENT_TYPE)(ELEMENT_TYPE *a, int n);
void FUNCTION_NAME(prefix_xor_combine, ELEMENT_TYPE)(ELEMENT_TYPE *a, int n)
{
	FUNCTION_NAME(prefix_xor_per_segment, ELEMENT_TYPE)(a, n);
	FUNCTION_NAME(prefix_xor_accumulate, ELEMENT_TYPE)(a, n);
}

static ArrowArray *
FUNCTION_NAME(gorilla_decompress_all, ELEMENT_TYPE)(DecompressionIterator *iter_base)
{
	Assert(iter_base->compression_algorithm == COMPRESSION_ALGORITHM_GORILLA);
	GorillaDecompressionIterator *iter = (GorillaDecompressionIterator *) iter_base;

	const bool has_nulls = iter->has_nulls;
	const int n_total = has_nulls ? iter->nulls.num_elements : iter->tag0s.num_elements;
	const int n_total_padded = ((n_total * sizeof(ELEMENT_TYPE) + 63) / 64 ) * 64 / sizeof(ELEMENT_TYPE);
	const int n_notnull = iter->tag0s.num_elements;
	const int n_notnull_padded = ((n_notnull * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);
	Assert(n_notnull_padded >= n_notnull);
	Assert(n_total >= n_notnull);
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	Assert(iter->tag0s.current_element == 0);

	const uint32 validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);
	ELEMENT_TYPE *restrict decompressed_values = palloc0(sizeof(ELEMENT_TYPE) * n_total_padded);

	/* All data valid by default, we will fill in the nulls later. */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	BitArray leading_zeros_bitarray = iter->gorilla_data.leading_zeros;
	BitArrayIterator leading_zeros_iterator;
	bit_array_iterator_init(&leading_zeros_iterator, &leading_zeros_bitarray);
//	uint8 leading_zeros[GLOBAL_MAX_ROWS_PER_COMPRESSION];
//	int num_leading_zero_values = bit_array_num_bits(&leading_zeros_bitarray) / BITS_PER_LEADING_ZEROS;
//	Assert(bit_array_num_bits(&leading_zeros_bitarray) % BITS_PER_LEADING_ZEROS == 0);
//	for (int i = 0; i < num_leading_zero_values; i++)
//	{
//		/*
//		 * FIXME not vectorized: non-reduction used outside loop, cannot
//		 * determine number of iterations.
//		 * Write a special function for bulk unpacking?
//		 */
//		leading_zeros[i] = bit_array_iter_next(&leading_zeros_iterator, BITS_PER_LEADING_ZEROS);
//	}
//	int leading_zeros_index = -1;

	/* Now fill the data w/o nulls. */
	Simple8bRleBitmap tag0s = iter->tag0s;
	Simple8bRleBitmap tag1s = iter->tag1s;
	Simple8bRleDecompressionIterator num_bits_used = iter->num_bits_used;
	Simple8bRleDecompressResult num_xor_bits;
	ELEMENT_TYPE prev = 0;
	uint8_t prev_leading_zeros = 0;
	for (int i = 0; i < n_notnull; i++)
	{
		Simple8bRleDecompressResult tag0 = simple8brle_bitmap_get_next(&tag0s);
		Assert(!tag0.is_done);
		if (tag0.val == 0)
		{
			/* Repeat previous value. */
			decompressed_values[i] = 0;
			break;
		}

		Simple8bRleDecompressResult tag1 = simple8brle_bitmap_get_next(&tag1s);
		Assert(!tag1.is_done);
		if (unlikely(tag1.val == 1))
		{
			/* Load new bit widths. */
			num_xor_bits = simple8brle_decompression_iterator_try_next_forward(&num_bits_used);
			Assert(!num_xor_bits.is_done);
			Assert(num_xor_bits.val >= 0 && num_xor_bits.val <= 64);
			/*
			 * FIXME unpacking of the bit array can be done separately, regardless
			 * of the tags? At least the tag0. Maybe leading_zeros can also be
			 * unpacked separately.
			 *
			 *
			 * FIXME could put this iterator on stack? Accessing fields like
			 * bits_used_in_current_bucket goes to memory.
			 */
			prev_leading_zeros = bit_array_iter_next(&leading_zeros_iterator, BITS_PER_LEADING_ZEROS);
		}

		/* Reuse previous bit widths. */
		prev ^= bit_array_iter_next(&iter->xors, num_xor_bits.val)
				<< (64 - (prev_leading_zeros + num_xor_bits.val));
		decompressed_values[i] = prev;
	}
	// Assert(leading_zeros_index == num_leading_zero_values - 1);
	Assert(simple8brle_decompression_iterator_try_next_forward(&num_bits_used).is_done);

//	ELEMENT_TYPE *dd2 = palloc0(sizeof(ELEMENT_TYPE) * n_total_padded);
//	memcpy(dd2, decompressed_values, sizeof(ELEMENT_TYPE) * n_total_padded);

//	FUNCTION_NAME(prefix_xor_combine, ELEMENT_TYPE)(decompressed_values, n_notnull_padded);

//	for (int i = 1; i < n_total_padded; i++)
//	{
//		dd2[i] ^= dd2[i - 1];
//	}
//
//	for (int i = 0; i < n_total_padded; i++)
//	{
//		Assert(dd2[i] == decompressed_values[i]);
//	}

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

#undef INNER_SIZE

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
