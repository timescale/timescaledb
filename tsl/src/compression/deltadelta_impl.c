/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Decompress the entire batch of deltadelta-compressed rows into an Arrow array.
 * Specialized for each supported data type.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

static ArrowArray *
FUNCTION_NAME(delta_delta_decompress_all, ELEMENT_TYPE)(Datum compressed)
{
	StringInfoData si = { .data = DatumGetPointer(compressed), .len = VARSIZE(compressed) };
	DeltaDeltaCompressed *header = consumeCompressedData(&si, sizeof(DeltaDeltaCompressed));
	Simple8bRleSerialized *deltas_compressed = bytes_deserialize_simple8b_and_advance(&si);

	const bool has_nulls = header->has_nulls == 1;

	Assert(header->has_nulls == 0 || header->has_nulls == 1);

	/* Can't use element type here because of zig-zag encoding. */
	int16 num_deltas;
	const uint64 *restrict deltas_zigzag =
		simple8brle_decompress_all_uint64(deltas_compressed, &num_deltas);

	Simple8bRleBitmap nulls = { 0 };
	if (has_nulls)
	{
		Simple8bRleSerialized *nulls_compressed = bytes_deserialize_simple8b_and_advance(&si);
		nulls = simple8brle_bitmap_decompress(nulls_compressed);
	}

	const int n_total = has_nulls ? nulls.num_elements : num_deltas;
	const int n_total_padded =
		((n_total * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	const int n_notnull = num_deltas;
	const int n_notnull_padded =
		((n_notnull * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);
	Assert(n_notnull_padded >= n_notnull);
	Assert(n_total >= n_notnull);
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const int validity_bitmap_bytes = sizeof(uint64) * ((n_total + 64 - 1) / 64);
	uint64 *restrict validity_bitmap = palloc(validity_bitmap_bytes);
	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_total_padded);

	/* Now fill the data w/o nulls. */
	ELEMENT_TYPE current_delta = 0;
	ELEMENT_TYPE current_element = 0;
	/*
	 * Manual unrolling speeds up this loop by about 10%. clang vectorizes
	 * the zig_zag_decode part, but not the double-prefix-sum part.
	 *
	 * Also tried using SIMD prefix sum from here twice:
	 * https://en.algorithmica.org/hpc/algorithms/prefix/, it's slower.
	 */
#define INNER_LOOP_SIZE 8
	Assert(n_notnull_padded % INNER_LOOP_SIZE == 0);
	for (int outer = 0; outer < n_notnull_padded; outer += INNER_LOOP_SIZE)
	{
		for (int inner = 0; inner < INNER_LOOP_SIZE; inner++)
		{
			current_delta += zig_zag_decode(deltas_zigzag[outer + inner]);
			current_element += current_delta;
			decompressed_values[outer + inner] = current_element;
		}
	}
#undef INNER_LOOP_SIZE

	/* All data valid by default, we will fill in the nulls later. */
	memset(validity_bitmap, 0xFF, validity_bitmap_bytes);

	/* Now move the data to account for nulls, and fill the validity bitmap. */
	if (has_nulls)
	{
		/*
		 * The number of not-null elements we have must be consistent with the
		 * nulls bitmap.
		 */
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
