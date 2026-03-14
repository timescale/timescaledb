/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * gx4 variant of gorilla_impl.c — gx3 + bitarray null scatter (from dd6).
 *
 * Same as gx3 (O4 hybrid: gx1 Pass 1 + gx2 Pass 2) but replaces the
 * Pass 3 null scatter:
 *
 *   Pass 3 (null scatter) — from dd6:
 *     Use simple8brle_bitarray_decompress(nulls, inverted=true) to produce
 *     the ArrowArray validity bitmap directly as a uint64[] bitarray.
 *     Eliminates: MemoryContextAlloc for validity_bitmap, memset(0xFF),
 *     tail masking, simple8brle_bitmap_decompress, per-element
 *     simple8brle_bitmap_get_at, and arrow_set_row_validity calls.
 *     The bitarray IS the validity bitmap — use it directly.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

static ArrowArray *
FUNCTION_NAME(gorilla_decompress_all, ELEMENT_TYPE)(CompressedGorillaData *gorilla_data,
												    MemoryContext dest_mctx)
{
	const bool has_nulls = gorilla_data->nulls != NULL;
	const uint32 n_total =
		has_nulls ? gorilla_data->nulls->num_elements : gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const uint32 n_total_padded =
		((n_total * sizeof(ELEMENT_TYPE) + 63) / 64) * 64 / sizeof(ELEMENT_TYPE);
	Assert(n_total_padded >= n_total);

	const int buffer_bytes = n_total_padded * sizeof(ELEMENT_TYPE) + 8;
	ELEMENT_TYPE *restrict decompressed_values = MemoryContextAlloc(dest_mctx, buffer_bytes);

	const uint32 n_notnull = gorilla_data->tag0s->num_elements;
	CheckCompressedData(n_total >= n_notnull);

	/* ================================================================
	 * Pass 1 — Pre-materialized forward XOR decode (from gx1).
	 *
	 * Pre-materialize tag1 bools, leading_zeros, and bit_widths into
	 * flat arrays, then run a tight loop with incremental rank.
	 * ================================================================ */

	/* Decompress tag1 bitmap into bool array (1 byte/element). */
	const Simple8bRleBitmap tag1_bools = simple8brle_bitmap_decompress(gorilla_data->tag1s);
	const bool *restrict tag1_data = (const bool *) tag1_bools.data;

	/* Pre-materialize leading_zeros (6-bit packed → uint8[]). */
	uint32 num_leading_zeros_padded;
	const uint8 *all_leading_zeros =
		unpack_leading_zeros_array(&gorilla_data->leading_zeros, &num_leading_zeros_padded);

	/* Pre-materialize num_bits_used (Simple8b → uint8[]). */
	uint32 num_bit_widths;
	const uint8 *bit_widths =
		simple8brle_decompress_all_uint8(gorilla_data->num_bits_used_per_xor, &num_bit_widths);

	/* xors: variable-width values from BitArray, read every element. */
	BitArray xors_bitarray = gorilla_data->xors;
	BitArrayIterator xors_iterator;
	bit_array_iterator_init(&xors_iterator, &xors_bitarray);

	/* Sanity checks. */
	CheckCompressedData(simple8brle_bitmap_num_ones(&tag1_bools) == num_bit_widths);
	CheckCompressedData(simple8brle_bitmap_num_ones(&tag1_bools) <= num_leading_zeros_padded);
	CheckCompressedData(tag1_data[0] == true);

	const uint16 n_different = tag1_bools.num_elements;
	CheckCompressedData(n_different <= n_notnull);

	/*
	 * Tight flat loop with incremental rank over tag1 bool array.
	 * On tag1=1: advance rank, look up new bit-widths from arrays.
	 * On tag1=0: rank unchanged, reuse previous bit-widths.
	 * Either way: read XOR bits from BitArray, apply to prev.
	 */
	ELEMENT_TYPE prev = 0;
	uint16 tag1_rank = 0;
	for (uint16 i = 0; i < n_different; i++)
	{
		tag1_rank += tag1_data[i];

		const uint8 current_xor_bits = bit_widths[tag1_rank - 1];
		const uint8 current_leading_zeros = all_leading_zeros[tag1_rank - 1];

		/*
		 * Truncate the shift here not to cause UB on the corrupt data.
		 */
		const uint8 shift = (64 - (current_xor_bits + current_leading_zeros)) & 63;

		const uint64 current_xor = bit_array_iter_next(&xors_iterator, current_xor_bits);
		prev ^= current_xor << shift;
		decompressed_values[i] = prev;
	}

	/* Free tag1 bool array (no longer needed after Pass 1). */
	pfree((void *) tag1_bools.data);

	/* ================================================================
	 * Pass 2 — Fused backward repeat fill (from gx2).
	 *
	 * Walk tag0s blocks in reverse.  On RLE(0) blocks, bulk-fill with
	 * the current distinct value (the main win for high-compression
	 * data where most elements are repeats).
	 * ================================================================ */

	Simple8bRleSerialized *tag0s = gorilla_data->tag0s;
	const uint32 tag0_num_sel_slots =
		simple8brle_num_selector_slots_for_num_blocks(tag0s->num_blocks);
	const uint64 *tag0_sel_slots = tag0s->slots;
	const uint64 *tag0_data_blocks = tag0s->slots + tag0_num_sel_slots;

	/*
	 * Pre-scan to find total stored elements (needed to compute how many
	 * elements in the last block are padding).
	 */
	uint32 total_stored = 0;
	for (uint32 b = 0; b < tag0s->num_blocks; b++)
	{
		const uint8 sel =
			(tag0_sel_slots[b / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT] >>
			 ((b % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT) * SIMPLE8B_BITS_PER_SELECTOR)) &
			0xF;
		if (simple8brle_selector_is_rle(sel))
			total_stored += simple8brle_rledata_repeatcount(tag0_data_blocks[b]);
		else
			total_stored += SIMPLE8B_NUM_ELEMENTS[sel];
	}
	const uint32 skipped_in_last = total_stored - tag0s->num_elements;

	int distinct_idx = n_different;
	int epos = (int)n_notnull - 1;

	for (int b = (int)tag0s->num_blocks - 1; b >= 0 && epos >= 0; b--)
	{
		const uint8 sel =
			(tag0_sel_slots[b / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT] >>
			 ((b % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT) * SIMPLE8B_BITS_PER_SELECTOR)) &
			0xF;
		const uint64 block_data = tag0_data_blocks[b];

		uint32 block_elems;
		if (simple8brle_selector_is_rle(sel))
			block_elems = simple8brle_rledata_repeatcount(block_data);
		else
			block_elems = SIMPLE8B_NUM_ELEMENTS[sel];

		/* Last block may have padding from the Simple8b encoder. */
		if (b == (int)tag0s->num_blocks - 1)
			block_elems -= skipped_in_last;

		if (simple8brle_selector_is_rle(sel))
		{
			const bool value = simple8brle_rledata_value(block_data) & 1;

			if (!value)
			{
				/*
				 * ★ RLE(0): bulk fill — all elements in this block are
				 * repeats of the current distinct value.  Load once, fill.
				 *
				 * Forward fill (O2 from doc 114) enables auto-vectorization:
				 * the compiler can emit vmovdqu stores (4 uint64s per AVX2
				 * store) instead of scalar backward stores.
				 */
				const uint32 n = (block_elems <= (uint32)(epos + 1))
								? block_elems : (uint32)(epos + 1);
				const ELEMENT_TYPE fill_val = decompressed_values[distinct_idx - 1];
				ELEMENT_TYPE *restrict dst = decompressed_values + epos - (int)n + 1;
				for (uint32 j = 0; j < n; j++)
					dst[j] = fill_val;
				epos -= (int)n;
			}
			else
			{
				/* RLE(1): each element is a new distinct value. */
				const uint32 n = (block_elems <= (uint32)(epos + 1))
								? block_elems : (uint32)(epos + 1);
				for (uint32 j = 0; j < n; j++)
				{
					decompressed_values[epos - j] = decompressed_values[distinct_idx - 1];
					distinct_idx--;
				}
				epos -= (int)n;
			}
		}
		else
		{
			/* Packed block: process bits in reverse order. */
			CheckCompressedData(sel == 1);
			const uint32 n = (block_elems <= (uint32)(epos + 1))
							? block_elems : (uint32)(epos + 1);
			int j = (int)block_elems - 1;

			for (uint32 k = 0; k < n; k++, j--, epos--)
			{
				decompressed_values[epos] = decompressed_values[distinct_idx - 1];
				distinct_idx -= (int)((block_data >> j) & 1);
			}
		}
	}

	/* Sanity: consumed exactly n_different distinct values. */
	CheckCompressedData(distinct_idx == 0);

	/* ================================================================
	 * Pass 3 — Null scatter via bitarray (from dd6).
	 *
	 * Use simple8brle_bitarray_decompress with inverted=true to produce
	 * the validity bitmap directly.  No separate alloc/memset/tail-mask.
	 * ================================================================ */

	uint64 *restrict validity_bitmap = NULL;
	if (has_nulls)
	{
		/*
		 * Decompress nulls directly into a validity bitmap (1=valid, 0=null).
		 * The bitarray IS the ArrowArray validity bitmap — use it directly.
		 */
		Simple8bRleBitArray validity_bits =
			simple8brle_bitarray_decompress(gorilla_data->nulls, /* inverted */ true);
		validity_bitmap = validity_bits.data;

		/*
		 * With inverted=true, num_ones counts valid (non-null) elements.
		 */
		CheckCompressedData(n_notnull == validity_bits.num_ones);

		/*
		 * Backward scatter: move non-null values from their compact
		 * positions to their final positions, leaving gaps for nulls.
		 * The validity bitmap is already correct — no bit manipulation.
		 */
		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			if (!((validity_bitmap[i / 64] >> (i % 64)) & 1))
			{
				/* Position i is null — nothing to do (bitmap already 0). */
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
