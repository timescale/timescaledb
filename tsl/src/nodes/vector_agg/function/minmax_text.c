/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
extern VectorAggFunctions FUNCTION_NAME(argdef);
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	/*
	 * Text min/max uses memcmp for comparison, which only produces correct
	 * ordering for C collation. For non-C collations, return NULL to fall
	 * back to the Postgres aggregation.
	 */
	if (lc_collate_is_c(collation))
		return &FUNCTION_NAME(argdef);
	return NULL;
#else

typedef MinMaxBytesState FUNCTION_NAME(state);

static pg_attribute_always_inline void
FUNCTION_NAME(one)(void *restrict agg_state, const BytesView new_value)
{
	FUNCTION_NAME(state) *restrict state = (FUNCTION_NAME(state) *) agg_state;

	/*
	 * If current value is null, we replace it with the new value, otherwise we
	 * have to check the predicate.
	 */
	bool replace = state->capacity == 0;
	if (likely(!replace))
	{
		const uint32 current_len = VARSIZE(state->data) - VARHDRSZ;
		const int result =
			memcmp(VARDATA(state->data), new_value.data, Min(current_len, new_value.len));
		if (result == 0)
		{
			replace = PREDICATE(current_len, new_value.len);
		}
		else
		{
			replace = PREDICATE(result, 0);
		}
	}

	if (replace)
	{
		const uint32 new_vardata_bytes = new_value.len + VARHDRSZ;
		if (new_vardata_bytes > state->capacity)
		{
			/*
			 * Reallocate to closest power of two to amortize the costs. Varlena
			 * is limited to 2^30 - 1 bytes.
			 */
			Assert(new_vardata_bytes < INT32_MAX / 2);
			const int lowest_power = pg_leftmost_one_pos32(new_vardata_bytes * 2 - 1);
			const int new_capacity = 1ULL << lowest_power;
			state->data = palloc(new_capacity);
			state->capacity = new_capacity;
		}
		SET_VARSIZE(state->data, new_vardata_bytes);
		memcpy(VARDATA(state->data), new_value.data, new_value.len);

		Assert(state->capacity > 0);
		Assert(VARSIZE(state->data) <= state->capacity);
	}
}

static void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *arrow, const uint64 *filter,
					  MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(state) *restrict state = (FUNCTION_NAME(state) *) agg_state;

	const int16 *body_offset_indexes = arrow->dictionary ? arrow->buffers[1] : NULL;
	const uint8 *bodies = arrow->dictionary ? arrow->dictionary->buffers[2] : arrow->buffers[2];
	const uint32 *body_offsets =
		arrow->dictionary ? arrow->dictionary->buffers[1] : arrow->buffers[1];

	const int n = arrow->length;
	for (int row = 0; row < n; row++)
	{
		const int body_offset_index = body_offset_indexes == NULL ? row : body_offset_indexes[row];
		const int body_offset = body_offsets[body_offset_index];
		const int body_bytes = body_offsets[body_offset_index + 1] - body_offset;
		const BytesView value = { .data = &bodies[body_offset], .len = body_bytes };

		if (arrow_row_is_valid(filter, row))
		{
			FUNCTION_NAME(one)(state, value);
		}
	}
}

static void
FUNCTION_NAME(many_vector)(void *restrict agg_states, const uint32 *state_indices,
						   const uint64 *filter, int start_row, int end_row,
						   const ArrowArray *arrow, MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(state) *restrict states = (FUNCTION_NAME(state) *) agg_states;
	const int16 *body_offset_indexes = arrow->dictionary ? arrow->buffers[1] : NULL;
	const uint8 *bodies = arrow->dictionary ? arrow->dictionary->buffers[2] : arrow->buffers[2];
	const uint32 *body_offsets =
		arrow->dictionary ? arrow->dictionary->buffers[1] : arrow->buffers[1];

	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	for (int row = start_row; row < end_row; row++)
	{
		FUNCTION_NAME(state) *restrict state = &states[state_indices[row]];

		const int body_offset_index = body_offset_indexes == NULL ? row : body_offset_indexes[row];
		const int body_offset = body_offsets[body_offset_index];
		const int body_bytes = body_offsets[body_offset_index + 1] - body_offset;
		const BytesView value = { .data = &bodies[body_offset], .len = body_bytes };

		if (arrow_row_is_valid(filter, row))
		{
			Assert(state_indices[row] != 0);
			FUNCTION_NAME(one)(state, value);
		}
	}
	MemoryContextSwitchTo(old);
}

static void
FUNCTION_NAME(scalar)(void *agg_state, Datum constvalue, bool constisnull, int n,
					  MemoryContext agg_extra_mctx)
{
	if (constisnull)
	{
		return;
	}

	BytesView value = { .data = (const uint8 *) VARDATA_ANY(constvalue),
						.len = VARSIZE_ANY_EXHDR(constvalue) };
	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	FUNCTION_NAME(one)(agg_state, value);
	MemoryContextSwitchTo(old);
}

VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(MinMaxBytesState),
	.agg_init = minmax_bytes_init,
	.agg_emit = minmax_bytes_emit,
	.agg_scalar = FUNCTION_NAME(scalar),
	.agg_vector = FUNCTION_NAME(vector),
	.agg_many_vector = FUNCTION_NAME(many_vector),
};
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM

#undef PREDICATE
#undef AGG_NAME
