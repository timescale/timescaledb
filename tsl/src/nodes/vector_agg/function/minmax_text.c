/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
extern VectorAggFunctions FUNCTION_NAME(argdef);
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else

typedef struct
{
	uint32 capacity;
	struct varlena *data;
} MinMaxBytesState;

typedef struct BytesView
{
	const uint8 *data;
	uint32 len;
} BytesView;

typedef MinMaxBytesState FUNCTION_NAME(state);

static void
minmax_bytes_init(void *restrict agg_states, int n)
{
	MinMaxBytesState *restrict states = (MinMaxBytesState *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i].capacity = 0;
		states[i].data = NULL;
	}
}

static void
minmax_bytes_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	MinMaxBytesState *state = (MinMaxBytesState *) agg_state;
	*out_isnull = state->capacity == 0;
	*out_result = PointerGetDatum(state->data);
}

static pg_attribute_always_inline void
FUNCTION_NAME(one)(void *restrict agg_state, const BytesView value)
{
	FUNCTION_NAME(state) *restrict state = (FUNCTION_NAME(state) *) agg_state;
	bool replace = state->capacity == 0;
	if (!replace)
	{
		const uint32 current_len = VARSIZE(state->data) - VARHDRSZ;
		const int result = memcmp(VARDATA(state->data), value.data, Min(current_len, value.len));
		if (result == 0)
		{
			replace = PREDICATE(current_len, value.len);
		}
		else
		{
			replace = PREDICATE(result, 0);
		}
	}

	if (replace)
	{
		const uint32 new_vardata_bytes = value.len + VARHDRSZ;
		if (new_vardata_bytes > state->capacity)
		{
			const int lowest_power = pg_leftmost_one_pos32(new_vardata_bytes * 2 - 1);
			const int new_capacity = 1ULL << lowest_power;
			state->data = palloc(new_capacity);
			state->capacity = new_capacity;
		}
		SET_VARSIZE(state->data, new_vardata_bytes);
		memcpy(VARDATA(state->data), value.data, value.len);

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

	//	fprintf(stderr, "range [%d, %d), dictionary %p\n", start_row, end_row, arrow->dictionary);

	//	for (int row = start_row; row < end_row; row++)
	//	{
	//		if (body_offsets[row + 1] < body_offsets[row])
	//		{
	//			fprintf(stderr, "[%d] oops!!! %d -> %d\n",
	//				row, body_offsets[row], body_offsets[row + 1]);
	//		}
	//	}

	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	for (int row = start_row; row < end_row; row++)
	{
		FUNCTION_NAME(state) *restrict state = &states[state_indices[row]];

		const int body_offset_index = body_offset_indexes == NULL ? row : body_offset_indexes[row];
		const int body_offset = body_offsets[body_offset_index];
		const int body_bytes = body_offsets[body_offset_index + 1] - body_offset;

		//		fprintf(stderr, "[%d/%ld] -> [%d]: filter %d, null %d, offset index %d, offset %d,
		// bytes %d\n", 			row, arrow->length, state_indices[row],
		// arrow_row_is_valid(filter, row), 			arrow_row_is_valid(arrow->buffers[0], row),
		// body_offset_index, body_offset,
		// body_bytes);

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
